import json
import logging
import math
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATE_DIRECTORY = Path(
    os.getenv("RTSED_PRODUCER_STATE_DIR", str(PROJECT_ROOT / "producer" / "state"))
)
STATE_DATABASE = STATE_DIRECTORY / "published_candles.sqlite3"

DEFAULT_WATCHLIST = (
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
)
WATCHLIST = tuple(
    symbol.strip()
    for symbol in os.getenv("RTSED_STOCKS", ",".join(DEFAULT_WATCHLIST)).split(",")
    if symbol.strip()
)
KAFKA_SERVER = os.getenv("RTSED_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock_data"
POLL_INTERVAL_SECONDS = int(os.getenv("RTSED_POLL_INTERVAL_SECONDS", "60"))
FINALIZATION_DELAY = timedelta(minutes=2)
OVERLAP_WINDOW = timedelta(minutes=10)
YAHOO_PERIOD = "1d"
YAHOO_INTERVAL = "1m"
YAHOO_MAX_ATTEMPTS = 3
RETRY_BASE_SECONDS = 1
KAFKA_ACK_TIMEOUT_SECONDS = 10
NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")

logger = logging.getLogger("rtsed.yahoo_producer")
REQUIRED_FIELDS = ("open", "high", "low", "close", "volume")


def _validate_configuration():
    if not WATCHLIST:
        raise ValueError("RTSED_STOCKS must contain at least one ticker symbol")
    if POLL_INTERVAL_SECONDS <= 0:
        raise ValueError("RTSED_POLL_INTERVAL_SECONDS must be greater than zero")


def _as_utc_timestamp(value):
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("Yahoo Finance returned an invalid candle timestamp")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(NSE_TIMEZONE)
    return timestamp.tz_convert("UTC")


def normalize_history(symbol, history, now=None):
    """Validate Yahoo rows and return finalized candles in timestamp order."""
    if history is None or history.empty:
        logger.info("No Yahoo Finance data returned for %s", symbol)
        return []

    column_names = {}
    for column in history.columns:
        candidates = column if isinstance(column, tuple) else (column,)
        for candidate in candidates:
            normalized = str(candidate).strip().lower()
            if normalized in REQUIRED_FIELDS:
                column_names.setdefault(normalized, column)
                break

    if any(field not in column_names for field in REQUIRED_FIELDS):
        logger.warning(
            "Skipping %s: unexpected Yahoo Finance columns %r",
            symbol,
            list(history.columns),
        )
        return []

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)
    finalized_before = pd.Timestamp(now - FINALIZATION_DELAY)
    overlap_after = pd.Timestamp(now - OVERLAP_WINDOW)

    rows = []
    ordered_history = history.loc[~history.index.duplicated(keep="last")]
    for raw_timestamp, values in ordered_history.iterrows():
        try:
            timestamp = _as_utc_timestamp(raw_timestamp)
            if timestamp > finalized_before or timestamp < overlap_after:
                continue

            prices = {
                field: float(values[column_names[field]])
                for field in ("open", "high", "low", "close")
            }
            raw_volume = float(values[column_names["volume"]])

            if not all(math.isfinite(value) and value > 0 for value in prices.values()):
                raise ValueError("OHLC values must be finite and greater than zero")
            if (
                prices["high"] < prices["low"]
                or prices["high"] < max(prices["open"], prices["close"])
                or prices["low"] > min(prices["open"], prices["close"])
            ):
                raise ValueError("OHLC values do not form a valid candle")
            if (
                not math.isfinite(raw_volume)
                or raw_volume < 0
                or not raw_volume.is_integer()
                or raw_volume > 9_223_372_036_854_775_807
            ):
                raise ValueError("volume must be a non-negative 64-bit integer")

            timestamp_text = (
                timestamp.to_pydatetime()
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
            rows.append(
                (
                    timestamp,
                    {
                        "timestamp": timestamp_text,
                        "symbol": symbol,
                        **prices,
                        "volume": int(raw_volume),
                    },
                )
            )
        except (TypeError, ValueError, OverflowError) as error:
            logger.warning(
                "Skipping invalid candle for %s at %s: %s",
                symbol,
                raw_timestamp,
                error,
            )

    rows.sort(key=lambda row: row[0])
    return [record for _, record in rows]


def fetch_history(symbol, attempts=YAHOO_MAX_ATTEMPTS):
    """Fetch one day of 1-minute data, retrying temporary Yahoo failures."""
    for attempt in range(1, attempts + 1):
        try:
            history = yf.Ticker(symbol).history(
                period=YAHOO_PERIOD,
                interval=YAHOO_INTERVAL,
                auto_adjust=False,
                actions=False,
                raise_errors=True,
            )
            if history is None or history.empty:
                logger.info("Yahoo Finance returned no recent data for %s", symbol)
            return history
        except Exception:
            if attempt == attempts:
                logger.exception(
                    "Yahoo Finance request failed for %s after %d attempts",
                    symbol,
                    attempts,
                )
                raise
            delay = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Yahoo Finance request failed for %s (attempt %d/%d); "
                "retrying in %d second(s)",
                symbol,
                attempt,
                attempts,
                delay,
                exc_info=True,
            )
            time.sleep(delay)


class CandleState:
    """Persistent keys for candles acknowledged by Kafka."""

    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=10)
        self.connection.execute("PRAGMA busy_timeout = 10000")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS published_candles (
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (symbol, timestamp)
            )
            """
        )
        self.connection.commit()

    def contains(self, symbol, timestamp):
        row = self.connection.execute(
            "SELECT 1 FROM published_candles WHERE symbol = ? AND timestamp = ?",
            (symbol, timestamp),
        ).fetchone()
        return row is not None

    def record_published(self, symbol, timestamp):
        with self.connection:
            self.connection.execute(
                "INSERT INTO published_candles (symbol, timestamp) VALUES (?, ?)",
                (symbol, timestamp),
            )

    def close(self):
        self.connection.close()


def serialize_record(record):
    return json.dumps(record, allow_nan=False).encode("utf-8")


def publish_records(records, kafka_producer, state):
    """Publish unseen candles and persist only after Kafka acknowledges each one."""
    published = 0
    for record in records:
        symbol = record["symbol"]
        timestamp = record["timestamp"]
        try:
            if state.contains(symbol, timestamp):
                logger.debug("Already published %s at %s", symbol, timestamp)
                continue
        except sqlite3.Error:
            logger.exception(
                "Could not check SQLite deduplication state for %s at %s",
                symbol,
                timestamp,
            )
            continue

        try:
            kafka_producer.send(
                KAFKA_TOPIC,
                key=symbol,
                value=record,
            ).get(timeout=KAFKA_ACK_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("Kafka publish failed for %s at %s", symbol, timestamp)
            continue

        try:
            state.record_published(symbol, timestamp)
            published += 1
        except sqlite3.Error:
            # Kafka and SQLite cannot share an atomic transaction; a restart may resend.
            logger.exception(
                "Kafka acknowledged %s at %s, but SQLite state could not be saved",
                symbol,
                timestamp,
            )

    return published


def poll_once(kafka_producer, state, now=None):
    """Fetch each symbol independently, then publish all candles in event-time order."""
    now = now or datetime.now(timezone.utc)
    records = []
    for symbol in WATCHLIST:
        try:
            history = fetch_history(symbol)
            records.extend(normalize_history(symbol, history, now=now))
        except Exception:
            logger.exception("Skipping this polling cycle for %s", symbol)

    records.sort(key=lambda record: record["timestamp"])
    sent = publish_records(records, kafka_producer, state)
    logger.info("Polling cycle complete: published %d finalized candle(s)", sent)
    return sent


def main():
    logging.basicConfig(
        level=os.getenv("RTSED_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _validate_configuration()
    logger.info(
        "Starting Yahoo Finance polling for %s; interval=%ds, topic=%s",
        ", ".join(WATCHLIST),
        POLL_INTERVAL_SECONDS,
        KAFKA_TOPIC,
    )
    logger.warning(
        "Yahoo Finance data is unofficial and may be delayed, incomplete, "
        "rate-limited, or unavailable."
    )

    state = CandleState(STATE_DATABASE)
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=serialize_record,
        )
    except Exception:
        state.close()
        raise

    try:
        while True:
            cycle_started = time.monotonic()
            try:
                poll_once(kafka_producer, state)
            except Exception:
                logger.exception("Polling cycle failed; the producer will retry")

            wait_seconds = max(
                0,
                POLL_INTERVAL_SECONDS - (time.monotonic() - cycle_started),
            )
            time.sleep(wait_seconds)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        try:
            try:
                kafka_producer.flush(timeout=KAFKA_ACK_TIMEOUT_SECONDS)
            except Exception:
                logger.exception(
                    "Failed while flushing the Kafka producer during shutdown"
                )
            kafka_producer.close()
        finally:
            state.close()


if __name__ == "__main__":
    main()
