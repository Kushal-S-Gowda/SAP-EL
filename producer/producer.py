"""Live Yahoo Finance polling producer for Kafka topic stock_data."""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from producer.candle_cache import CandleCache
from producer.ohlcv import normalize_history, serialize_record
from producer.providers import get_provider
from producer.publisher import KAFKA_ACK_TIMEOUT_SECONDS, publish_records
from producer.sink import store_pipeline_status, store_published_candles
from producer.state_store import CandleState
from producer.status import write_status

logger = logging.getLogger("rtsed.yahoo_producer")


def _validate_configuration():
    if config.POLL_INTERVAL_SECONDS <= 0:
        raise ValueError("RTSED_POLL_INTERVAL_SECONDS must be greater than zero")
    config.watchlist()


def poll_once(kafka_producer, state, provider, cache, now=None):
    """Fetch each symbol independently, then publish candles in event-time order."""
    now = now or datetime.now(timezone.utc)
    symbols = config.watchlist()
    records = []
    errors = []
    for symbol in symbols:
        try:
            history = provider.fetch_ohlcv(symbol)
            if history is None or getattr(history, "empty", True):
                logger.info("No Yahoo Finance data returned for %s", symbol)
                continue
            delay = timedelta(seconds=config.FINALIZATION_DELAY_SECONDS)
            for candle in normalize_history(
                symbol,
                history,
                now=now,
                finalization_delay=delay,
                overlap_window=None,
                source_mode="LIVE",
            ):
                cache.store(candle)
            records.extend(
                normalize_history(
                    symbol,
                    history,
                    now=now,
                    finalization_delay=delay,
                    overlap_window=timedelta(seconds=config.OVERLAP_WINDOW_SECONDS),
                    source_mode="LIVE",
                )
            )
        except Exception as error:
            logger.exception("Skipping this polling cycle for %s", symbol)
            errors.append(f"{symbol}: {error}")

    records.sort(key=lambda record: record["timestamp"])
    published = publish_records(
        records, kafka_producer, state, config.STOCK_DATA_TOPIC
    )
    store_published_candles(published)

    latest = published[-1]["timestamp"] if published else None
    last_fetch = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    status = {
        "operating_mode": "LIVE",
        "provider": provider.name,
        "watchlist": list(symbols),
        "watchlist_count": len(symbols),
        "last_successful_fetch": last_fetch if not errors or published else None,
        "latest_candle_timestamp": latest,
        "last_published_count": len(published),
        "cached_candles": cache.count(),
        "last_error": "; ".join(errors) if errors else None,
        "limitations": provider.describe_limitations(),
    }
    if errors and not published:
        status["last_successful_fetch"] = None
    elif not errors:
        status["last_successful_fetch"] = last_fetch
    write_status(config.STATUS_FILE, status)
    store_pipeline_status(status)
    logger.info("Polling cycle complete: published %d finalized candle(s)", len(published))
    return published


def main():
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _validate_configuration()
    symbols = config.watchlist()
    provider = get_provider(config.MARKET_DATA_PROVIDER)
    logger.info(
        "Starting %s polling for %s; interval=%ds, topic=%s",
        provider.name,
        ", ".join(symbols),
        config.POLL_INTERVAL_SECONDS,
        config.STOCK_DATA_TOPIC,
    )
    logger.warning(provider.describe_limitations())

    state = CandleState(config.PUBLISHED_STATE_PATH)
    cache = CandleCache(config.CANDLE_CACHE_PATH)
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=serialize_record,
        )
    except Exception:
        state.close()
        cache.close()
        raise

    try:
        while True:
            cycle_started = time.monotonic()
            try:
                poll_once(kafka_producer, state, provider, cache)
            except Exception:
                logger.exception("Polling cycle failed; the producer will retry")
                write_status(
                    config.STATUS_FILE,
                    {
                        "operating_mode": "LIVE",
                        "provider": provider.name,
                        "watchlist_count": len(symbols),
                        "last_error": "polling cycle failed; see producer logs",
                    },
                )

            wait_seconds = max(
                0,
                config.POLL_INTERVAL_SECONDS - (time.monotonic() - cycle_started),
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
            cache.close()


if __name__ == "__main__":
    main()
