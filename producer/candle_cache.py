"""Local cache of genuine Yahoo Finance candles for after-hours demo replay."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger("rtsed.candle_cache")


class CandleCache:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=10)
        self.connection.execute("PRAGMA busy_timeout = 10000")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cached_candles (
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                PRIMARY KEY (symbol, timestamp)
            )
            """
        )
        self.connection.commit()

    def store(self, record: dict) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO cached_candles (
                    symbol, timestamp, open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["symbol"],
                    record["timestamp"],
                    record["open"],
                    record["high"],
                    record["low"],
                    record["close"],
                    record["volume"],
                ),
            )

    def count(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM cached_candles"
        ).fetchone()
        return int(row[0]) if row else 0

    def load_all(self) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM cached_candles
            ORDER BY timestamp, symbol
            """
        ).fetchall()
        records = []
        for symbol, timestamp, open_, high, low, close, volume in rows:
            records.append(
                {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "source_mode": "DEMO_REPLAY",
                }
            )
        return records

    def close(self) -> None:
        self.connection.close()


def fill_cache_from_yahoo(cache: CandleCache) -> int:
    """Fetch genuine Yahoo 1-minute candles and store finalized rows locally."""
    import config
    from producer.ohlcv import normalize_history
    from producer.providers import get_provider

    provider = get_provider(config.MARKET_DATA_PROVIDER)
    now = datetime.now(timezone.utc)
    delay = timedelta(seconds=config.FINALIZATION_DELAY_SECONDS)
    stored = 0
    for symbol in config.watchlist():
        try:
            history = provider.fetch_ohlcv(symbol)
            if history is None or getattr(history, "empty", True):
                logger.info("Yahoo returned no candles to cache for %s", symbol)
                continue
            rows = normalize_history(
                symbol,
                history,
                now=now,
                finalization_delay=delay,
                overlap_window=None,
                source_mode="LIVE",
            )
            for row in rows:
                cache.store(row)
                stored += 1
            logger.info("Cached %d finalized Yahoo candle(s) for %s", len(rows), symbol)
        except Exception:
            logger.exception("Could not cache Yahoo candles for %s", symbol)
    return stored
