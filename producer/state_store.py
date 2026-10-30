"""Persistent (symbol, timestamp) deduplication after Kafka acknowledgement."""

from __future__ import annotations

import sqlite3
from pathlib import Path


class CandleState:
    """Persistent keys for candles acknowledged by Kafka."""

    def __init__(self, path: Path):
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

    def contains(self, symbol: str, timestamp: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM published_candles WHERE symbol = ? AND timestamp = ?",
            (symbol, timestamp),
        ).fetchone()
        return row is not None

    def record_published(self, symbol: str, timestamp: str) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO published_candles (symbol, timestamp) VALUES (?, ?)",
                (symbol, timestamp),
            )

    def close(self) -> None:
        self.connection.close()
