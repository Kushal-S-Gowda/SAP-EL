"""Kafka publish helpers shared by live, demo, and historical producers."""

from __future__ import annotations

import logging
import sqlite3

from producer.state_store import CandleState

logger = logging.getLogger("rtsed.publisher")

KAFKA_ACK_TIMEOUT_SECONDS = 10


def publish_records(records, kafka_producer, state: CandleState, topic: str) -> list[dict]:
    """Publish unseen candles and persist only after Kafka acknowledges each one."""
    published = []
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
                topic,
                key=symbol,
                value=record,
            ).get(timeout=KAFKA_ACK_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("Kafka publish failed for %s at %s", symbol, timestamp)
            continue

        try:
            state.record_published(symbol, timestamp)
            published.append(record)
        except sqlite3.Error:
            logger.exception(
                "Kafka acknowledged %s at %s, but SQLite state could not be saved",
                symbol,
                timestamp,
            )

    return published
