"""Optional PostgreSQL candle/status writes that must not block Kafka ingestion."""

from __future__ import annotations

import logging

from database.db import apply_schema, upsert_candles, upsert_pipeline_status

logger = logging.getLogger("rtsed.sink")

_schema_ready = False


def ensure_schema() -> bool:
    global _schema_ready
    if _schema_ready:
        return True
    try:
        apply_schema()
        _schema_ready = True
        return True
    except Exception:
        logger.exception("PostgreSQL schema is unavailable; candle sink disabled")
        return False


def store_published_candles(records: list[dict]) -> None:
    if not records:
        return
    if not ensure_schema():
        return
    try:
        upsert_candles(records)
    except Exception:
        logger.exception("Could not store published candles in PostgreSQL")


def store_pipeline_status(payload: dict) -> None:
    if not ensure_schema():
        return
    try:
        upsert_pipeline_status(payload)
    except Exception:
        logger.exception("Could not update pipeline_status")
