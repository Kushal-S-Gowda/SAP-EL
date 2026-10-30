"""PostgreSQL helpers and idempotent schema application."""

from __future__ import annotations

from pathlib import Path

import psycopg2
from psycopg2.extras import Json

import config

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def connect():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def apply_schema(connection=None) -> None:
    own_connection = connection is None
    conn = connection or connect()
    try:
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        if own_connection:
            conn.close()


def upsert_candles(records: list[dict]) -> None:
    if not records:
        return
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO stock_candles (
                    symbol, ts, open, high, low, close, volume, source_mode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, ts) DO NOTHING
                """,
                [
                    (
                        record["symbol"],
                        record["timestamp"],
                        record["open"],
                        record["high"],
                        record["low"],
                        record["close"],
                        record["volume"],
                        record.get("source_mode", "UNKNOWN"),
                    )
                    for record in records
                ],
            )
        conn.commit()
    finally:
        conn.close()


def upsert_candle(record: dict) -> None:
    upsert_candles([record])


def upsert_pipeline_status(payload: dict) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_status (
                    id, operating_mode, provider, last_successful_fetch,
                    latest_candle_timestamp, watchlist_count,
                    last_published_count, last_error, updated_at
                ) VALUES (1, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    operating_mode = EXCLUDED.operating_mode,
                    provider = EXCLUDED.provider,
                    last_successful_fetch = EXCLUDED.last_successful_fetch,
                    latest_candle_timestamp = EXCLUDED.latest_candle_timestamp,
                    watchlist_count = EXCLUDED.watchlist_count,
                    last_published_count = EXCLUDED.last_published_count,
                    last_error = EXCLUDED.last_error,
                    updated_at = NOW()
                """,
                (
                    payload.get("operating_mode"),
                    payload.get("provider"),
                    payload.get("last_successful_fetch"),
                    payload.get("latest_candle_timestamp"),
                    payload.get("watchlist_count"),
                    payload.get("last_published_count"),
                    payload.get("last_error"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def as_json(value):
    return Json(value)
