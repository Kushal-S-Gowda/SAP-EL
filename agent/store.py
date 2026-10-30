"""Idempotent investigation persistence."""

from __future__ import annotations

from database.db import as_json, connect


def lookup_event_id(event: dict) -> int | None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM stock_events
                WHERE symbol = %s
                  AND event_start = %s
                  AND event_end = %s
                  AND event_type = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    event.get("symbol"),
                    event.get("event_start"),
                    event.get("event_end"),
                    event.get("event_type"),
                ),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None
    finally:
        conn.close()


def enqueue_investigation(event: dict) -> dict:
    event_id = lookup_event_id(event)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_investigations (
                    event_id, symbol, event_type, event_start, event_end,
                    investigation_status
                ) VALUES (%s, %s, %s, %s, %s, 'PENDING')
                ON CONFLICT (symbol, event_start, event_end, event_type)
                DO UPDATE SET
                    event_id = COALESCE(ai_investigations.event_id, EXCLUDED.event_id)
                RETURNING investigation_id, investigation_status, event_id
                """,
                (
                    event_id,
                    event.get("symbol"),
                    event.get("event_type"),
                    event.get("event_start"),
                    event.get("event_end"),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return {
            "investigation_id": row[0],
            "investigation_status": row[1],
            "event_id": row[2],
        }
    finally:
        conn.close()


def claim_pending(limit: int = 5) -> list[dict]:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT investigation_id, event_id, symbol, event_type,
                       event_start, event_end
                FROM ai_investigations
                WHERE investigation_status IN ('PENDING', 'FAILED')
                   OR (
                        investigation_status = 'PROCESSING'
                        AND updated_at < NOW() - INTERVAL '10 minutes'
                   )
                ORDER BY
                    CASE
                        WHEN investigation_status = 'PENDING' THEN 0
                        WHEN investigation_status = 'PROCESSING' THEN 1
                        ELSE 2
                    END,
                    investigation_id DESC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (limit,),
            )
            rows = cur.fetchall()
            claimed = []
            for row in rows:
                cur.execute(
                    """
                    UPDATE ai_investigations
                    SET investigation_status = 'PROCESSING',
                        updated_at = NOW()
                    WHERE investigation_id = %s
                    """,
                    (row[0],),
                )
                claimed.append(
                    {
                        "investigation_id": row[0],
                        "event_id": row[1],
                        "symbol": row[2],
                        "event_type": row[3],
                        "event_start": str(row[4]),
                        "event_end": str(row[5]),
                    }
                )
        conn.commit()
        return claimed
    finally:
        conn.close()


def mark_completed(investigation_id: int, result: dict) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ai_investigations
                SET severity = %s,
                    investigation_summary = %s,
                    evidence = %s,
                    price_change_percent = %s,
                    volume_ratio = %s,
                    previous_event_count = %s,
                    confidence = %s,
                    investigation_status = 'COMPLETED',
                    error_message = NULL,
                    updated_at = NOW()
                WHERE investigation_id = %s
                """,
                (
                    result.get("severity"),
                    result.get("investigation_summary"),
                    as_json(result.get("evidence") or {}),
                    result.get("price_change_percent"),
                    result.get("volume_ratio"),
                    result.get("previous_event_count"),
                    result.get("confidence"),
                    investigation_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def mark_failed(investigation_id: int, error_message: str) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ai_investigations
                SET investigation_status = 'FAILED',
                    error_message = %s,
                    updated_at = NOW()
                WHERE investigation_id = %s
                """,
                (error_message[:2000], investigation_id),
            )
        conn.commit()
    finally:
        conn.close()


def load_event_metrics(event_id: int | None, claimed: dict) -> dict:
    conn = connect()
    try:
        with conn.cursor() as cur:
            if event_id is not None:
                cur.execute(
                    """
                    SELECT id, symbol, event_type, event_start, event_end,
                           avg_close, total_volume, avg_price_change_pct, record_count
                    FROM stock_events
                    WHERE id = %s
                    """,
                    (event_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, symbol, event_type, event_start, event_end,
                           avg_close, total_volume, avg_price_change_pct, record_count
                    FROM stock_events
                    WHERE symbol = %s AND event_start = %s
                      AND event_end = %s AND event_type = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (
                        claimed["symbol"],
                        claimed["event_start"],
                        claimed["event_end"],
                        claimed["event_type"],
                    ),
                )
            row = cur.fetchone()
            if not row:
                return dict(claimed)
            columns = [desc[0] for desc in cur.description]
            data = dict(zip(columns, row))
            data["event_id"] = data.pop("id")
            data["event_start"] = str(data["event_start"])
            data["event_end"] = str(data["event_end"])
            return data
    finally:
        conn.close()
