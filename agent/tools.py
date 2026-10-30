"""Read-only investigation tools. No arbitrary SQL, shell, or filesystem access."""

from __future__ import annotations

from datetime import datetime

from agent.severity import assess_event_severity
from database.db import connect


def _fetchall(query: str, params: tuple) -> list[dict]:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def _fetchone(query: str, params: tuple) -> dict | None:
    rows = _fetchall(query, params)
    return rows[0] if rows else None


def get_event_details(event_id: int | None = None, **identity) -> dict:
    """Retrieve the original Spark-detected event from PostgreSQL."""
    if event_id is not None:
        row = _fetchone(
            """
            SELECT id, symbol, event_type, event_start, event_end, avg_close,
                   total_volume, avg_price_change_pct, record_count, created_at
            FROM stock_events
            WHERE id = %s
            """,
            (int(event_id),),
        )
        if not row:
            return {"found": False, "reason": "No stock_events row for this event_id"}
        return {"found": True, "event": _stringify(row)}

    row = _fetchone(
        """
        SELECT id, symbol, event_type, event_start, event_end, avg_close,
               total_volume, avg_price_change_pct, record_count, created_at
        FROM stock_events
        WHERE symbol = %s AND event_start = %s AND event_end = %s AND event_type = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (
            identity["symbol"],
            identity["event_start"],
            identity["event_end"],
            identity["event_type"],
        ),
    )
    if not row:
        return {
            "found": False,
            "reason": "Event is in Kafka but not yet stored in stock_events",
            "identity": identity,
        }
    return {"found": True, "event": _stringify(row)}


def get_recent_stock_history(symbol: str, minutes: int = 60, as_of: str | None = None) -> dict:
    minutes = max(1, min(int(minutes), 24 * 60))
    rows = _fetchall(
        """
        SELECT symbol, ts, open, high, low, close, volume, source_mode
        FROM stock_candles
        WHERE symbol = %s
          AND ts <= COALESCE(%s::timestamptz, NOW())
          AND ts >= COALESCE(%s::timestamptz, NOW()) - (%s * INTERVAL '1 minute')
        ORDER BY ts
        """,
        (symbol, as_of, as_of, minutes),
    )
    return {
        "symbol": symbol,
        "minutes": minutes,
        "row_count": len(rows),
        "candles": _stringify(rows),
        "note": (
            "Empty history means candles have not been ingested into PostgreSQL "
            "yet, not that the market had no activity."
        ),
    }


def calculate_price_movement(symbol: str, window: int = 15, as_of: str | None = None) -> dict:
    window = max(1, min(int(window), 24 * 60))
    rows = _fetchall(
        """
        SELECT ts, open, close
        FROM stock_candles
        WHERE symbol = %s
          AND ts <= COALESCE(%s::timestamptz, NOW())
        ORDER BY ts DESC
        LIMIT %s
        """,
        (symbol, as_of, window),
    )
    if len(rows) < 2:
        return {
            "symbol": symbol,
            "window": window,
            "available_rows": len(rows),
            "percent_change": None,
            "reason": "Need at least two candles to calculate price movement",
        }
    chronological = list(reversed(rows))
    start = float(chronological[0]["open"])
    end = float(chronological[-1]["close"])
    percent = ((end - start) / start) * 100 if start else None
    return {
        "symbol": symbol,
        "window_candles": len(chronological),
        "start_open": start,
        "end_close": end,
        "percent_change": percent,
        "first_ts": str(chronological[0]["ts"]),
        "last_ts": str(chronological[-1]["ts"]),
    }


def calculate_volume_anomaly(symbol: str, window: int = 20, as_of: str | None = None) -> dict:
    window = max(2, min(int(window), 500))
    rows = _fetchall(
        """
        SELECT ts, volume
        FROM stock_candles
        WHERE symbol = %s
          AND ts <= COALESCE(%s::timestamptz, NOW())
        ORDER BY ts DESC
        LIMIT %s
        """,
        (symbol, as_of, window),
    )
    if not rows:
        return {
            "symbol": symbol,
            "volume_ratio": None,
            "reason": "No candle volume history is stored for this symbol",
        }
    current = float(rows[0]["volume"])
    baseline_rows = rows[1:]
    if not baseline_rows:
        return {
            "symbol": symbol,
            "current_volume": current,
            "volume_ratio": None,
            "reason": "Need a rolling baseline of earlier candles",
        }
    baseline = sum(float(row["volume"]) for row in baseline_rows) / len(baseline_rows)
    ratio = (current / baseline) if baseline else None
    return {
        "symbol": symbol,
        "current_volume": current,
        "baseline_volume": baseline,
        "baseline_candles": len(baseline_rows),
        "volume_ratio": ratio,
        "current_ts": str(rows[0]["ts"]),
    }


def get_previous_stock_events(symbol: str, limit: int = 10) -> dict:
    limit = max(1, min(int(limit), 50))
    rows = _fetchall(
        """
        SELECT id, event_type, event_start, event_end, avg_close, total_volume,
               avg_price_change_pct
        FROM stock_events
        WHERE symbol = %s
        ORDER BY event_start DESC
        LIMIT %s
        """,
        (symbol, limit),
    )
    return {
        "symbol": symbol,
        "previous_event_count": len(rows),
        "events": _stringify(rows),
        "pattern_hint": (
            "Multiple similar events in a short span can indicate a cluster; "
            "a single row is isolated in the stored history."
        ),
    }


def assess_event_severity_tool(event_metrics: dict) -> dict:
    return assess_event_severity(
        event_type=event_metrics.get("event_type") or "",
        price_change_percent=_to_float(event_metrics.get("price_change_percent")),
        volume=_to_float(event_metrics.get("volume")),
        volume_ratio=_to_float(event_metrics.get("volume_ratio")),
    )


TOOL_REGISTRY = {
    "get_event_details": get_event_details,
    "get_recent_stock_history": get_recent_stock_history,
    "calculate_price_movement": calculate_price_movement,
    "calculate_volume_anomaly": calculate_volume_anomaly,
    "get_previous_stock_events": get_previous_stock_events,
    "assess_event_severity": assess_event_severity_tool,
}

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_event_details",
            "description": "Retrieve the original Spark-detected event from PostgreSQL.",
            "parameters": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_stock_history",
            "description": "Retrieve recent stored OHLCV candles for a symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "minutes": {"type": "integer"},
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_price_movement",
            "description": "Calculate percentage price movement from stored candles.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "window": {"type": "integer"},
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_volume_anomaly",
            "description": "Compare current volume against a rolling baseline of stored candles.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "window": {"type": "integer"},
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_previous_stock_events",
            "description": "Retrieve earlier detected events for the same stock.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "assess_event_severity",
            "description": "Calculate evidence-based severity from documented numerical bands.",
            "parameters": {
                "type": "object",
                "properties": {
                    "event_metrics": {"type": "object"},
                },
                "required": ["event_metrics"],
            },
        },
    },
]


def execute_tool(name: str, arguments: dict, event_context: dict) -> dict:
    if name not in TOOL_REGISTRY:
        return {"error": f"Unknown tool {name}"}
    args = dict(arguments or {})
    if name == "get_event_details":
        if "event_id" not in args and event_context.get("event_id") is not None:
            args["event_id"] = event_context["event_id"]
        elif "event_id" not in args:
            args.update(
                {
                    "symbol": event_context.get("symbol"),
                    "event_start": event_context.get("event_start"),
                    "event_end": event_context.get("event_end"),
                    "event_type": event_context.get("event_type"),
                }
            )
    if name in {
        "get_recent_stock_history",
        "calculate_price_movement",
        "calculate_volume_anomaly",
        "get_previous_stock_events",
    }:
        args.setdefault("symbol", event_context.get("symbol"))
        if name != "get_previous_stock_events":
            args.setdefault(
                "as_of",
                event_context.get("event_end") or event_context.get("event_start"),
            )
    if name == "assess_event_severity":
        metrics = dict(args.get("event_metrics") or {})
        metrics.setdefault("event_type", event_context.get("event_type"))
        metrics.setdefault(
            "price_change_percent", event_context.get("avg_price_change_pct")
        )
        metrics.setdefault("volume", event_context.get("total_volume"))
        args["event_metrics"] = metrics
    try:
        return TOOL_REGISTRY[name](**args)
    except TypeError as error:
        return {"error": f"Invalid arguments for {name}: {error}"}
    except Exception as error:
        return {"error": f"{name} failed: {error}"}


def _to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def _stringify(value):
    if isinstance(value, list):
        return [_stringify(item) for item in value]
    if isinstance(value, dict):
        return {key: _stringify(item) for key, item in value.items()}
    if isinstance(value, datetime):
        return value.isoformat()
    return value
