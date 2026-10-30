"""OHLCV validation, timestamp normalization, and Kafka record shaping."""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from config import NSE_TIMEZONE_NAME


REQUIRED_FIELDS = ("open", "high", "low", "close", "volume")
NSE_TIMEZONE = ZoneInfo(NSE_TIMEZONE_NAME)


def as_utc_timestamp(value):
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("Invalid candle timestamp")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(NSE_TIMEZONE)
    return timestamp.tz_convert("UTC")


def utc_iso(timestamp: pd.Timestamp) -> str:
    return (
        timestamp.to_pydatetime()
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def validate_ohlcv(open_price, high_price, low_price, close_price, volume):
    prices = {
        "open": float(open_price),
        "high": float(high_price),
        "low": float(low_price),
        "close": float(close_price),
    }
    raw_volume = float(volume)

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

    return {**prices, "volume": int(raw_volume)}


def build_record(symbol: str, timestamp, ohlcv: dict, source_mode: str) -> dict:
    ts = as_utc_timestamp(timestamp)
    values = validate_ohlcv(
        ohlcv["open"], ohlcv["high"], ohlcv["low"], ohlcv["close"], ohlcv["volume"]
    )
    return {
        "timestamp": utc_iso(ts),
        "symbol": symbol,
        "open": values["open"],
        "high": values["high"],
        "low": values["low"],
        "close": values["close"],
        "volume": values["volume"],
        "source_mode": source_mode,
    }


def map_ohlcv_columns(history: pd.DataFrame) -> dict:
    column_names = {}
    for column in history.columns:
        candidates = column if isinstance(column, tuple) else (column,)
        for candidate in candidates:
            normalized = str(candidate).strip().lower()
            if normalized in REQUIRED_FIELDS:
                column_names.setdefault(normalized, column)
                break
    return column_names


def normalize_history(
    symbol: str,
    history: pd.DataFrame | None,
    now: datetime | None = None,
    finalization_delay=None,
    overlap_window="default",
    source_mode: str = "LIVE",
) -> list[dict]:
    """Validate provider rows and return finalized candles in timestamp order.

    overlap_window:
      - "default" or omitted → last 10 minutes (live publish window)
      - timedelta → custom lower bound
      - None → no lower bound (used to cache a full Yahoo session)
    """
    if history is None or getattr(history, "empty", True):
        return []

    column_names = map_ohlcv_columns(history)
    if any(field not in column_names for field in REQUIRED_FIELDS):
        raise ValueError(f"unexpected OHLCV columns {list(history.columns)!r}")

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)

    if finalization_delay is None:
        finalization_delay = timedelta(minutes=2)
    if overlap_window == "default":
        overlap_window = timedelta(minutes=10)

    finalized_before = pd.Timestamp(now - finalization_delay)
    overlap_after = None
    if overlap_window is not None:
        overlap_after = pd.Timestamp(now - overlap_window)

    rows = []
    ordered_history = history.loc[~history.index.duplicated(keep="last")]
    for raw_timestamp, values in ordered_history.iterrows():
        timestamp = as_utc_timestamp(raw_timestamp)
        if timestamp > finalized_before:
            continue
        if overlap_after is not None and timestamp < overlap_after:
            continue
        record = build_record(
            symbol,
            timestamp,
            {
                "open": values[column_names["open"]],
                "high": values[column_names["high"]],
                "low": values[column_names["low"]],
                "close": values[column_names["close"]],
                "volume": values[column_names["volume"]],
            },
            source_mode,
        )
        rows.append((timestamp, record))

    rows.sort(key=lambda row: row[0])
    return [record for _, record in rows]


def spark_payload(record: dict) -> dict:
    payload = {
        "timestamp": record["timestamp"],
        "symbol": record["symbol"],
        "open": record["open"],
        "high": record["high"],
        "low": record["low"],
        "close": record["close"],
        "volume": record["volume"],
    }
    if record.get("source_mode"):
        payload["source_mode"] = record["source_mode"]
    return payload


def serialize_record(record: dict) -> bytes:
    return json.dumps(spark_payload(record), allow_nan=False).encode("utf-8")
