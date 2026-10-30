from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from producer.ohlcv import (
    build_record,
    normalize_history,
    serialize_record,
    validate_ohlcv,
)


def test_validate_ohlcv_rejects_inverted_high_low():
    with pytest.raises(ValueError):
        validate_ohlcv(10, 9, 11, 10, 100)


def test_validate_ohlcv_rejects_fractional_volume():
    with pytest.raises(ValueError):
        validate_ohlcv(10, 11, 9, 10.5, 12.5)


def test_build_record_utc_zulu():
    record = build_record(
        "RELIANCE.NS",
        datetime(2026, 10, 30, 3, 45, tzinfo=timezone.utc),
        {"open": 1400.0, "high": 1405.0, "low": 1398.0, "close": 1402.0, "volume": 15000},
        "LIVE",
    )
    assert record["timestamp"] == "2026-10-30T03:45:00Z"
    assert record["symbol"] == "RELIANCE.NS"
    assert record["volume"] == 15000


def test_naive_nse_timestamp_localized_to_ist_then_utc():
    record = build_record(
        "TCS.NS",
        datetime(2026, 10, 30, 9, 15),
        {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1},
        "LIVE",
    )
    assert record["timestamp"] == "2026-10-30T03:45:00Z"


def test_normalize_skips_incomplete_and_old_candles():
    index = pd.to_datetime(
        [
            "2026-10-30T03:30:00Z",
            "2026-10-30T03:48:00Z",
            "2026-10-30T03:50:00Z",
        ],
        utc=True,
    )
    history = pd.DataFrame(
        {
            "Open": [100.0, 101.0, 102.0],
            "High": [101.0, 102.0, 103.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [100.5, 101.5, 102.5],
            "Volume": [10, 20, 30],
        },
        index=index,
    )
    now = datetime(2026, 10, 30, 3, 51, tzinfo=timezone.utc)
    rows = normalize_history(
        "INFY.NS",
        history,
        now=now,
        finalization_delay=timedelta(minutes=2),
        overlap_window=timedelta(minutes=10),
        source_mode="LIVE",
    )
    timestamps = [row["timestamp"] for row in rows]
    assert timestamps == ["2026-10-30T03:48:00Z"]

    cached = normalize_history(
        "INFY.NS",
        history,
        now=now,
        finalization_delay=timedelta(minutes=2),
        overlap_window=None,
        source_mode="LIVE",
    )
    assert [row["timestamp"] for row in cached] == [
        "2026-10-30T03:30:00Z",
        "2026-10-30T03:48:00Z",
    ]


def test_empty_history_returns_empty_list():
    assert normalize_history("INFY.NS", pd.DataFrame()) == []


def test_serialize_record_is_valid_json_bytes():
    record = build_record(
        "HDFCBANK.NS",
        datetime(2026, 10, 30, 4, 0, tzinfo=timezone.utc),
        {"open": 2.0, "high": 2.1, "low": 1.9, "close": 2.0, "volume": 5},
        "LIVE",
    )
    payload = serialize_record(record)
    assert payload.startswith(b"{")
    assert b"HDFCBANK.NS" in payload
    assert b"timestamp" in payload
