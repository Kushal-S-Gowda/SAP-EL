from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd


def to_ist_label(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return "—"
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert(ZoneInfo("Asia/Kolkata")).strftime(
        "%Y-%m-%d %H:%M:%S IST"
    )


def test_utc_displayed_as_ist():
    label = to_ist_label(datetime(2026, 10, 30, 3, 45, tzinfo=timezone.utc))
    assert label == "2026-10-30 09:15:00 IST"


def test_missing_timestamp_is_placeholder():
    assert to_ist_label(None) == "—"
