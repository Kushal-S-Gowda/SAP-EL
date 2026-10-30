from pathlib import Path

from producer.publisher import publish_records
from producer.state_store import CandleState


class _Future:
    def __init__(self, fail=False):
        self.fail = fail

    def get(self, timeout=None):
        if self.fail:
            raise RuntimeError("kafka down")
        return True


class _Producer:
    def __init__(self, fail_keys=()):
        self.sent = []
        self.fail_keys = set(fail_keys)

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        return _Future(fail=key in self.fail_keys)


def test_duplicate_candles_are_not_republished(tmp_path: Path):
    state = CandleState(tmp_path / "state.sqlite3")
    producer = _Producer()
    record = {
        "symbol": "RELIANCE.NS",
        "timestamp": "2026-10-30T03:45:00Z",
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1,
        "source_mode": "LIVE",
    }
    first = publish_records([record], producer, state, "stock_data")
    second = publish_records([record], producer, state, "stock_data")
    state.close()
    assert len(first) == 1
    assert second == []
    assert len(producer.sent) == 1


def test_failed_kafka_publish_is_not_recorded(tmp_path: Path):
    state = CandleState(tmp_path / "state.sqlite3")
    producer = _Producer(fail_keys={"TCS.NS"})
    record = {
        "symbol": "TCS.NS",
        "timestamp": "2026-10-30T03:45:00Z",
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1,
    }
    published = publish_records([record], producer, state, "stock_data")
    assert published == []
    assert state.contains("TCS.NS", "2026-10-30T03:45:00Z") is False
    state.close()
