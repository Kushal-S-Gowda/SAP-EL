from pathlib import Path

from producer.candle_cache import CandleCache


def test_cache_round_trip(tmp_path: Path):
    cache = CandleCache(tmp_path / "cache.sqlite3")
    record = {
        "symbol": "RELIANCE.NS",
        "timestamp": "2026-10-30T03:45:00Z",
        "open": 1400.0,
        "high": 1405.0,
        "low": 1398.0,
        "close": 1402.0,
        "volume": 15000,
    }
    cache.store(record)
    cache.store(record)
    rows = cache.load_all()
    assert cache.count() == 1
    assert rows[0]["source_mode"] == "DEMO_REPLAY"
    cache.close()
