import pandas as pd

from producer.providers.yahoo_provider import YahooFinanceProvider


class _FailingTicker:
    def history(self, **kwargs):
        raise RuntimeError("rate limited")


def test_yahoo_retries_then_raises(monkeypatch):
    calls = {"n": 0}

    def fake_ticker(symbol):
        calls["n"] += 1
        return _FailingTicker()

    monkeypatch.setattr("producer.providers.yahoo_provider.yf.Ticker", fake_ticker)
    monkeypatch.setattr("producer.providers.yahoo_provider.time.sleep", lambda _: None)
    provider = YahooFinanceProvider(max_attempts=3)
    try:
        provider.fetch_ohlcv("INFY.NS")
        assert False, "expected failure"
    except RuntimeError:
        assert calls["n"] == 3


def test_yahoo_empty_frame_is_not_an_error(monkeypatch):
    class _Empty:
        def history(self, **kwargs):
            return pd.DataFrame()

    monkeypatch.setattr(
        "producer.providers.yahoo_provider.yf.Ticker", lambda symbol: _Empty()
    )
    history = YahooFinanceProvider().fetch_ohlcv("INFY.NS")
    assert history.empty
