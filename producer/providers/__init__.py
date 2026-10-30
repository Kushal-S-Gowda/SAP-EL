from .base_provider import MarketDataProvider
from .yahoo_provider import YahooFinanceProvider


def get_provider(name: str) -> MarketDataProvider:
    normalized = (name or "yahoo").strip().lower()
    if normalized in {"yahoo", "yfinance", "yahoo_finance"}:
        return YahooFinanceProvider()
    raise ValueError(
        f"Unsupported market-data provider {name!r}. "
        "Only the free Yahoo Finance provider is implemented."
    )


__all__ = ["MarketDataProvider", "YahooFinanceProvider", "get_provider"]
