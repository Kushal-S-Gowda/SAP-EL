"""Common interface for free market-data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class MarketDataProvider(ABC):
    """Fetch 1-minute OHLCV candles for a single symbol."""

    name = "base"

    @abstractmethod
    def fetch_ohlcv(self, symbol: str) -> pd.DataFrame:
        """Return a DataFrame indexed by candle time with OHLCV columns.

        An empty DataFrame means the provider had no usable data, not an error.
        Temporary transport failures should raise so the caller can retry or isolate.
        """

    def describe_limitations(self) -> str:
        return "Market data may be delayed, incomplete, or unavailable."
