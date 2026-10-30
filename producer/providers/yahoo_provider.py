"""Yahoo Finance 1-minute OHLCV provider via the unofficial yfinance library."""

from __future__ import annotations

import logging
import time

import pandas as pd
import yfinance as yf

from .base_provider import MarketDataProvider

logger = logging.getLogger("rtsed.yahoo_provider")

YAHOO_PERIOD = "1d"
YAHOO_INTERVAL = "1m"
YAHOO_MAX_ATTEMPTS = 3
RETRY_BASE_SECONDS = 1


class YahooFinanceProvider(MarketDataProvider):
    name = "yahoo"

    def __init__(
        self,
        period: str = YAHOO_PERIOD,
        interval: str = YAHOO_INTERVAL,
        max_attempts: int = YAHOO_MAX_ATTEMPTS,
    ):
        self.period = period
        self.interval = interval
        self.max_attempts = max_attempts

    def describe_limitations(self) -> str:
        return (
            "Yahoo Finance through yfinance is an unofficial interface. "
            "NSE quotes may be delayed; 1-minute history is limited to a recent "
            "intraday window; rate limits, gaps, and downtime are possible. "
            "This is not a guaranteed real-time exchange feed."
        )

    def fetch_ohlcv(self, symbol: str) -> pd.DataFrame:
        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                history = yf.Ticker(symbol).history(
                    period=self.period,
                    interval=self.interval,
                    auto_adjust=False,
                    actions=False,
                    raise_errors=True,
                )
                if history is None:
                    return pd.DataFrame()
                return history
            except Exception as error:
                last_error = error
                if attempt == self.max_attempts:
                    logger.exception(
                        "Yahoo Finance request failed for %s after %d attempts",
                        symbol,
                        self.max_attempts,
                    )
                    raise
                delay = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Yahoo Finance request failed for %s (attempt %d/%d); "
                    "retrying in %d second(s)",
                    symbol,
                    attempt,
                    self.max_attempts,
                    delay,
                    exc_info=True,
                )
                time.sleep(delay)
        raise last_error
