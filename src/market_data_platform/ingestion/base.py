"""Abstract ingestion interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


class MarketDataSource(ABC):
    """Abstract interface for OHLCV market data sources."""

    @abstractmethod
    def fetch_historical_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> pd.DataFrame:
        """Fetch OHLCV candles between two timestamps.

        Args:
            symbol: Trading symbol such as ``BTCUSDT``.
            interval: Candle interval such as ``1m``.
            start_time: Inclusive lower bound for extraction.
            end_time: Inclusive upper bound for extraction.

        Returns:
            pd.DataFrame: Raw extracted market data.
        """

    @abstractmethod
    def fetch_recent_ohlcv(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        """Fetch the most recent OHLCV candles.

        Args:
            symbol: Trading symbol such as ``BTCUSDT``.
            interval: Candle interval such as ``1m``.
            limit: Maximum number of rows to fetch.

        Returns:
            pd.DataFrame: Raw extracted market data.
        """

    @abstractmethod
    def fetch_latest_price(self, symbol: str) -> dict[str, str]:
        """Fetch the latest spot price for a symbol.

        Args:
            symbol: Trading symbol such as ``BTCUSDT``.

        Returns:
            dict[str, str]: Latest price payload from the market data source.
        """
