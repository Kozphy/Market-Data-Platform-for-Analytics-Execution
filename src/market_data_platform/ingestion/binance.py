"""Binance REST client with retry and rate-limit handling."""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from market_data_platform.config import ApiSettings
from market_data_platform.ingestion.base import MarketDataSource
from market_data_platform.schema import RAW_KLINE_COLUMNS
from market_data_platform.utils.time_utils import binance_interval_to_timedelta, ensure_utc, utc_now

LOGGER = logging.getLogger(__name__)


class ApiRequestError(RuntimeError):
    """Raised when the market data API fails after all retries."""


class BinanceMarketDataSource(MarketDataSource):
    """Fetch market OHLCV data from the Binance REST API."""

    def __init__(self, settings: ApiSettings) -> None:
        """Initialize the Binance client.

        Args:
            settings: API client settings.
        """
        self._settings = settings
        self._client = httpx.Client(
            base_url=settings.base_url,
            timeout=settings.timeout_seconds,
            headers={"User-Agent": "market-data-platform/0.1.0"},
        )

    def close(self) -> None:
        """Close the underlying HTTP client.

        Returns:
            None: This method releases the HTTP connection pool.
        """
        self._client.close()

    def _request(self, path: str, params: dict[str, Any]) -> Any:
        """Execute a GET request with retry and backoff.

        Args:
            path: Relative endpoint path.
            params: Query parameters.

        Returns:
            list[list[Any]]: Parsed JSON payload from Binance.

        Raises:
            ApiRequestError: If the request fails after retries.
        """
        for attempt in range(1, self._settings.max_retries + 1):
            try:
                response = self._client.get(path, params=params)
                if response.status_code in {418, 429}:
                    retry_after = response.headers.get("Retry-After")
                    wait_seconds = float(retry_after) if retry_after else self._backoff_seconds(attempt)
                    LOGGER.warning(
                        "Rate limit encountered",
                        extra={"payload": {"attempt": attempt, "wait_seconds": wait_seconds}},
                    )
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == self._settings.max_retries:
                    raise ApiRequestError("Binance request failed after retries") from exc
                wait_seconds = self._backoff_seconds(attempt)
                LOGGER.warning(
                    "Transient API failure",
                    extra={
                        "payload": {
                            "attempt": attempt,
                            "wait_seconds": wait_seconds,
                            "error": str(exc),
                        }
                    },
                )
                time.sleep(wait_seconds)

        raise ApiRequestError("Binance request exhausted retries")

    def _backoff_seconds(self, attempt: int) -> float:
        """Calculate exponential backoff for a retry attempt.

        Args:
            attempt: Current retry attempt starting at one.

        Returns:
            float: Number of seconds to wait before retrying.
        """
        raw_backoff = self._settings.base_backoff_seconds * math.pow(2, attempt - 1)
        return min(raw_backoff, self._settings.max_backoff_seconds)

    def _payload_to_frame(self, payload: list[list[Any]], symbol: str, interval: str) -> pd.DataFrame:
        """Convert Binance kline payloads into a raw DataFrame.

        Args:
            payload: Binance kline payload.
            symbol: Trading symbol.
            interval: Candle interval.

        Returns:
            pd.DataFrame: Raw extracted frame with metadata columns.
        """
        columns = ["exchange", "symbol", "interval", *RAW_KLINE_COLUMNS, "load_source", "ingested_at"]
        if not payload:
            return pd.DataFrame(columns=columns)

        frame = pd.DataFrame(payload, columns=RAW_KLINE_COLUMNS)
        frame["exchange"] = "binance"
        frame["symbol"] = symbol
        frame["interval"] = interval
        frame["load_source"] = "binance_rest_api"
        frame["ingested_at"] = utc_now()
        return frame[columns]

    def fetch_historical_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> pd.DataFrame:
        """Fetch candles over a historical date range.

        Args:
            symbol: Trading symbol such as ``BTCUSDT``.
            interval: Candle interval such as ``1m``.
            start_time: Inclusive lower bound for extraction.
            end_time: Inclusive upper bound for extraction.

        Returns:
            pd.DataFrame: Concatenated raw market data.
        """
        start_cursor = ensure_utc(start_time)
        end_bound = ensure_utc(end_time)
        interval_delta = binance_interval_to_timedelta(interval)
        all_frames: list[pd.DataFrame] = []

        while start_cursor <= end_bound:
            payload = self._request(
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": int(start_cursor.timestamp() * 1000),
                    "endTime": int(end_bound.timestamp() * 1000),
                    "limit": 1000,
                },
            )
            batch = self._payload_to_frame(payload, symbol=symbol, interval=interval)
            if batch.empty:
                break

            all_frames.append(batch)

            last_open_time = pd.to_datetime(batch["open_time"].iloc[-1], unit="ms", utc=True).to_pydatetime()
            next_cursor = last_open_time + interval_delta
            if next_cursor <= start_cursor:
                break
            start_cursor = next_cursor

            if len(batch) < 1000:
                break

        if not all_frames:
            return self._payload_to_frame([], symbol=symbol, interval=interval)

        return pd.concat(all_frames, ignore_index=True)

    def fetch_recent_ohlcv(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        """Fetch the most recent candles from Binance.

        Args:
            symbol: Trading symbol such as ``BTCUSDT``.
            interval: Candle interval such as ``1m``.
            limit: Maximum number of rows to fetch.

        Returns:
            pd.DataFrame: Raw extracted market data.
        """
        payload = self._request(
            "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        return self._payload_to_frame(payload, symbol=symbol, interval=interval)

    def fetch_latest_price(self, symbol: str) -> dict[str, str]:
        """Fetch the latest spot price for a symbol.

        Args:
            symbol: Trading symbol such as ``BTCUSDT``.

        Returns:
            dict[str, str]: Latest price payload from Binance.
        """
        payload = self._request("/api/v3/ticker/price", {"symbol": symbol})
        if isinstance(payload, dict):
            return payload
        raise ApiRequestError("Unexpected payload returned by Binance ticker price endpoint")
