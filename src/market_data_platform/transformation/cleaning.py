"""Cleaning and normalization logic for OHLCV data."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from market_data_platform.schema import BASE_COLUMNS, NUMERIC_COLUMNS
from market_data_platform.utils.time_utils import binance_interval_to_pandas_freq, binance_interval_to_timedelta


def normalize_ohlcv_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize types and column order for raw OHLCV records.

    Args:
        frame: Raw extracted market data.

    Returns:
        pd.DataFrame: Typed and normalized frame.
    """
    working = frame.copy()

    for column in NUMERIC_COLUMNS:
        working[column] = pd.to_numeric(working[column], errors="coerce")

    working["trade_count"] = working["trade_count"].fillna(0).astype("int64")
    working["open_time"] = pd.to_datetime(working["open_time"], unit="ms", utc=True)
    working["close_time"] = pd.to_datetime(working["close_time"], unit="ms", utc=True)
    working["ingested_at"] = pd.to_datetime(working["ingested_at"], utc=True)
    working["is_synthetic"] = False

    return working[BASE_COLUMNS]


def _synthetic_close_time(open_time: pd.Timestamp, interval_delta: timedelta) -> pd.Timestamp:
    """Calculate the close timestamp for a synthetic bar.

    Args:
        open_time: Synthetic bar open time.
        interval_delta: Interval duration.

    Returns:
        pd.Timestamp: Synthetic close time.
    """
    return open_time + pd.to_timedelta(interval_delta) - pd.to_timedelta(1, unit="ms")


def fill_missing_bars(frame: pd.DataFrame, interval: str, enabled: bool) -> pd.DataFrame:
    """Reindex the frame and optionally fill missing bars with synthetic candles.

    Args:
        frame: Typed OHLCV frame.
        interval: Candle interval such as ``1m``.
        enabled: Whether missing bars should be synthesized.

    Returns:
        pd.DataFrame: Reindexed frame with gap handling applied.
    """
    if frame.empty:
        return frame

    if not enabled:
        return frame.sort_values("open_time").reset_index(drop=True)

    frequency = binance_interval_to_pandas_freq(interval)
    interval_delta = binance_interval_to_timedelta(interval)

    working = frame.sort_values("open_time").set_index("open_time")
    full_index = pd.date_range(
        start=working.index.min(),
        end=working.index.max(),
        freq=frequency,
        tz="UTC",
    )
    reindexed = working.reindex(full_index)
    reindexed.index.name = "open_time"
    missing_mask = reindexed["exchange"].isna()

    reindexed["exchange"] = reindexed["exchange"].ffill().bfill()
    reindexed["symbol"] = reindexed["symbol"].ffill().bfill()
    reindexed["interval"] = reindexed["interval"].ffill().bfill()
    reindexed["load_source"] = reindexed["load_source"].ffill().bfill()
    reindexed["ingested_at"] = reindexed["ingested_at"].ffill().bfill()
    reindexed["is_synthetic"] = missing_mask

    previous_close = reindexed["close"].ffill()
    for column in ["open", "high", "low", "close"]:
        reindexed.loc[missing_mask, column] = previous_close.loc[missing_mask]

    for column in [
        "volume",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]:
        reindexed.loc[missing_mask, column] = 0

    reindexed.loc[missing_mask, "close_time"] = [
        _synthetic_close_time(open_time=index_value, interval_delta=interval_delta)
        for index_value in reindexed.index[missing_mask]
    ]

    return reindexed.reset_index()[BASE_COLUMNS]


def clean_ohlcv_frame(frame: pd.DataFrame, interval: str, synthesize_missing_bars: bool) -> pd.DataFrame:
    """Clean raw OHLCV data and make it ready for downstream feature creation.

    Args:
        frame: Raw extracted market data.
        interval: Candle interval such as ``1m``.
        synthesize_missing_bars: Whether missing bars should be synthesized.

    Returns:
        pd.DataFrame: Cleaned OHLCV frame.
    """
    if frame.empty:
        return normalize_ohlcv_frame(frame)

    working = normalize_ohlcv_frame(frame)
    working = working.sort_values(["open_time", "ingested_at"])
    working = working.drop_duplicates(subset=["exchange", "symbol", "interval", "open_time"], keep="last")
    working = fill_missing_bars(working, interval=interval, enabled=synthesize_missing_bars)
    return working.sort_values("open_time").reset_index(drop=True)
