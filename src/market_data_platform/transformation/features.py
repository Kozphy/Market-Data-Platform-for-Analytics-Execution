"""Feature engineering for time-series market data."""

from __future__ import annotations

import numpy as np
import pandas as pd

from market_data_platform.schema import FEATURE_COLUMNS, SERVING_COLUMNS


def add_return_features(frame: pd.DataFrame, windows: list[int] | None = None) -> pd.DataFrame:
    """Add simple and log return features.

    Args:
        frame: Cleaned OHLCV frame.
        windows: Rolling windows for simple returns.

    Returns:
        pd.DataFrame: Frame with return features appended.
    """
    windows = windows or [1, 5, 20]
    working = frame.copy()
    for window in windows:
        working[f"return_{window}"] = working["close"].pct_change(periods=window)
    working["log_return_1"] = np.log(working["close"]).diff()
    return working


def add_moving_average_features(frame: pd.DataFrame, windows: list[int] | None = None) -> pd.DataFrame:
    """Add moving-average features.

    Args:
        frame: Cleaned OHLCV frame.
        windows: Rolling windows for moving averages.

    Returns:
        pd.DataFrame: Frame with moving-average features appended.
    """
    windows = windows or [5, 20, 50]
    working = frame.copy()
    for window in windows:
        working[f"ma_{window}"] = working["close"].rolling(window=window, min_periods=window).mean()
    return working


def add_volatility_features(frame: pd.DataFrame, windows: list[int] | None = None) -> pd.DataFrame:
    """Add rolling volatility features based on simple returns.

    Args:
        frame: Frame with OHLCV and return columns.
        windows: Rolling windows for volatility.

    Returns:
        pd.DataFrame: Frame with volatility features appended.
    """
    windows = windows or [5, 20]
    working = frame.copy()
    base_returns = working["close"].pct_change()
    for window in windows:
        working[f"vol_{window}"] = base_returns.rolling(window=window, min_periods=window).std(ddof=0)
    return working


def build_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Build the final feature-rich serving frame.

    Args:
        frame: Cleaned OHLCV frame.

    Returns:
        pd.DataFrame: Final serving frame with engineered features.
    """
    working = add_return_features(frame)
    working = add_moving_average_features(working)
    working = add_volatility_features(working)

    for column in FEATURE_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA

    return working[SERVING_COLUMNS].sort_values("open_time").reset_index(drop=True)
