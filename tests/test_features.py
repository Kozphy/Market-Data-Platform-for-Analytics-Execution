"""Unit tests for time-series feature engineering."""

from __future__ import annotations

import pandas as pd

from market_data_platform.transformation.features import build_feature_frame


def test_build_feature_frame_adds_returns_and_rolling_metrics() -> None:
    """Ensure the serving frame includes portfolio-relevant technical features.

    Returns:
        None: Assertions validate expected feature behavior.
    """
    frame = pd.DataFrame(
        {
            "exchange": ["binance"] * 6,
            "symbol": ["BTCUSDT"] * 6,
            "interval": ["1m"] * 6,
            "open_time": pd.date_range("2024-05-01", periods=6, freq="1min", tz="UTC"),
            "close_time": pd.date_range("2024-05-01 00:00:59", periods=6, freq="1min", tz="UTC"),
            "open": [100, 101, 102, 103, 104, 105],
            "high": [101, 102, 103, 104, 105, 106],
            "low": [99, 100, 101, 102, 103, 104],
            "close": [100, 101, 102, 103, 104, 105],
            "volume": [10, 11, 12, 13, 14, 15],
            "quote_volume": [1000, 1111, 1224, 1339, 1456, 1575],
            "trade_count": [5, 5, 6, 6, 7, 7],
            "taker_buy_base_volume": [4, 4, 5, 5, 6, 6],
            "taker_buy_quote_volume": [400, 404, 510, 515, 624, 630],
            "is_synthetic": [False] * 6,
            "load_source": ["binance_rest_api"] * 6,
            "ingested_at": pd.date_range("2024-05-01", periods=6, freq="1min", tz="UTC"),
        }
    )

    feature_frame = build_feature_frame(frame)

    assert "return_1" in feature_frame.columns
    assert "ma_5" in feature_frame.columns
    assert "vol_5" in feature_frame.columns
    assert round(feature_frame.loc[1, "return_1"], 6) == 0.01
    assert round(feature_frame.loc[4, "ma_5"], 6) == 102.0
    assert feature_frame.loc[5, "vol_5"] > 0
