"""Unit tests for threshold-strategy backtesting."""

from __future__ import annotations

import math

import pandas as pd

from market_data_platform.quant.backtesting import ThresholdBacktester
from market_data_platform.signals.models import ThresholdSignalRule


def test_threshold_backtester_generates_positions_and_metrics() -> None:
    """Ensure the threshold backtester produces trades and summary metrics.

    Returns:
        None: Assertions validate backtest behavior.
    """
    frame = pd.DataFrame(
        {
            "exchange": ["binance"] * 8,
            "symbol": ["BTCUSDT"] * 8,
            "interval": ["1m"] * 8,
            "open_time": pd.date_range("2024-05-01", periods=8, freq="1min", tz="UTC"),
            "close_time": pd.date_range("2024-05-01 00:00:59", periods=8, freq="1min", tz="UTC"),
            "open": [95, 99, 101, 103, 100, 97, 94, 96],
            "high": [95, 99, 101, 103, 100, 97, 94, 96],
            "low": [95, 99, 101, 103, 100, 97, 94, 96],
            "close": [95, 99, 101, 103, 100, 97, 94, 96],
            "volume": [10] * 8,
            "quote_volume": [1000] * 8,
            "trade_count": [5] * 8,
            "taker_buy_base_volume": [4] * 8,
            "taker_buy_quote_volume": [400] * 8,
            "is_synthetic": [False] * 8,
            "load_source": ["binance_rest_api"] * 8,
            "ingested_at": pd.date_range("2024-05-01", periods=8, freq="1min", tz="UTC"),
            "return_1": [0.0] * 8,
            "return_5": [0.0] * 8,
            "return_20": [0.0] * 8,
            "log_return_1": [0.0] * 8,
            "ma_5": [0.0] * 8,
            "ma_20": [0.0] * 8,
            "ma_50": [0.0] * 8,
            "vol_5": [0.0] * 8,
            "vol_20": [0.0] * 8,
        }
    )

    backtester = ThresholdBacktester(
        entry_rule=ThresholdSignalRule(
            symbol="BTCUSDT",
            interval="1m",
            direction="cross_above",
            threshold=100.0,
            signal_name="entry",
        ),
        exit_rule=ThresholdSignalRule(
            symbol="BTCUSDT",
            interval="1m",
            direction="cross_below",
            threshold=98.0,
            signal_name="exit",
        ),
        initial_capital=100000.0,
        transaction_cost_bps=5.0,
    )

    result = backtester.run(frame, interval="1m")

    assert result.metrics.trade_count == 2
    assert list(result.result_frame["target_position"]) == [0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0]
    assert result.metrics.max_drawdown <= 0.0
    assert math.isfinite(result.metrics.sharpe_ratio)
