"""Simple threshold-strategy backtesting engine."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from market_data_platform.quant.metrics import (
    PerformanceMetrics,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
)
from market_data_platform.signals.models import ThresholdSignalRule
from market_data_platform.signals.thresholds import is_threshold_triggered
from market_data_platform.utils.time_utils import interval_to_periods_per_year


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """Structured output of a backtest run."""

    result_frame: pd.DataFrame
    metrics: PerformanceMetrics


class ThresholdBacktester:
    """Backtest a long-flat threshold breakout strategy."""

    def __init__(
        self,
        entry_rule: ThresholdSignalRule,
        exit_rule: ThresholdSignalRule,
        initial_capital: float,
        transaction_cost_bps: float,
    ) -> None:
        """Initialize the backtesting engine.

        Args:
            entry_rule: Threshold rule used to enter a long position.
            exit_rule: Threshold rule used to exit a long position.
            initial_capital: Initial portfolio value.
            transaction_cost_bps: Transaction cost in basis points per position change.
        """
        self._entry_rule = entry_rule
        self._exit_rule = exit_rule
        self._initial_capital = initial_capital
        self._transaction_cost_bps = transaction_cost_bps

    def _build_positions(self, frame: pd.DataFrame) -> pd.Series:
        """Build target positions from threshold rules.

        Args:
            frame: Historical feature frame.

        Returns:
            pd.Series: Long-flat target position series.
        """
        positions: list[int] = []
        current_position = 0
        previous_close: float | None = None

        for close in frame["close"]:
            if current_position == 0:
                enter_triggered, _ = is_threshold_triggered(
                    direction=self._entry_rule.direction,
                    previous_value=previous_close,
                    current_value=float(close),
                    threshold=self._entry_rule.threshold,
                )
                if enter_triggered:
                    current_position = 1
            else:
                exit_triggered, _ = is_threshold_triggered(
                    direction=self._exit_rule.direction,
                    previous_value=previous_close,
                    current_value=float(close),
                    threshold=self._exit_rule.threshold,
                )
                if exit_triggered:
                    current_position = 0

            positions.append(current_position)
            previous_close = float(close)

        return pd.Series(positions, index=frame.index, dtype="float64")

    def run(self, frame: pd.DataFrame, interval: str) -> BacktestResult:
        """Run a threshold strategy backtest on historical candles.

        Args:
            frame: Historical feature frame sorted by time.
            interval: Candle interval for annualization.

        Returns:
            BacktestResult: Detailed result frame and summary metrics.
        """
        if frame.empty:
            raise ValueError("Backtest input frame is empty")

        working = frame.sort_values("open_time").reset_index(drop=True).copy()
        working["target_position"] = self._build_positions(working)
        working["position"] = working["target_position"].shift(1).fillna(0.0)
        working["asset_return"] = working["close"].pct_change().fillna(0.0)
        working["turnover"] = working["target_position"].diff().abs().fillna(working["target_position"])
        working["transaction_cost"] = working["turnover"] * (self._transaction_cost_bps / 10000.0)
        working["strategy_return"] = (
            working["position"] * working["asset_return"] - working["transaction_cost"]
        )
        working["equity_curve"] = self._initial_capital * (1.0 + working["strategy_return"]).cumprod()

        periods_per_year = float(interval_to_periods_per_year(interval))
        total_periods = max(len(working), 1)
        total_return = float(working["equity_curve"].iloc[-1] / self._initial_capital - 1.0)
        annualized_return = float(
            (working["equity_curve"].iloc[-1] / self._initial_capital) ** (periods_per_year / total_periods)
            - 1.0
        )
        annualized_volatility = float(working["strategy_return"].std(ddof=0) * periods_per_year ** 0.5)
        sharpe_ratio = calculate_sharpe_ratio(working["strategy_return"], periods_per_year)
        max_drawdown = calculate_max_drawdown(working["equity_curve"])

        metrics = PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            annualized_volatility=annualized_volatility,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            trade_count=int(working["turnover"].gt(0).sum()),
        )
        return BacktestResult(result_frame=working, metrics=metrics)
