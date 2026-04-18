"""Performance metrics for backtesting results."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class PerformanceMetrics:
    """Summary performance metrics for a backtest."""

    total_return: float
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: float
    max_drawdown: float
    trade_count: int

    def to_payload(self) -> dict[str, float | int]:
        """Serialize metrics for logging or display.

        Returns:
            dict[str, float | int]: JSON-friendly metrics payload.
        """
        return {
            "total_return": self.total_return,
            "annualized_return": self.annualized_return,
            "annualized_volatility": self.annualized_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "trade_count": self.trade_count,
        }


def calculate_max_drawdown(equity_curve: pd.Series) -> float:
    """Calculate maximum drawdown from an equity curve.

    Args:
        equity_curve: Backtest equity curve.

    Returns:
        float: Minimum drawdown as a negative decimal.
    """
    running_peak = equity_curve.cummax()
    drawdown = equity_curve / running_peak - 1.0
    return float(drawdown.min()) if not drawdown.empty else 0.0


def calculate_sharpe_ratio(strategy_returns: pd.Series, periods_per_year: float) -> float:
    """Calculate an annualized Sharpe ratio from periodic returns.

    Args:
        strategy_returns: Periodic strategy returns.
        periods_per_year: Annualization factor.

    Returns:
        float: Annualized Sharpe ratio.
    """
    volatility = float(strategy_returns.std(ddof=0))
    if volatility == 0.0:
        return 0.0
    return float(strategy_returns.mean() / volatility * periods_per_year ** 0.5)
