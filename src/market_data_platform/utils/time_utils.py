"""Time and interval helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

BINANCE_TO_PANDAS_FREQ = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1D",
}


def binance_interval_to_pandas_freq(interval: str) -> str:
    """Translate a Binance interval into a pandas frequency string.

    Args:
        interval: Binance interval such as ``1m`` or ``1h``.

    Returns:
        str: Equivalent pandas frequency string.

    Raises:
        ValueError: If the interval is unsupported.
    """
    if interval not in BINANCE_TO_PANDAS_FREQ:
        raise ValueError(f"Unsupported Binance interval: {interval}")
    return BINANCE_TO_PANDAS_FREQ[interval]


def binance_interval_to_timedelta(interval: str) -> timedelta:
    """Translate a Binance interval into a ``timedelta``.

    Args:
        interval: Binance interval such as ``1m`` or ``1h``.

    Returns:
        timedelta: Equivalent time delta.
    """
    pandas_frequency = binance_interval_to_pandas_freq(interval)
    return pd.to_timedelta(pandas_frequency).to_pytimedelta()


def ensure_utc(value: datetime) -> datetime:
    """Normalize a ``datetime`` to timezone-aware UTC.

    Args:
        value: Input datetime.

    Returns:
        datetime: Timezone-aware UTC datetime.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp.

    Returns:
        datetime: Current UTC timestamp.
    """
    return datetime.now(timezone.utc)


def interval_to_periods_per_year(interval: str) -> float:
    """Estimate the number of periods per year for a bar interval.

    Args:
        interval: Binance interval such as ``1m`` or ``1h``.

    Returns:
        float: Approximate periods per year for annualization formulas.
    """
    delta = binance_interval_to_timedelta(interval)
    return pd.to_timedelta("365D") / pd.to_timedelta(delta)
