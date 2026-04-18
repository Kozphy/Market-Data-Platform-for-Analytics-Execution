"""Configuration loading for the market data pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _read_bool(name: str, default: bool) -> bool:
    """Read a boolean environment variable.

    Args:
        name: Environment variable name.
        default: Fallback value when the environment variable is not set.

    Returns:
        bool: Parsed boolean value.
    """
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_float(name: str, default: float) -> float:
    """Read a float environment variable.

    Args:
        name: Environment variable name.
        default: Fallback value when the environment variable is not set.

    Returns:
        float: Parsed float value.
    """
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return float(raw_value)


def _read_int(name: str, default: int) -> int:
    """Read an integer environment variable.

    Args:
        name: Environment variable name.
        default: Fallback value when the environment variable is not set.

    Returns:
        int: Parsed integer value.
    """
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return int(raw_value)


def _read_list(name: str, default: list[str]) -> list[str]:
    """Read a comma-separated list environment variable.

    Args:
        name: Environment variable name.
        default: Fallback values when the environment variable is not set.

    Returns:
        list[str]: Parsed non-empty values.
    """
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    values = [item.strip() for item in raw_value.split(",")]
    return [item for item in values if item]


def _read_semicolon_list(name: str, default: list[str]) -> list[str]:
    """Read a semicolon-separated list environment variable.

    Args:
        name: Environment variable name.
        default: Fallback values when the environment variable is not set.

    Returns:
        list[str]: Parsed non-empty values.
    """
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    values = [item.strip() for item in raw_value.split(";")]
    return [item for item in values if item]


@dataclass(frozen=True, slots=True)
class ApiSettings:
    """Settings for the market data API client."""

    base_url: str
    timeout_seconds: float
    max_retries: int
    base_backoff_seconds: float
    max_backoff_seconds: float

    @classmethod
    def from_env(cls) -> "ApiSettings":
        """Build API settings from the environment.

        Returns:
            ApiSettings: Parsed API client settings.
        """
        return cls(
            base_url=os.getenv("MARKET_DATA_API_BASE_URL", "https://api.binance.com"),
            timeout_seconds=_read_float("MARKET_DATA_API_TIMEOUT_SECONDS", 15.0),
            max_retries=_read_int("MARKET_DATA_API_MAX_RETRIES", 5),
            base_backoff_seconds=_read_float("MARKET_DATA_API_BASE_BACKOFF_SECONDS", 1.0),
            max_backoff_seconds=_read_float("MARKET_DATA_API_MAX_BACKOFF_SECONDS", 30.0),
        )


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """Settings for storage, scheduling, and pipeline defaults."""

    symbols: list[str]
    intervals: list[str]
    fill_missing_bars: bool
    parquet_root: Path
    state_path: Path
    write_parquet: bool
    write_postgres: bool
    postgres_dsn: str | None
    batch_lookback_minutes: int
    realtime_poll_seconds: int
    daily_batch_hour_utc: int

    @classmethod
    def from_env(cls) -> "PipelineSettings":
        """Build pipeline settings from the environment.

        Returns:
            PipelineSettings: Parsed pipeline settings.
        """
        return cls(
            symbols=_read_list("MARKET_DATA_PIPELINE_SYMBOLS", ["BTCUSDT", "ETHUSDT"]),
            intervals=_read_list("MARKET_DATA_PIPELINE_INTERVALS", ["1m", "5m"]),
            fill_missing_bars=_read_bool("MARKET_DATA_PIPELINE_FILL_MISSING_BARS", True),
            parquet_root=Path(os.getenv("MARKET_DATA_PIPELINE_PARQUET_ROOT", "./data/lakehouse")),
            state_path=Path(
                os.getenv(
                    "MARKET_DATA_PIPELINE_STATE_PATH",
                    "./data/state/pipeline_state.json",
                )
            ),
            write_parquet=_read_bool("MARKET_DATA_PIPELINE_WRITE_PARQUET", True),
            write_postgres=_read_bool("MARKET_DATA_PIPELINE_WRITE_POSTGRES", True),
            postgres_dsn=os.getenv("MARKET_DATA_POSTGRES_DSN"),
            batch_lookback_minutes=_read_int("MARKET_DATA_PIPELINE_BATCH_LOOKBACK_MINUTES", 1440),
            realtime_poll_seconds=_read_int("MARKET_DATA_PIPELINE_REALTIME_POLL_SECONDS", 30),
            daily_batch_hour_utc=_read_int("MARKET_DATA_PIPELINE_DAILY_BATCH_HOUR_UTC", 0),
        )

    def ensure_directories(self) -> None:
        """Create local storage directories required by the pipeline.

        Returns:
            None: This method creates directories for local pipeline artifacts.
        """
        self.parquet_root.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True, slots=True)
class StreamingSettings:
    """Settings for the polling-based streaming pipeline."""

    poll_seconds: int
    alerts_path: Path
    stream_ticks_path: Path
    alert_rules: list[str]

    @classmethod
    def from_env(cls) -> "StreamingSettings":
        """Build streaming settings from the environment.

        Returns:
            StreamingSettings: Parsed streaming pipeline settings.
        """
        return cls(
            poll_seconds=_read_int("MARKET_DATA_STREAM_POLL_SECONDS", 5),
            alerts_path=Path(
                os.getenv(
                    "MARKET_DATA_STREAM_ALERTS_PATH",
                    "./data/alerts/threshold_alerts.jsonl",
                )
            ),
            stream_ticks_path=Path(
                os.getenv(
                    "MARKET_DATA_STREAM_TICKS_PATH",
                    "./data/streaming/ticks",
                )
            ),
            alert_rules=_read_semicolon_list("MARKET_DATA_STREAM_RULES", []),
        )

    def ensure_directories(self) -> None:
        """Create local storage directories required by the streaming pipeline.

        Returns:
            None: This method creates directories for streaming artifacts.
        """
        self.alerts_path.parent.mkdir(parents=True, exist_ok=True)
        self.stream_ticks_path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True, slots=True)
class BacktestSettings:
    """Settings for the backtesting engine."""

    initial_capital: float
    transaction_cost_bps: float

    @classmethod
    def from_env(cls) -> "BacktestSettings":
        """Build backtest settings from the environment.

        Returns:
            BacktestSettings: Parsed backtest settings.
        """
        return cls(
            initial_capital=_read_float("MARKET_DATA_BACKTEST_INITIAL_CAPITAL", 100000.0),
            transaction_cost_bps=_read_float("MARKET_DATA_BACKTEST_TRANSACTION_COST_BPS", 5.0),
        )
