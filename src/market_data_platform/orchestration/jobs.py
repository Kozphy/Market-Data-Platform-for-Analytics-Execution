"""Job orchestration for historical, batch, and realtime loads."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from market_data_platform.config import PipelineSettings
from market_data_platform.ingestion.base import MarketDataSource
from market_data_platform.monitoring.metrics import JobMonitor
from market_data_platform.storage.parquet import ParquetLakehouseWriter
from market_data_platform.storage.postgres import PostgresServingStore
from market_data_platform.storage.state import JsonStateStore
from market_data_platform.transformation.cleaning import clean_ohlcv_frame
from market_data_platform.transformation.features import build_feature_frame
from market_data_platform.utils.time_utils import binance_interval_to_timedelta, ensure_utc, utc_now


class PipelineRunner:
    """Run end-to-end ETL jobs for market data."""

    def __init__(
        self,
        settings: PipelineSettings,
        source: MarketDataSource,
        parquet_writer: ParquetLakehouseWriter | None,
        state_store: JsonStateStore,
        monitor: JobMonitor,
        postgres_store: PostgresServingStore | None = None,
    ) -> None:
        """Initialize pipeline dependencies.

        Args:
            settings: Pipeline configuration.
            source: Market data source.
            parquet_writer: Optional Parquet writer.
            state_store: Checkpoint state store.
            monitor: Job lifecycle monitor.
            postgres_store: Optional PostgreSQL serving store.
        """
        self._settings = settings
        self._source = source
        self._parquet_writer = parquet_writer
        self._state_store = state_store
        self._monitor = monitor
        self._postgres_store = postgres_store

    def bootstrap(self) -> None:
        """Bootstrap storage prerequisites.

        Returns:
            None: This method prepares local directories and database schema.
        """
        self._settings.ensure_directories()
        if self._postgres_store:
            self._postgres_store.ensure_schema()

    def _checkpoint_key(self, job_name: str, symbol: str, interval: str) -> str:
        """Create a checkpoint key for a job and symbol.

        Args:
            job_name: Job identifier.
            symbol: Trading symbol.
            interval: Candle interval.

        Returns:
            str: Composite checkpoint key.
        """
        return f"{job_name}:binance:{symbol}:{interval}"

    def _persist_outputs(
        self,
        raw_frame: pd.DataFrame,
        cleaned_frame: pd.DataFrame,
        feature_frame: pd.DataFrame,
    ) -> int:
        """Persist raw, cleaned, and feature-rich outputs.

        Args:
            raw_frame: Raw extracted frame.
            cleaned_frame: Cleaned OHLCV frame.
            feature_frame: Final serving frame.

        Returns:
            int: Total rows loaded into the primary serving store.
        """
        loaded_rows = 0
        if self._parquet_writer:
            self._parquet_writer.write_layer(raw_frame, layer="bronze")
            self._parquet_writer.write_layer(cleaned_frame, layer="silver")
            self._parquet_writer.write_layer(feature_frame, layer="gold")

        if self._postgres_store:
            loaded_rows = self._postgres_store.upsert_market_data(feature_frame)

        return loaded_rows if loaded_rows else len(feature_frame)

    def _run_window(
        self,
        job_name: str,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Extract, transform, and load one processing window.

        Args:
            job_name: Job name for monitoring and checkpoints.
            symbol: Trading symbol.
            interval: Candle interval.
            start_time: Inclusive lower bound for extraction.
            end_time: Inclusive upper bound for extraction.

        Returns:
            int: Number of rows loaded.
        """
        summary = self._monitor.start(
            job_name=job_name,
            exchange="binance",
            symbol=symbol,
            interval=interval,
        )

        try:
            raw_frame = self._source.fetch_historical_ohlcv(
                symbol=symbol,
                interval=interval,
                start_time=start_time,
                end_time=end_time,
            )
            summary.rows_extracted = len(raw_frame)

            cleaned_frame = clean_ohlcv_frame(
                raw_frame,
                interval=interval,
                synthesize_missing_bars=self._settings.fill_missing_bars,
            )
            summary.rows_cleaned = len(cleaned_frame)

            feature_frame = build_feature_frame(cleaned_frame)
            summary.rows_loaded = self._persist_outputs(raw_frame, cleaned_frame, feature_frame)
            summary.metadata["window_start"] = ensure_utc(start_time).isoformat()
            summary.metadata["window_end"] = ensure_utc(end_time).isoformat()

            if not feature_frame.empty:
                checkpoint_key = self._checkpoint_key(job_name, symbol=symbol, interval=interval)
                self._state_store.write_checkpoint(
                    checkpoint_key,
                    {
                        "last_success_open_time": feature_frame["open_time"].max().isoformat(),
                        "last_row_count": len(feature_frame),
                        "updated_at": utc_now().isoformat(),
                    },
                )

            self._monitor.success(summary)
            return summary.rows_loaded
        except Exception as error:
            self._monitor.failure(summary, error)
            raise

    def run_historical_backfill(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Run a historical backfill for a symbol and interval.

        Args:
            symbol: Trading symbol.
            interval: Candle interval.
            start_time: Inclusive lower bound for extraction.
            end_time: Inclusive upper bound for extraction.

        Returns:
            int: Number of rows loaded.
        """
        return self._run_window(
            job_name="historical_backfill",
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )

    def run_incremental_batch(self, symbol: str, interval: str, lookback_minutes: int | None = None) -> int:
        """Run an incremental batch load using a persisted watermark.

        Args:
            symbol: Trading symbol.
            interval: Candle interval.
            lookback_minutes: Fallback lookback when no checkpoint exists.

        Returns:
            int: Number of rows loaded.
        """
        job_name = "incremental_batch"
        checkpoint_key = self._checkpoint_key(job_name, symbol=symbol, interval=interval)
        checkpoint = self._state_store.get_checkpoint(checkpoint_key)
        lookback = lookback_minutes or self._settings.batch_lookback_minutes
        end_time = utc_now()

        if checkpoint and checkpoint.get("last_success_open_time"):
            start_time = datetime.fromisoformat(checkpoint["last_success_open_time"]).astimezone(timezone.utc)
            start_time = start_time + binance_interval_to_timedelta(interval)
        else:
            start_time = end_time - timedelta(minutes=lookback)

        return self._run_window(
            job_name=job_name,
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )

    def run_realtime_update(self, symbol: str, interval: str, recent_bars: int = 5) -> int:
        """Run a short-window realtime refresh.

        Args:
            symbol: Trading symbol.
            interval: Candle interval.
            recent_bars: Number of recent candles to refresh.

        Returns:
            int: Number of rows loaded.
        """
        summary = self._monitor.start(
            job_name="realtime_update",
            exchange="binance",
            symbol=symbol,
            interval=interval,
        )

        try:
            raw_frame = self._source.fetch_recent_ohlcv(symbol=symbol, interval=interval, limit=recent_bars)
            summary.rows_extracted = len(raw_frame)
            cleaned_frame = clean_ohlcv_frame(
                raw_frame,
                interval=interval,
                synthesize_missing_bars=self._settings.fill_missing_bars,
            )
            summary.rows_cleaned = len(cleaned_frame)
            feature_frame = build_feature_frame(cleaned_frame)
            summary.rows_loaded = self._persist_outputs(raw_frame, cleaned_frame, feature_frame)
            summary.metadata["recent_bars"] = recent_bars
            self._monitor.success(summary)
            return summary.rows_loaded
        except Exception as error:
            self._monitor.failure(summary, error)
            raise


def default_schema_sql_path(project_root: Path) -> Path:
    """Resolve the default schema SQL path.

    Args:
        project_root: Repository root path.

    Returns:
        Path: Default schema SQL file path.
    """
    return project_root / "sql" / "postgres" / "001_create_market_data.sql"
