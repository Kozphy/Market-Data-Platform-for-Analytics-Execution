"""Staged ETL steps for Airflow (or manual) orchestration over a fixed time window."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from market_data_platform.config import ApiSettings, PipelineSettings
from market_data_platform.ingestion.binance import BinanceMarketDataSource
from market_data_platform.monitoring.metrics import JobMonitor
from market_data_platform.orchestration.jobs import default_schema_sql_path
from market_data_platform.storage.parquet import ParquetLakehouseWriter
from market_data_platform.storage.parquet_scan import read_parquet_window
from market_data_platform.storage.postgres import PostgresServingStore
from market_data_platform.transformation.cleaning import clean_ohlcv_frame
from market_data_platform.transformation.features import build_feature_frame
from market_data_platform.utils.time_utils import ensure_utc

LOGGER = logging.getLogger(__name__)


def run_extract_ohlcv_window(
    symbol: str,
    interval: str,
    window_start: datetime,
    window_end: datetime,
) -> int:
    """Fetch OHLCV from Binance for ``[window_start, window_end]`` and write bronze only.

    Idempotent with respect to PostgreSQL; re-running may append additional bronze Parquet
    files, but downstream cleaning deduplicates on ``(exchange, symbol, interval, open_time)``.
    """
    settings = PipelineSettings.from_env()
    settings.ensure_directories()
    if not settings.write_parquet:
        raise RuntimeError("MARKET_DATA_PIPELINE_WRITE_PARQUET must be true for staged ETL")

    monitor = JobMonitor()
    summary = monitor.start(
        job_name="airflow_extract_ohlcv",
        exchange="binance",
        symbol=symbol,
        interval=interval,
    )
    source = BinanceMarketDataSource(ApiSettings.from_env())
    writer = ParquetLakehouseWriter(settings.parquet_root)
    try:
        raw_frame = source.fetch_historical_ohlcv(
            symbol=symbol,
            interval=interval,
            start_time=ensure_utc(window_start),
            end_time=ensure_utc(window_end),
        )
        summary.rows_extracted = len(raw_frame)
        rows = writer.write_layer(raw_frame, layer="bronze")
        summary.rows_loaded = rows
        monitor.success(summary)
        LOGGER.info(
            "Extract stage completed",
            extra={"payload": {"symbol": symbol, "interval": interval, "rows": rows}},
        )
        return rows
    except Exception as exc:
        monitor.failure(summary, exc)
        raise
    finally:
        source.close()


def run_transform_bronze_to_silver_window(
    symbol: str,
    interval: str,
    window_start: datetime,
    window_end: datetime,
) -> int:
    """Read bronze for the window, clean, write silver."""
    settings = PipelineSettings.from_env()
    settings.ensure_directories()
    if not settings.write_parquet:
        raise RuntimeError("MARKET_DATA_PIPELINE_WRITE_PARQUET must be true for staged ETL")

    monitor = JobMonitor()
    summary = monitor.start(
        job_name="airflow_transform_bronze_to_silver",
        exchange="binance",
        symbol=symbol,
        interval=interval,
    )
    try:
        raw_frame = read_parquet_window(
            settings.parquet_root,
            layer="bronze",
            exchange="binance",
            symbol=symbol,
            interval=interval,
            window_start=window_start,
            window_end=window_end,
        )
        summary.rows_extracted = len(raw_frame)
        cleaned = clean_ohlcv_frame(
            raw_frame,
            interval=interval,
            synthesize_missing_bars=settings.fill_missing_bars,
        )
        summary.rows_cleaned = len(cleaned)
        writer = ParquetLakehouseWriter(settings.parquet_root)
        rows = writer.write_layer(cleaned, layer="silver")
        summary.rows_loaded = rows
        monitor.success(summary)
        LOGGER.info(
            "Bronze to silver completed",
            extra={"payload": {"symbol": symbol, "interval": interval, "rows": rows}},
        )
        return rows
    except Exception as exc:
        monitor.failure(summary, exc)
        raise


def run_transform_silver_to_gold_window(
    symbol: str,
    interval: str,
    window_start: datetime,
    window_end: datetime,
) -> int:
    """Read silver for the window, build features, write gold."""
    settings = PipelineSettings.from_env()
    settings.ensure_directories()
    if not settings.write_parquet:
        raise RuntimeError("MARKET_DATA_PIPELINE_WRITE_PARQUET must be true for staged ETL")

    monitor = JobMonitor()
    summary = monitor.start(
        job_name="airflow_transform_silver_to_gold",
        exchange="binance",
        symbol=symbol,
        interval=interval,
    )
    try:
        silver = read_parquet_window(
            settings.parquet_root,
            layer="silver",
            exchange="binance",
            symbol=symbol,
            interval=interval,
            window_start=window_start,
            window_end=window_end,
        )
        summary.rows_extracted = len(silver)
        gold = build_feature_frame(silver)
        summary.rows_cleaned = len(gold)
        writer = ParquetLakehouseWriter(settings.parquet_root)
        rows = writer.write_layer(gold, layer="gold")
        summary.rows_loaded = rows
        monitor.success(summary)
        LOGGER.info(
            "Silver to gold completed",
            extra={"payload": {"symbol": symbol, "interval": interval, "rows": rows}},
        )
        return rows
    except Exception as exc:
        monitor.failure(summary, exc)
        raise


def run_load_gold_to_postgres_window(
    project_root: Path,
    symbol: str,
    interval: str,
    window_start: datetime,
    window_end: datetime,
) -> int:
    """Read gold for the window and upsert into PostgreSQL."""
    settings = PipelineSettings.from_env()
    settings.ensure_directories()
    if not settings.write_postgres or not settings.postgres_dsn:
        raise RuntimeError("PostgreSQL writes must be enabled with MARKET_DATA_POSTGRES_DSN set")

    monitor = JobMonitor()
    summary = monitor.start(
        job_name="airflow_load_gold_to_postgres",
        exchange="binance",
        symbol=symbol,
        interval=interval,
    )
    store = PostgresServingStore(
        dsn=settings.postgres_dsn,
        schema_sql_path=default_schema_sql_path(project_root),
    )
    try:
        store.ensure_schema()
        gold = read_parquet_window(
            settings.parquet_root,
            layer="gold",
            exchange="binance",
            symbol=symbol,
            interval=interval,
            window_start=window_start,
            window_end=window_end,
        )
        summary.rows_extracted = len(gold)
        loaded = store.upsert_market_data(gold)
        summary.rows_loaded = loaded
        monitor.success(summary)
        LOGGER.info(
            "Load to Postgres completed",
            extra={"payload": {"symbol": symbol, "interval": interval, "rows": loaded}},
        )
        return loaded
    except Exception as exc:
        monitor.failure(summary, exc)
        raise
