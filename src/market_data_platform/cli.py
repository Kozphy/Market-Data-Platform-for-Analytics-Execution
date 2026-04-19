"""Command line interface for the market data pipeline."""

from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path

from market_data_platform.config import ApiSettings, BacktestSettings, PipelineSettings, StreamingSettings
from market_data_platform.ingestion.binance import BinanceMarketDataSource
from market_data_platform.logging_utils import configure_logging
from market_data_platform.monitoring.metrics import JobMonitor
from market_data_platform.orchestration.data_quality import run_postgres_window_checks
from market_data_platform.orchestration.jobs import PipelineRunner, default_schema_sql_path
from market_data_platform.orchestration.staged_etl import (
    run_extract_ohlcv_window,
    run_load_gold_to_postgres_window,
    run_transform_bronze_to_silver_window,
    run_transform_silver_to_gold_window,
)
from market_data_platform.orchestration.scheduler import LightweightScheduler
from market_data_platform.quant.backtesting import ThresholdBacktester
from market_data_platform.signals.models import ThresholdSignalRule
from market_data_platform.signals.thresholds import parse_threshold_rules
from market_data_platform.storage.parquet import ParquetLakehouseWriter
from market_data_platform.storage.postgres import PostgresServingStore
from market_data_platform.storage.state import JsonStateStore
from market_data_platform.streaming.pipeline import PollingSignalPipeline
from market_data_platform.streaming.storage import JsonLineAlertSink, TickParquetWriter
from market_data_platform.transformation.cleaning import clean_ohlcv_frame
from market_data_platform.transformation.features import build_feature_frame
from market_data_platform.utils.time_utils import ensure_utc

LOGGER = logging.getLogger(__name__)


def _parse_timestamp(raw_value: str) -> datetime:
    """Parse a CLI timestamp and normalize it to UTC.

    Args:
        raw_value: Timestamp string in ISO-8601 format.

    Returns:
        datetime: Parsed UTC timestamp.
    """
    return ensure_utc(datetime.fromisoformat(raw_value))


def build_runner(project_root: Path) -> tuple[PipelineRunner, BinanceMarketDataSource]:
    """Build the pipeline runner from environment configuration.

    Args:
        project_root: Repository root path.

    Returns:
        tuple[PipelineRunner, BinanceMarketDataSource]: Runner and source client.
    """
    pipeline_settings = PipelineSettings.from_env()
    api_settings = ApiSettings.from_env()
    source = BinanceMarketDataSource(api_settings)
    parquet_writer = (
        ParquetLakehouseWriter(pipeline_settings.parquet_root)
        if pipeline_settings.write_parquet
        else None
    )
    postgres_store = (
        PostgresServingStore(
            dsn=pipeline_settings.postgres_dsn,
            schema_sql_path=default_schema_sql_path(project_root),
        )
        if pipeline_settings.write_postgres and pipeline_settings.postgres_dsn
        else None
    )
    runner = PipelineRunner(
        settings=pipeline_settings,
        source=source,
        parquet_writer=parquet_writer,
        state_store=JsonStateStore(pipeline_settings.state_path),
        monitor=JobMonitor(),
        postgres_store=postgres_store,
    )
    return runner, source


def build_streaming_pipeline(
    pipeline_settings: PipelineSettings,
    streaming_settings: StreamingSettings,
    rules: list[ThresholdSignalRule],
) -> tuple[PollingSignalPipeline, BinanceMarketDataSource]:
    """Build the streaming signal pipeline from environment configuration.

    Args:
        pipeline_settings: General pipeline settings.
        streaming_settings: Streaming pipeline settings.
        rules: Threshold rules to evaluate.

    Returns:
        tuple[PollingSignalPipeline, BinanceMarketDataSource]: Streaming pipeline and source client.
    """
    source = BinanceMarketDataSource(ApiSettings.from_env())
    pipeline = PollingSignalPipeline(
        settings=streaming_settings,
        source=source,
        state_store=JsonStateStore(pipeline_settings.state_path),
        tick_writer=TickParquetWriter(streaming_settings.stream_ticks_path),
        alert_sink=JsonLineAlertSink(streaming_settings.alerts_path),
        monitor=JobMonitor(),
        rules=rules,
    )
    return pipeline, source


def _build_feature_frame_for_backtest(
    source: BinanceMarketDataSource,
    pipeline_settings: PipelineSettings,
    symbol: str,
    interval: str,
    start_time: datetime,
    end_time: datetime,
):
    """Fetch, clean, and enrich historical data for a backtest run.

    Args:
        source: Binance market data source.
        pipeline_settings: General pipeline settings.
        symbol: Trading symbol.
        interval: Candle interval.
        start_time: Inclusive lower bound for extraction.
        end_time: Inclusive upper bound for extraction.

    Returns:
        pandas.DataFrame: Feature-rich historical market data.
    """
    raw_frame = source.fetch_historical_ohlcv(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
    )
    cleaned_frame = clean_ohlcv_frame(
        raw_frame,
        interval=interval,
        synthesize_missing_bars=pipeline_settings.fill_missing_bars,
    )
    return build_feature_frame(cleaned_frame)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        argparse.ArgumentParser: Configured parser with subcommands.
    """
    parser = argparse.ArgumentParser(description="Market data engineering pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Initialize directories and database schema.")

    historical = subparsers.add_parser("historical", help="Run a historical backfill.")
    historical.add_argument("--symbol", required=True)
    historical.add_argument("--interval", required=True)
    historical.add_argument("--start", required=True, help="ISO-8601 timestamp.")
    historical.add_argument("--end", required=True, help="ISO-8601 timestamp.")

    incremental = subparsers.add_parser("incremental", help="Run an incremental batch load.")
    incremental.add_argument("--symbol", required=True)
    incremental.add_argument("--interval", required=True)
    incremental.add_argument("--lookback-minutes", type=int, default=None)

    realtime = subparsers.add_parser("realtime", help="Run a realtime refresh.")
    realtime.add_argument("--symbol", required=True)
    realtime.add_argument("--interval", required=True)
    realtime.add_argument("--recent-bars", type=int, default=5)

    streaming = subparsers.add_parser("streaming", help="Run the streaming signal pipeline.")
    streaming.add_argument("--symbol", action="append", default=None)
    streaming.add_argument(
        "--rule",
        action="append",
        default=None,
        help="Threshold rule using symbol|direction|threshold|name or symbol|interval|direction|threshold|name.",
    )
    streaming.add_argument("--poll-seconds", type=int, default=None)
    streaming.add_argument("--run-once", action="store_true")

    backtest = subparsers.add_parser("backtest", help="Run a simple threshold-strategy backtest.")
    backtest.add_argument("--symbol", required=True)
    backtest.add_argument("--interval", required=True)
    backtest.add_argument("--start", required=True, help="ISO-8601 timestamp.")
    backtest.add_argument("--end", required=True, help="ISO-8601 timestamp.")
    backtest.add_argument("--entry-threshold", type=float, required=True)
    backtest.add_argument("--exit-threshold", type=float, required=True)
    backtest.add_argument("--output-path", default=None)

    subparsers.add_parser("scheduler", help="Run the lightweight scheduler.")

    def _add_window_args(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--symbol", required=True)
        sub.add_argument("--interval", required=True)
        sub.add_argument("--window-start", required=True, help="Inclusive ISO-8601 UTC window start.")
        sub.add_argument("--window-end", required=True, help="Inclusive ISO-8601 UTC window end.")

    extract_airflow = subparsers.add_parser(
        "extract-ohlcv",
        help="Airflow stage: fetch Binance OHLCV for a window and write bronze Parquet only.",
    )
    _add_window_args(extract_airflow)

    to_silver = subparsers.add_parser(
        "transform-bronze-to-silver",
        help="Airflow stage: read bronze for a window, clean, write silver Parquet.",
    )
    _add_window_args(to_silver)

    to_gold = subparsers.add_parser(
        "transform-silver-to-gold",
        help="Airflow stage: read silver for a window, engineer features, write gold Parquet.",
    )
    _add_window_args(to_gold)

    load_pg = subparsers.add_parser(
        "load-gold-to-postgres",
        help="Airflow stage: read gold for a window and upsert into PostgreSQL.",
    )
    _add_window_args(load_pg)

    dq = subparsers.add_parser(
        "data-quality-check",
        help="Airflow stage: validate Postgres rows for a window (dup PK, gaps, non-empty).",
    )
    _add_window_args(dq)

    return parser


def main() -> None:
    """Execute the command line interface.

    Returns:
        None: This function dispatches to the requested subcommand.
    """
    configure_logging(level=logging.INFO)
    parser = build_argument_parser()
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parents[2]
    runner, source = build_runner(project_root)

    try:
        if args.command == "bootstrap":
            runner.bootstrap()
            LOGGER.info("Bootstrap completed")
        elif args.command == "historical":
            runner.bootstrap()
            rows = runner.run_historical_backfill(
                symbol=args.symbol,
                interval=args.interval,
                start_time=_parse_timestamp(args.start),
                end_time=_parse_timestamp(args.end),
            )
            LOGGER.info("Historical load completed", extra={"payload": {"rows_loaded": rows}})
        elif args.command == "incremental":
            runner.bootstrap()
            rows = runner.run_incremental_batch(
                symbol=args.symbol,
                interval=args.interval,
                lookback_minutes=args.lookback_minutes,
            )
            LOGGER.info("Incremental load completed", extra={"payload": {"rows_loaded": rows}})
        elif args.command == "realtime":
            runner.bootstrap()
            rows = runner.run_realtime_update(
                symbol=args.symbol,
                interval=args.interval,
                recent_bars=args.recent_bars,
            )
            LOGGER.info("Realtime load completed", extra={"payload": {"rows_loaded": rows}})
        elif args.command == "scheduler":
            runner.bootstrap()
            LightweightScheduler(PipelineSettings.from_env(), runner).run_forever()
        elif args.command == "streaming":
            pipeline_settings = PipelineSettings.from_env()
            streaming_settings = StreamingSettings.from_env()
            rule_strings = args.rule if args.rule is not None else streaming_settings.alert_rules
            rules = parse_threshold_rules(rule_strings)
            if args.poll_seconds is not None:
                streaming_settings = replace(streaming_settings, poll_seconds=args.poll_seconds)
            symbols = args.symbol if args.symbol is not None else pipeline_settings.symbols
            streaming_pipeline, streaming_source = build_streaming_pipeline(
                pipeline_settings=pipeline_settings,
                streaming_settings=streaming_settings,
                rules=rules,
            )
            try:
                streaming_pipeline.bootstrap()
                if args.run_once:
                    counts = streaming_pipeline.run_once(symbols)
                    LOGGER.info("Streaming run completed", extra={"payload": counts})
                else:
                    streaming_pipeline.run_forever(symbols)
            finally:
                streaming_source.close()
        elif args.command == "extract-ohlcv":
            runner.bootstrap()
            rows = run_extract_ohlcv_window(
                symbol=args.symbol,
                interval=args.interval,
                window_start=_parse_timestamp(args.window_start),
                window_end=_parse_timestamp(args.window_end),
            )
            LOGGER.info("Extract stage completed", extra={"payload": {"rows_bronze": rows}})
        elif args.command == "transform-bronze-to-silver":
            runner.bootstrap()
            rows = run_transform_bronze_to_silver_window(
                symbol=args.symbol,
                interval=args.interval,
                window_start=_parse_timestamp(args.window_start),
                window_end=_parse_timestamp(args.window_end),
            )
            LOGGER.info("Silver transform completed", extra={"payload": {"rows_silver": rows}})
        elif args.command == "transform-silver-to-gold":
            runner.bootstrap()
            rows = run_transform_silver_to_gold_window(
                symbol=args.symbol,
                interval=args.interval,
                window_start=_parse_timestamp(args.window_start),
                window_end=_parse_timestamp(args.window_end),
            )
            LOGGER.info("Gold transform completed", extra={"payload": {"rows_gold": rows}})
        elif args.command == "load-gold-to-postgres":
            runner.bootstrap()
            rows = run_load_gold_to_postgres_window(
                project_root=project_root,
                symbol=args.symbol,
                interval=args.interval,
                window_start=_parse_timestamp(args.window_start),
                window_end=_parse_timestamp(args.window_end),
            )
            LOGGER.info("Postgres load completed", extra={"payload": {"rows_upserted": rows}})
        elif args.command == "data-quality-check":
            runner.bootstrap()
            report = run_postgres_window_checks(
                symbol=args.symbol,
                interval=args.interval,
                window_start=_parse_timestamp(args.window_start),
                window_end=_parse_timestamp(args.window_end),
            )
            LOGGER.info("Data quality completed", extra={"payload": asdict(report)})
        elif args.command == "backtest":
            pipeline_settings = PipelineSettings.from_env()
            backtest_settings = BacktestSettings.from_env()
            feature_frame = _build_feature_frame_for_backtest(
                source=source,
                pipeline_settings=pipeline_settings,
                symbol=args.symbol,
                interval=args.interval,
                start_time=_parse_timestamp(args.start),
                end_time=_parse_timestamp(args.end),
            )
            entry_rule = ThresholdSignalRule(
                symbol=args.symbol,
                interval=args.interval,
                direction="cross_above",
                threshold=args.entry_threshold,
                signal_name=f"{args.symbol.lower()}_entry",
            )
            exit_rule = ThresholdSignalRule(
                symbol=args.symbol,
                interval=args.interval,
                direction="cross_below",
                threshold=args.exit_threshold,
                signal_name=f"{args.symbol.lower()}_exit",
            )
            backtester = ThresholdBacktester(
                entry_rule=entry_rule,
                exit_rule=exit_rule,
                initial_capital=backtest_settings.initial_capital,
                transaction_cost_bps=backtest_settings.transaction_cost_bps,
            )
            result = backtester.run(feature_frame, interval=args.interval)
            LOGGER.info("Backtest completed", extra={"payload": result.metrics.to_payload()})
            if args.output_path:
                output_path = Path(args.output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                result.result_frame.to_csv(output_path, index=False)
    finally:
        source.close()


if __name__ == "__main__":
    main()
