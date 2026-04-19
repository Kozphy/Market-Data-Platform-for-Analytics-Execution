"""Airflow DAG orchestrating staged market data ETL (Binance -> lakehouse -> Postgres -> DQ)."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOGGER = logging.getLogger(__name__)


def _dag_failure_alert(context) -> None:
    """Optional Slack webhook alert; otherwise emit a structured error log."""
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    run_id = context.get("run_id")
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    exception = context.get("exception")

    message = (
        f":rotating_light: Airflow DAG failure\n"
        f"- dag: `{dag_id}`\n"
        f"- run_id: `{run_id}`\n"
        f"- task: `{task_id}`\n"
        f"- exception: `{exception}`\n"
    )

    if webhook:
        try:
            payload = json.dumps({"text": message}).encode("utf-8")
            request = urllib.request.Request(
                webhook,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status >= 400:
                    LOGGER.error("Slack webhook returned HTTP %s", response.status)
        except urllib.error.URLError as exc:
            LOGGER.error("Slack webhook post failed: %s", exc)
    else:
        LOGGER.error("DAG failure (set SLACK_WEBHOOK_URL for Slack): %s", message)


def _default_symbol() -> str:
    raw = os.environ.get("MARKET_DATA_AIRFLOW_SYMBOL") or os.environ.get(
        "MARKET_DATA_PIPELINE_SYMBOLS", "BTCUSDT"
    )
    return raw.split(",")[0].strip()


def _default_interval() -> str:
    raw = os.environ.get("MARKET_DATA_AIRFLOW_INTERVAL") or os.environ.get(
        "MARKET_DATA_PIPELINE_INTERVALS", "1m"
    )
    return raw.split(",")[0].strip()


with DAG(
    dag_id="market_data_pipeline",
    description="Staged Binance OHLCV ETL with lakehouse layers and Postgres serving checks.",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Backfill: set catchup=True for this start_date range, or use `airflow dags backfill`.
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "market-data",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _dag_failure_alert,
    },
    params={
        "symbol": _default_symbol(),
        "interval": _default_interval(),
    },
    tags=["market-data", "binance", "etl"],
) as dag:
    # Airflow data intervals are half-open [start, end). Binance windows are inclusive on both ends
    # for this demo, so shrink the exclusive end by one microsecond.
    window_start = "{{ data_interval_start.in_timezone('UTC').isoformat() }}"
    window_end = "{{ data_interval_end.subtract(microseconds=1).in_timezone('UTC').isoformat() }}"

    extract_ohlcv = BashOperator(
        task_id="extract_ohlcv",
        bash_command=f"python -m market_data_platform.cli extract-ohlcv "
        f"--symbol {{{{ params.symbol }}}} "
        f"--interval {{{{ params.interval }}}} "
        f"--window-start '{window_start}' "
        f"--window-end '{window_end}'",
    )

    transform_bronze_to_silver = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command=f"python -m market_data_platform.cli transform-bronze-to-silver "
        f"--symbol {{{{ params.symbol }}}} "
        f"--interval {{{{ params.interval }}}} "
        f"--window-start '{window_start}' "
        f"--window-end '{window_end}'",
    )

    transform_silver_to_gold = BashOperator(
        task_id="transform_silver_to_gold",
        bash_command=f"python -m market_data_platform.cli transform-silver-to-gold "
        f"--symbol {{{{ params.symbol }}}} "
        f"--interval {{{{ params.interval }}}} "
        f"--window-start '{window_start}' "
        f"--window-end '{window_end}'",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python -m market_data_platform.cli load-gold-to-postgres "
        f"--symbol {{{{ params.symbol }}}} "
        f"--interval {{{{ params.interval }}}} "
        f"--window-start '{window_start}' "
        f"--window-end '{window_end}'",
    )

    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=f"python -m market_data_platform.cli data-quality-check "
        f"--symbol {{{{ params.symbol }}}} "
        f"--interval {{{{ params.interval }}}} "
        f"--window-start '{window_start}' "
        f"--window-end '{window_end}'",
    )

    extract_ohlcv >> transform_bronze_to_silver >> transform_silver_to_gold >> load_to_postgres >> data_quality_check
