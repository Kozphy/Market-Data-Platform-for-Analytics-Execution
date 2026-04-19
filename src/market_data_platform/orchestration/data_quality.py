"""Lightweight data quality checks for the serving layer over a time window."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime

import psycopg

from market_data_platform.config import PipelineSettings
from market_data_platform.utils.time_utils import binance_interval_to_timedelta, ensure_utc

LOGGER = logging.getLogger(__name__)


def _allow_empty_window() -> bool:
    raw = os.getenv("MARKET_DATA_DQ_ALLOW_EMPTY_WINDOW")
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True, slots=True)
class DataQualityReport:
    """Summary output for a DQ run."""

    row_count: int
    distinct_pk: int
    min_open_time: datetime | None
    max_open_time: datetime | None
    gap_violations: int


def run_postgres_window_checks(
    symbol: str,
    interval: str,
    window_start: datetime,
    window_end: datetime,
) -> DataQualityReport:
    """Validate Postgres rows for ``binance`` / ``symbol`` / ``interval`` in ``[start, end]``.

    Checks (when ``row_count > 0``):
    - No duplicate primary key
    - No missing expected bar timestamps (based on Binance bar cadence)

    By default, an empty window fails (``row_count == 0``) so the DAG surfaces upstream
    gaps. Set ``MARKET_DATA_DQ_ALLOW_EMPTY_WINDOW=true`` to allow empty windows (for
    sparse symbols or partial connectivity during demos).
    """
    settings = PipelineSettings.from_env()
    if not settings.postgres_dsn:
        raise RuntimeError("MARKET_DATA_POSTGRES_DSN is required for data quality checks")

    start = ensure_utc(window_start)
    end = ensure_utc(window_end)

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::bigint,
                       COUNT(DISTINCT (exchange, symbol, interval, open_time))::bigint,
                       MIN(open_time),
                       MAX(open_time)
                FROM market_ohlcv_features
                WHERE exchange = %s AND symbol = %s AND interval = %s
                  AND open_time >= %s AND open_time <= %s
                """,
                ("binance", symbol, interval, start, end),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Unexpected empty aggregate result from Postgres")
            row_count, distinct_pk, min_ot, max_ot = int(row[0]), int(row[1]), row[2], row[3]

    LOGGER.info(
        "Data quality summary",
        extra={
            "payload": {
                "symbol": symbol,
                "interval": interval,
                "row_count": row_count,
                "distinct_pk": distinct_pk,
                "min_open_time": min_ot.isoformat() if min_ot else None,
                "max_open_time": max_ot.isoformat() if max_ot else None,
            }
        },
    )

    if row_count == 0:
        if _allow_empty_window():
            LOGGER.warning(
                "DQ allowed empty window (MARKET_DATA_DQ_ALLOW_EMPTY_WINDOW=true)",
                extra={"payload": {"symbol": symbol, "interval": interval}},
            )
            return DataQualityReport(
                row_count=0,
                distinct_pk=0,
                min_open_time=None,
                max_open_time=None,
                gap_violations=0,
            )
        raise RuntimeError(
            "Data quality failed: row_count is 0 for the window "
            f"(symbol={symbol}, interval={interval}). "
            "Set MARKET_DATA_DQ_ALLOW_EMPTY_WINDOW=true to permit empty windows."
        )

    if row_count != distinct_pk:
        raise RuntimeError(
            f"Duplicate primary keys detected: rows={row_count} distinct_pk={distinct_pk}"
        )

    if min_ot is None or max_ot is None:
        raise RuntimeError("Unexpected null min/max open_time with non-zero row count")

    step = binance_interval_to_timedelta(interval)
    # Integer bar counts from epoch milliseconds avoid float edge cases.
    min_ms = int(ensure_utc(min_ot).timestamp() * 1000)
    max_ms = int(ensure_utc(max_ot).timestamp() * 1000)
    step_ms = int(step.total_seconds() * 1000)
    if step_ms <= 0:
        raise RuntimeError(f"Invalid bar step for interval={interval}")
    expected = (max_ms - min_ms) // step_ms + 1
    gap_violations = max(expected - row_count, 0)
    if gap_violations > 0:
        raise RuntimeError(
            f"Missing timestamps suspected: rows={row_count} expected>={expected} "
            f"(min={min_ot}, max={max_ot}, step={step})"
        )

    LOGGER.info("Data quality checks passed", extra={"payload": {"gap_violations": 0}})
    return DataQualityReport(
        row_count=row_count,
        distinct_pk=distinct_pk,
        min_open_time=ensure_utc(min_ot),
        max_open_time=ensure_utc(max_ot),
        gap_violations=0,
    )
