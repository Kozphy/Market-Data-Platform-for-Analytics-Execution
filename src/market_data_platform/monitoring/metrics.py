"""Simple job monitoring helpers built on structured logs."""

from __future__ import annotations

import logging

from market_data_platform.contracts import LoadSummary
from market_data_platform.utils.time_utils import utc_now

LOGGER = logging.getLogger(__name__)


class JobMonitor:
    """Emit structured lifecycle logs for pipeline jobs."""

    def start(self, job_name: str, exchange: str, symbol: str, interval: str) -> LoadSummary:
        """Record the start of a job run.

        Args:
            job_name: Job name.
            exchange: Exchange identifier.
            symbol: Trading symbol.
            interval: Candle interval.

        Returns:
            LoadSummary: Mutable execution summary.
        """
        summary = LoadSummary(
            job_name=job_name,
            exchange=exchange,
            symbol=symbol,
            interval=interval,
            started_at=utc_now(),
        )
        LOGGER.info("Job started", extra={"payload": summary.to_log_payload()})
        return summary

    def success(self, summary: LoadSummary) -> None:
        """Record a successful job run.

        Args:
            summary: Execution summary to finalize.

        Returns:
            None: This method emits a structured log.
        """
        summary.status = "success"
        summary.finished_at = utc_now()
        LOGGER.info("Job finished", extra={"payload": summary.to_log_payload()})

    def failure(self, summary: LoadSummary, error: Exception) -> None:
        """Record a failed job run.

        Args:
            summary: Execution summary to finalize.
            error: Exception that caused the failure.

        Returns:
            None: This method emits a structured log.
        """
        summary.status = "failed"
        summary.finished_at = utc_now()
        summary.metadata["error"] = str(error)
        LOGGER.error("Job failed", extra={"payload": summary.to_log_payload()}, exc_info=True)
