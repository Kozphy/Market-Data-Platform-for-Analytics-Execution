"""Lightweight scheduler that simulates orchestration without Airflow."""

from __future__ import annotations

import logging
import time
from datetime import date

from market_data_platform.config import PipelineSettings
from market_data_platform.orchestration.jobs import PipelineRunner
from market_data_platform.utils.time_utils import utc_now

LOGGER = logging.getLogger(__name__)


class LightweightScheduler:
    """Run realtime and daily batch jobs in one long-lived process."""

    def __init__(self, settings: PipelineSettings, runner: PipelineRunner) -> None:
        """Initialize the scheduler.

        Args:
            settings: Pipeline settings.
            runner: Pipeline runner instance.
        """
        self._settings = settings
        self._runner = runner
        self._last_batch_date: date | None = None

    def _run_realtime_jobs(self) -> None:
        """Run one pass of realtime jobs across all configured symbols and intervals.

        Returns:
            None: This method triggers job executions.
        """
        for symbol in self._settings.symbols:
            for interval in self._settings.intervals:
                self._runner.run_realtime_update(symbol=symbol, interval=interval)

    def _run_daily_batch_jobs(self) -> None:
        """Run one pass of batch jobs across all configured symbols and intervals.

        Returns:
            None: This method triggers job executions.
        """
        for symbol in self._settings.symbols:
            for interval in self._settings.intervals:
                self._runner.run_incremental_batch(symbol=symbol, interval=interval)

    def _should_run_daily_batch(self) -> bool:
        """Determine whether the scheduler should run the daily batch.

        Returns:
            bool: ``True`` when the batch should run now.
        """
        now = utc_now()
        if now.hour != self._settings.daily_batch_hour_utc:
            return False
        if self._last_batch_date == now.date():
            return False
        return True

    def run_forever(self) -> None:
        """Run the scheduler loop until interrupted.

        Returns:
            None: This method blocks indefinitely.
        """
        LOGGER.info(
            "Scheduler started",
            extra={
                "payload": {
                    "symbols": self._settings.symbols,
                    "intervals": self._settings.intervals,
                    "poll_seconds": self._settings.realtime_poll_seconds,
                    "daily_batch_hour_utc": self._settings.daily_batch_hour_utc,
                }
            },
        )

        while True:
            if self._should_run_daily_batch():
                self._run_daily_batch_jobs()
                self._last_batch_date = utc_now().date()

            self._run_realtime_jobs()
            time.sleep(self._settings.realtime_poll_seconds)
