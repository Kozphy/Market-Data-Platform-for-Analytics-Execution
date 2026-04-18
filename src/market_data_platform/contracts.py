"""Dataclasses used across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class LoadSummary:
    """Summarize one pipeline execution for logging and monitoring."""

    job_name: str
    exchange: str
    symbol: str
    interval: str
    started_at: datetime
    rows_extracted: int = 0
    rows_cleaned: int = 0
    rows_loaded: int = 0
    finished_at: datetime | None = None
    status: str = "running"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_log_payload(self) -> dict[str, Any]:
        """Convert the summary into a structured log payload.

        Returns:
            dict[str, Any]: Structured fields that can be serialized as JSON.
        """
        return {
            "job_name": self.job_name,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "interval": self.interval,
            "started_at": self.started_at.isoformat(),
            "rows_extracted": self.rows_extracted,
            "rows_cleaned": self.rows_cleaned,
            "rows_loaded": self.rows_loaded,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status": self.status,
            "metadata": self.metadata,
        }
