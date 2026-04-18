"""Structured logging helpers for the ETL pipeline."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Render application logs in JSON for easier monitoring."""

    def format(self, record: logging.LogRecord) -> str:
        """Format one log record as JSON.

        Args:
            record: Standard Python log record.

        Returns:
            str: JSON-encoded log record.
        """
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "payload"):
            payload["payload"] = record.payload
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging for the application.

    Args:
        level: Root log level.

    Returns:
        None: This function mutates global logging configuration.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)
