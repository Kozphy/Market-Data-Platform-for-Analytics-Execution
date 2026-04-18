"""Storage helpers for streaming tick data and alerts."""

from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from market_data_platform.signals.models import SignalEvent
from market_data_platform.streaming.models import PriceTick


class AlertSink(ABC):
    """Abstract sink interface for emitted signal events."""

    @abstractmethod
    def emit(self, events: list[SignalEvent]) -> int:
        """Persist emitted signal events.

        Args:
            events: Triggered signal events.

        Returns:
            int: Number of events persisted.
        """


class JsonLineAlertSink(AlertSink):
    """Append signal events to a JSONL file."""

    def __init__(self, path: Path) -> None:
        """Initialize the JSONL alert sink.

        Args:
            path: Destination file path.
        """
        self._path = path

    def emit(self, events: list[SignalEvent]) -> int:
        """Persist signal events to a JSONL file.

        Args:
            events: Triggered signal events.

        Returns:
            int: Number of events persisted.
        """
        if not events:
            return 0

        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event.to_payload(), default=str) + "\n")

        return len(events)


class TickParquetWriter:
    """Persist polled price ticks into partitioned Parquet files."""

    def __init__(self, root_path: Path) -> None:
        """Initialize the tick writer.

        Args:
            root_path: Root folder for tick datasets.
        """
        self._root_path = root_path

    def write_ticks(self, ticks: list[PriceTick]) -> int:
        """Persist price ticks into Parquet partitions.

        Args:
            ticks: Price ticks to persist.

        Returns:
            int: Number of ticks written.
        """
        if not ticks:
            return 0

        frame = pd.DataFrame([tick.to_payload() for tick in ticks])
        frame["partition_date"] = pd.to_datetime(frame["event_time"], utc=True).dt.strftime("%Y-%m-%d")
        batch_identifier = uuid.uuid4().hex

        for keys, subset in frame.groupby(["exchange", "symbol", "partition_date"], dropna=False):
            exchange, symbol, partition_date = keys
            destination = (
                self._root_path
                / f"exchange={exchange}"
                / f"symbol={symbol}"
                / f"date={partition_date}"
            )
            destination.mkdir(parents=True, exist_ok=True)
            subset.drop(columns=["partition_date"]).to_parquet(
                destination / f"{batch_identifier}.parquet",
                index=False,
            )

        return len(ticks)
