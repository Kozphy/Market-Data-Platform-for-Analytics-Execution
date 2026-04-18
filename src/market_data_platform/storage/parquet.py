"""Parquet writer for raw, cleaned, and feature-rich datasets."""

from __future__ import annotations

import uuid
from pathlib import Path

import pandas as pd


class ParquetLakehouseWriter:
    """Write pipeline outputs into partitioned Parquet datasets."""

    def __init__(self, base_path: Path) -> None:
        """Initialize the Parquet writer.

        Args:
            base_path: Root folder for lakehouse datasets.
        """
        self._base_path = base_path

    def write_layer(self, frame: pd.DataFrame, layer: str) -> int:
        """Persist a DataFrame into a lakehouse layer.

        Args:
            frame: DataFrame to persist.
            layer: Logical layer name such as ``bronze`` or ``gold``.

        Returns:
            int: Number of rows written.
        """
        if frame.empty:
            return 0

        working = frame.copy()
        working["partition_date"] = pd.to_datetime(working["open_time"], utc=True).dt.strftime("%Y-%m-%d")
        batch_identifier = uuid.uuid4().hex
        layer_root = self._base_path / layer

        for keys, subset in working.groupby(["exchange", "symbol", "interval", "partition_date"], dropna=False):
            exchange, symbol, interval, partition_date = keys
            destination = (
                layer_root
                / f"exchange={exchange}"
                / f"symbol={symbol}"
                / f"interval={interval}"
                / f"date={partition_date}"
            )
            destination.mkdir(parents=True, exist_ok=True)
            subset.drop(columns=["partition_date"]).to_parquet(
                destination / f"{batch_identifier}.parquet",
                index=False,
            )

        return len(frame)
