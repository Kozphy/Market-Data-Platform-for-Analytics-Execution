"""Read partitioned Parquet lakehouse layers for a symbol/interval/time window."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from market_data_platform.utils.time_utils import ensure_utc

LOGGER = logging.getLogger(__name__)


def _layer_symbol_path(parquet_root: Path, layer: str, exchange: str, symbol: str, interval: str) -> Path:
    return parquet_root / layer / f"exchange={exchange}" / f"symbol={symbol}" / f"interval={interval}"


def read_parquet_window(
    parquet_root: Path,
    layer: str,
    exchange: str,
    symbol: str,
    interval: str,
    window_start: datetime,
    window_end: datetime,
) -> pd.DataFrame:
    """Load all Parquet rows under a hive-style path, filtered to ``[window_start, window_end]``.

    Bronze stores ``open_time`` as integer milliseconds (raw Binance). Silver/Gold store
    timezone-aware datetimes after normalization.

    Args:
        parquet_root: Lakehouse root (parent of bronze/silver/gold).
        layer: ``bronze``, ``silver``, or ``gold``.
        exchange: Partition exchange value.
        symbol: Partition symbol value.
        interval: Partition interval value.
        window_start: Inclusive window start (UTC).
        window_end: Inclusive window end (UTC).

    Returns:
        Concatenated frame for the window (possibly empty).
    """
    base = _layer_symbol_path(parquet_root, layer, exchange, symbol, interval)
    if not base.exists():
        LOGGER.info("No parquet data under path", extra={"payload": {"path": str(base)}})
        return pd.DataFrame()

    files = sorted(base.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in files:
        frames.append(pd.read_parquet(path))

    frame = pd.concat(frames, ignore_index=True)
    if frame.empty:
        return frame

    start = ensure_utc(window_start)
    end = ensure_utc(window_end)

    if layer == "bronze":
        ot = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
    else:
        ot = pd.to_datetime(frame["open_time"], utc=True)

    mask = (ot >= start) & (ot <= end)
    out = frame.loc[mask].copy()
    LOGGER.info(
        "Parquet window read",
        extra={
            "payload": {
                "layer": layer,
                "files": len(files),
                "rows_total": len(frame),
                "rows_in_window": len(out),
            }
        },
    )
    return out.reset_index(drop=True)
