"""Unit tests for the polling-based streaming signal pipeline."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from market_data_platform.config import StreamingSettings
from market_data_platform.ingestion.base import MarketDataSource
from market_data_platform.monitoring.metrics import JobMonitor
from market_data_platform.signals.models import SignalEvent, ThresholdSignalRule
from market_data_platform.storage.state import JsonStateStore
from market_data_platform.streaming.pipeline import PollingSignalPipeline


class StubMarketDataSource(MarketDataSource):
    """Stub price source for streaming pipeline tests."""

    def __init__(self, symbol_prices: dict[str, list[float]]) -> None:
        """Initialize the stub data source.

        Args:
            symbol_prices: Price sequences keyed by trading symbol.
        """
        self._symbol_prices = {symbol: list(values) for symbol, values in symbol_prices.items()}
        self._positions = {symbol: 0 for symbol in symbol_prices}

    def fetch_historical_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_time,
        end_time,
    ) -> pd.DataFrame:
        """Return an empty historical frame for interface compatibility.

        Args:
            symbol: Trading symbol.
            interval: Candle interval.
            start_time: Extraction start time.
            end_time: Extraction end time.

        Returns:
            pd.DataFrame: Empty DataFrame.
        """
        return pd.DataFrame()

    def fetch_recent_ohlcv(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        """Return an empty recent frame for interface compatibility.

        Args:
            symbol: Trading symbol.
            interval: Candle interval.
            limit: Number of requested rows.

        Returns:
            pd.DataFrame: Empty DataFrame.
        """
        return pd.DataFrame()

    def fetch_latest_price(self, symbol: str) -> dict[str, str]:
        """Return the next stubbed price for a symbol.

        Args:
            symbol: Trading symbol.

        Returns:
            dict[str, str]: Price payload compatible with the Binance client.
        """
        index = self._positions[symbol]
        price = self._symbol_prices[symbol][index]
        if index < len(self._symbol_prices[symbol]) - 1:
            self._positions[symbol] += 1
        return {"symbol": symbol, "price": str(price)}


class InMemoryTickWriter:
    """In-memory tick writer for streaming tests."""

    def __init__(self) -> None:
        """Initialize the writer.

        Returns:
            None: This constructor prepares internal state.
        """
        self.ticks = []

    def write_ticks(self, ticks) -> int:
        """Record ticks in memory.

        Args:
            ticks: Ticks to persist.

        Returns:
            int: Number of ticks recorded.
        """
        self.ticks.extend(ticks)
        return len(ticks)


class InMemoryAlertSink:
    """In-memory alert sink for streaming tests."""

    def __init__(self) -> None:
        """Initialize the sink.

        Returns:
            None: This constructor prepares internal state.
        """
        self.events: list[SignalEvent] = []

    def emit(self, events: list[SignalEvent]) -> int:
        """Record events in memory.

        Args:
            events: Events to persist.

        Returns:
            int: Number of events recorded.
        """
        self.events.extend(events)
        return len(events)


def test_polling_signal_pipeline_emits_cross_above_alert_once(tmp_path: Path) -> None:
    """Ensure threshold alerts trigger once when a price crosses upward.

    Args:
        tmp_path: Temporary test directory.

    Returns:
        None: Assertions validate streaming alert behavior.
    """
    source = StubMarketDataSource({"BTCUSDT": [99.0, 101.0, 105.0]})
    tick_writer = InMemoryTickWriter()
    alert_sink = InMemoryAlertSink()
    rules = [
        ThresholdSignalRule(
            symbol="BTCUSDT",
            direction="cross_above",
            threshold=100.0,
            signal_name="btc_breakout",
        )
    ]
    pipeline = PollingSignalPipeline(
        settings=StreamingSettings(
            poll_seconds=1,
            alerts_path=tmp_path / "alerts.jsonl",
            stream_ticks_path=tmp_path / "ticks",
            alert_rules=[],
        ),
        source=source,
        state_store=JsonStateStore(tmp_path / "state.json"),
        tick_writer=tick_writer,
        alert_sink=alert_sink,
        monitor=JobMonitor(),
        rules=rules,
    )

    pipeline.bootstrap()
    first_result = pipeline.run_once(["BTCUSDT"])
    second_result = pipeline.run_once(["BTCUSDT"])
    third_result = pipeline.run_once(["BTCUSDT"])

    assert first_result["alert_count"] == 0
    assert second_result["alert_count"] == 1
    assert third_result["alert_count"] == 0
    assert len(tick_writer.ticks) == 3
    assert len(alert_sink.events) == 1
    assert alert_sink.events[0].signal_name == "btc_breakout"
