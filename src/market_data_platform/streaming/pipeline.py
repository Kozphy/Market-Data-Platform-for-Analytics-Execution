"""Polling-based streaming pipeline for ticker prices and threshold alerts."""

from __future__ import annotations

import logging
import time

from market_data_platform.config import StreamingSettings
from market_data_platform.ingestion.base import MarketDataSource
from market_data_platform.monitoring.metrics import JobMonitor
from market_data_platform.signals.models import SignalEvent, ThresholdSignalRule
from market_data_platform.signals.thresholds import build_signal_event, is_threshold_triggered
from market_data_platform.storage.state import JsonStateStore
from market_data_platform.streaming.models import PriceTick
from market_data_platform.streaming.storage import AlertSink, TickParquetWriter
from market_data_platform.utils.time_utils import utc_now

LOGGER = logging.getLogger(__name__)


class PollingSignalPipeline:
    """Poll live prices, persist ticks, and emit threshold alerts."""

    def __init__(
        self,
        settings: StreamingSettings,
        source: MarketDataSource,
        state_store: JsonStateStore,
        tick_writer: TickParquetWriter,
        alert_sink: AlertSink,
        monitor: JobMonitor,
        rules: list[ThresholdSignalRule],
    ) -> None:
        """Initialize the streaming pipeline.

        Args:
            settings: Streaming pipeline settings.
            source: Market data source.
            state_store: Checkpoint and rule-state store.
            tick_writer: Tick Parquet writer.
            alert_sink: Sink for triggered alerts.
            monitor: Job monitor for structured logs.
            rules: Threshold rules to evaluate.
        """
        self._settings = settings
        self._source = source
        self._state_store = state_store
        self._tick_writer = tick_writer
        self._alert_sink = alert_sink
        self._monitor = monitor
        self._rules = rules

    def bootstrap(self) -> None:
        """Create local directories required by the streaming pipeline.

        Returns:
            None: This method prepares local streaming artifacts.
        """
        self._settings.ensure_directories()

    def _rule_state_key(self, rule: ThresholdSignalRule) -> str:
        """Build the state key for a threshold rule.

        Args:
            rule: Threshold rule.

        Returns:
            str: Unique state key for the rule.
        """
        interval = rule.interval or "spot"
        return f"streaming-rule:{rule.signal_name}:{rule.symbol}:{interval}"

    def _poll_tick(self, symbol: str) -> PriceTick:
        """Poll the latest spot price for one symbol.

        Args:
            symbol: Trading symbol.

        Returns:
            PriceTick: Current price snapshot.
        """
        payload = self._source.fetch_latest_price(symbol)
        event_time = utc_now()
        return PriceTick(
            exchange="binance",
            symbol=symbol,
            event_time=event_time,
            price=float(payload["price"]),
            source="binance_ticker_price_poll",
            ingested_at=event_time,
        )

    def _evaluate_rules(self, tick: PriceTick) -> list[SignalEvent]:
        """Evaluate threshold rules for a polled price tick.

        Args:
            tick: Current price snapshot.

        Returns:
            list[SignalEvent]: Triggered signal events.
        """
        events: list[SignalEvent] = []
        for rule in self._rules:
            if not rule.applies_to(symbol=tick.symbol, interval="spot"):
                continue

            state_key = self._rule_state_key(rule)
            previous_state = self._state_store.get_checkpoint(state_key) or {}
            previous_value = previous_state.get("previous_value")
            previous_condition = previous_state.get("current_condition")
            triggered, current_condition = is_threshold_triggered(
                direction=rule.direction,
                previous_value=float(previous_value) if previous_value is not None else None,
                current_value=tick.price,
                threshold=rule.threshold,
                previous_condition=bool(previous_condition) if previous_condition is not None else None,
            )

            if triggered:
                events.append(
                    build_signal_event(
                        rule=rule,
                        exchange=tick.exchange,
                        symbol=tick.symbol,
                        event_time=tick.event_time,
                        observed_value=tick.price,
                        source=tick.source,
                        interval="spot",
                        metadata={"event_type": "threshold_alert"},
                    )
                )

            self._state_store.write_checkpoint(
                state_key,
                {
                    "previous_value": tick.price,
                    "current_condition": current_condition,
                    "updated_at": tick.ingested_at.isoformat(),
                },
            )

        return events

    def run_once(self, symbols: list[str]) -> dict[str, int]:
        """Run one poll cycle across a list of symbols.

        Args:
            symbols: Trading symbols to poll.

        Returns:
            dict[str, int]: Summary counts for ticks and alerts emitted.
        """
        all_ticks: list[PriceTick] = []
        all_events: list[SignalEvent] = []

        for symbol in symbols:
            summary = self._monitor.start(
                job_name="streaming_poll",
                exchange="binance",
                symbol=symbol,
                interval="spot",
            )
            try:
                tick = self._poll_tick(symbol)
                events = self._evaluate_rules(tick)
                summary.rows_extracted = 1
                summary.rows_loaded = 1
                summary.metadata["price"] = tick.price
                summary.metadata["alert_count"] = len(events)
                self._monitor.success(summary)
                all_ticks.append(tick)
                all_events.extend(events)
            except Exception as error:
                self._monitor.failure(summary, error)
                raise

        tick_count = self._tick_writer.write_ticks(all_ticks)
        alert_count = self._alert_sink.emit(all_events)
        LOGGER.info(
            "Streaming poll completed",
            extra={"payload": {"tick_count": tick_count, "alert_count": alert_count}},
        )
        return {"tick_count": tick_count, "alert_count": alert_count}

    def run_forever(self, symbols: list[str]) -> None:
        """Run the streaming pipeline continuously until interrupted.

        Args:
            symbols: Trading symbols to poll.

        Returns:
            None: This method blocks indefinitely.
        """
        LOGGER.info(
            "Streaming pipeline started",
            extra={
                "payload": {
                    "symbols": symbols,
                    "poll_seconds": self._settings.poll_seconds,
                    "rule_count": len(self._rules),
                }
            },
        )
        while True:
            self.run_once(symbols)
            time.sleep(self._settings.poll_seconds)
