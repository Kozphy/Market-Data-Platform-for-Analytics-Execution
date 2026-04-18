"""Dataclasses for signal rules and emitted events."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class ThresholdSignalRule:
    """Configuration for a price-threshold signal rule."""

    symbol: str
    direction: str
    threshold: float
    signal_name: str
    interval: str | None = None

    def applies_to(self, symbol: str, interval: str | None = None) -> bool:
        """Determine whether a rule applies to a symbol and interval.

        Args:
            symbol: Trading symbol under evaluation.
            interval: Optional interval or stream label.

        Returns:
            bool: ``True`` when the rule applies to the requested market slice.
        """
        if self.symbol != symbol:
            return False
        if self.interval is None:
            return True
        return self.interval == interval

    def to_payload(self) -> dict[str, Any]:
        """Serialize the rule for logging or persistence.

        Returns:
            dict[str, Any]: JSON-friendly representation of the rule.
        """
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "threshold": self.threshold,
            "signal_name": self.signal_name,
            "interval": self.interval,
        }


@dataclass(frozen=True, slots=True)
class SignalEvent:
    """Signal event emitted by the streaming or backtesting layers."""

    signal_name: str
    exchange: str
    symbol: str
    event_time: datetime
    observed_value: float
    threshold: float
    direction: str
    source: str
    interval: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Serialize the signal event for logging or persistence.

        Returns:
            dict[str, Any]: JSON-friendly representation of the event.
        """
        return {
            "signal_name": self.signal_name,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "interval": self.interval,
            "event_time": self.event_time.isoformat(),
            "observed_value": self.observed_value,
            "threshold": self.threshold,
            "direction": self.direction,
            "source": self.source,
            "metadata": self.metadata,
        }
