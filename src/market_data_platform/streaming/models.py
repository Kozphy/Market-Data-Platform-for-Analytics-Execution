"""Dataclasses for streaming price snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PriceTick:
    """One polled market price snapshot."""

    exchange: str
    symbol: str
    event_time: datetime
    price: float
    source: str
    ingested_at: datetime

    def to_payload(self) -> dict[str, object]:
        """Serialize the tick for logging or persistence.

        Returns:
            dict[str, object]: JSON-friendly tick payload.
        """
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "event_time": self.event_time.isoformat(),
            "price": self.price,
            "source": self.source,
            "ingested_at": self.ingested_at.isoformat(),
        }
