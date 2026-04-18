"""Threshold rule parsing and evaluation helpers."""

from __future__ import annotations

from datetime import datetime

from market_data_platform.signals.models import SignalEvent, ThresholdSignalRule

VALID_THRESHOLD_DIRECTIONS = {"above", "below", "cross_above", "cross_below"}


def parse_threshold_rules(raw_rules: list[str]) -> list[ThresholdSignalRule]:
    """Parse CLI or environment rule strings into structured rules.

    Args:
        raw_rules: Rule strings using ``symbol|direction|threshold|name`` or
            ``symbol|interval|direction|threshold|name`` format.

    Returns:
        list[ThresholdSignalRule]: Parsed signal rules.

    Raises:
        ValueError: If any rule uses an invalid format.
    """
    parsed_rules: list[ThresholdSignalRule] = []
    for raw_rule in raw_rules:
        parts = [part.strip() for part in raw_rule.split("|")]
        if len(parts) == 4:
            symbol, direction, threshold, signal_name = parts
            interval = None
        elif len(parts) == 5:
            symbol, interval, direction, threshold, signal_name = parts
        else:
            raise ValueError(
                "Threshold rules must use symbol|direction|threshold|name "
                "or symbol|interval|direction|threshold|name format"
            )

        if direction not in VALID_THRESHOLD_DIRECTIONS:
            raise ValueError(f"Unsupported threshold direction: {direction}")

        parsed_rules.append(
            ThresholdSignalRule(
                symbol=symbol,
                interval=interval,
                direction=direction,
                threshold=float(threshold),
                signal_name=signal_name,
            )
        )

    return parsed_rules


def is_threshold_triggered(
    direction: str,
    previous_value: float | None,
    current_value: float,
    threshold: float,
    previous_condition: bool | None = None,
) -> tuple[bool, bool]:
    """Evaluate whether a threshold rule triggered on the current observation.

    Args:
        direction: Rule direction such as ``cross_above`` or ``below``.
        previous_value: Previous observed value when available.
        current_value: Current observed value.
        threshold: Threshold level.
        previous_condition: Previously persisted condition state for ``above`` and
            ``below`` alerts.

    Returns:
        tuple[bool, bool]: Triggered flag and current condition state.
    """
    if direction == "above":
        current_condition = current_value >= threshold
        prior_condition = previous_condition if previous_condition is not None else False
        return current_condition and not prior_condition, current_condition

    if direction == "below":
        current_condition = current_value <= threshold
        prior_condition = previous_condition if previous_condition is not None else False
        return current_condition and not prior_condition, current_condition

    if previous_value is None:
        current_condition = current_value >= threshold if direction == "cross_above" else current_value <= threshold
        return False, current_condition

    if direction == "cross_above":
        current_condition = current_value >= threshold
        return previous_value < threshold <= current_value, current_condition

    if direction == "cross_below":
        current_condition = current_value <= threshold
        return previous_value > threshold >= current_value, current_condition

    raise ValueError(f"Unsupported threshold direction: {direction}")


def build_signal_event(
    rule: ThresholdSignalRule,
    exchange: str,
    symbol: str,
    event_time: datetime,
    observed_value: float,
    source: str,
    interval: str | None = None,
    metadata: dict[str, object] | None = None,
) -> SignalEvent:
    """Construct a structured signal event from a triggered rule.

    Args:
        rule: Triggered threshold rule.
        exchange: Exchange identifier.
        symbol: Trading symbol.
        event_time: Event timestamp.
        observed_value: Current observed market value.
        source: Source system that emitted the event.
        interval: Optional interval or stream label.
        metadata: Optional metadata payload.

    Returns:
        SignalEvent: Structured signal event.
    """
    return SignalEvent(
        signal_name=rule.signal_name,
        exchange=exchange,
        symbol=symbol,
        interval=interval or rule.interval,
        event_time=event_time,
        observed_value=observed_value,
        threshold=rule.threshold,
        direction=rule.direction,
        source=source,
        metadata=metadata or {},
    )
