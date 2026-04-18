"""Checkpoint storage for incremental processing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class JsonStateStore:
    """Manage checkpoint state in a local JSON file."""

    def __init__(self, path: Path) -> None:
        """Initialize the state store.

        Args:
            path: File path for the JSON state store.
        """
        self._path = path

    def _read_all(self) -> dict[str, Any]:
        """Read the entire JSON state document.

        Returns:
            dict[str, Any]: Parsed state payload.
        """
        if not self._path.exists():
            return {}
        return json.loads(self._path.read_text(encoding="utf-8"))

    def get_checkpoint(self, key: str) -> dict[str, Any] | None:
        """Fetch a checkpoint payload by key.

        Args:
            key: Logical checkpoint key.

        Returns:
            dict[str, Any] | None: Stored checkpoint payload when present.
        """
        return self._read_all().get(key)

    def write_checkpoint(self, key: str, value: dict[str, Any]) -> None:
        """Persist a checkpoint payload.

        Args:
            key: Logical checkpoint key.
            value: Checkpoint payload to store.

        Returns:
            None: This method updates the JSON state document.
        """
        payload = self._read_all()
        payload[key] = value
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
