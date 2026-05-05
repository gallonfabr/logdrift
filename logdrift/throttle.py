"""Alert throttling to suppress repeated alerts for the same field/value."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class ThrottleConfig:
    """Configuration for alert throttling."""

    window_seconds: float = 60.0
    max_alerts: int = 3

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.max_alerts < 1:
            raise ValueError("max_alerts must be at least 1")


# Key is (field_name, value)
_AlertKey = Tuple[str, str]


class Throttle:
    """Suppress duplicate alerts within a rolling time window."""

    def __init__(self, config: Optional[ThrottleConfig] = None) -> None:
        self._config = config or ThrottleConfig()
        # maps key -> list of timestamps (floats)
        self._history: Dict[_AlertKey, list] = {}

    def _purge_old(self, key: _AlertKey, now: float) -> None:
        cutoff = now - self._config.window_seconds
        self._history[key] = [
            ts for ts in self._history.get(key, []) if ts > cutoff
        ]

    def allow(self, field_name: str, value: str) -> bool:
        """Return True if the alert should be emitted, False if throttled."""
        key: _AlertKey = (field_name, value)
        now = time.monotonic()
        self._purge_old(key, now)
        count = len(self._history.get(key, []))
        if count < self._config.max_alerts:
            self._history.setdefault(key, []).append(now)
            return True
        return False

    def reset(self, field_name: str, value: str) -> None:
        """Clear throttle history for a specific key."""
        self._history.pop((field_name, value), None)

    def reset_all(self) -> None:
        """Clear all throttle history."""
        self._history.clear()
