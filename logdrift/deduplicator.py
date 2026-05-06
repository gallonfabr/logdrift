"""Deduplicator: suppress repeated anomaly events within a rolling time window."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from logdrift.detector import AnomalyEvent


@dataclass
class DeduplicatorConfig:
    window_seconds: float = 60.0
    key_fields: tuple = ("field", "value")

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if not self.key_fields:
            raise ValueError("key_fields must not be empty")


class Deduplicator:
    """Tracks recently seen anomaly events and filters duplicates."""

    def __init__(self, config: Optional[DeduplicatorConfig] = None) -> None:
        self._config = config or DeduplicatorConfig()
        # fingerprint -> first-seen timestamp
        self._seen: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_duplicate(self, event: AnomalyEvent) -> bool:
        """Return True if an equivalent event was seen within the window."""
        self._purge_expired()
        fp = self._fingerprint(event)
        if fp in self._seen:
            return True
        self._seen[fp] = time.monotonic()
        return False

    def reset(self) -> None:
        """Clear all tracked fingerprints."""
        self._seen.clear()

    def __len__(self) -> int:
        return len(self._seen)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _fingerprint(self, event: AnomalyEvent) -> str:
        parts = []
        for key in self._config.key_fields:
            parts.append(f"{key}={getattr(event, key, '')}")
        raw = "|".join(parts)
        return hashlib.sha1(raw.encode()).hexdigest()

    def _purge_expired(self) -> None:
        cutoff = time.monotonic() - self._config.window_seconds
        expired = [fp for fp, ts in self._seen.items() if ts < cutoff]
        for fp in expired:
            del self._seen[fp]
