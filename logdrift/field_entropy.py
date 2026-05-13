"""Field entropy tracker: flags fields whose value distribution becomes unexpectedly uniform or concentrated."""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class EntropyError(ValueError):
    """Raised when FieldEntropyConfig receives invalid parameters."""


@dataclass
class FieldEntropyConfig:
    field_name: str
    window_seconds: float = 60.0
    min_samples: int = 10
    low_entropy_threshold: float = 0.2   # normalised 0-1; below this → alert
    high_entropy_threshold: float = 0.95  # above this → alert

    def __post_init__(self) -> None:
        if not self.field_name:
            raise EntropyError("field_name must not be empty")
        if self.window_seconds <= 0:
            raise EntropyError("window_seconds must be positive")
        if self.min_samples < 2:
            raise EntropyError("min_samples must be at least 2")
        if not (0.0 <= self.low_entropy_threshold < self.high_entropy_threshold <= 1.0):
            raise EntropyError(
                "thresholds must satisfy 0 <= low_entropy_threshold < high_entropy_threshold <= 1"
            )


@dataclass
class EntropyAnomaly:
    field_name: str
    normalised_entropy: float
    kind: str          # 'low' or 'high'
    sample_count: int
    top_values: List[str]

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"EntropyAnomaly(field={self.field_name!r}, entropy={self.normalised_entropy:.3f}, "
            f"kind={self.kind!r}, n={self.sample_count})"
        )


class FieldEntropy:
    """Sliding-window Shannon entropy tracker for a single log field."""

    def __init__(self, config: FieldEntropyConfig) -> None:
        self._cfg = config
        # list of (timestamp, value)
        self._window: List[tuple] = []
        self._counts: Dict[str, int] = defaultdict(int)

    def observe(self, value: str, ts: float) -> Optional[EntropyAnomaly]:
        """Record *value* at time *ts* and return an anomaly if thresholds are breached."""
        self._window.append((ts, value))
        self._counts[value] += 1
        self._purge(ts)

        n = len(self._window)
        if n < self._cfg.min_samples:
            return None

        entropy = self._normalised_entropy()
        if entropy < self._cfg.low_entropy_threshold:
            kind = "low"
        elif entropy > self._cfg.high_entropy_threshold:
            kind = "high"
        else:
            return None

        top = sorted(self._counts, key=lambda k: -self._counts[k])[:5]
        return EntropyAnomaly(
            field_name=self._cfg.field_name,
            normalised_entropy=round(entropy, 6),
            kind=kind,
            sample_count=n,
            top_values=top,
        )

    # ------------------------------------------------------------------
    def _purge(self, now: float) -> None:
        cutoff = now - self._cfg.window_seconds
        while self._window and self._window[0][0] < cutoff:
            _, old_val = self._window.pop(0)
            self._counts[old_val] -= 1
            if self._counts[old_val] == 0:
                del self._counts[old_val]

    def _normalised_entropy(self) -> float:
        n = len(self._window)
        if n <= 1:
            return 0.0
        h = -sum(
            (c / n) * math.log2(c / n)
            for c in self._counts.values()
            if c > 0
        )
        max_h = math.log2(n)
        return h / max_h if max_h > 0 else 0.0
