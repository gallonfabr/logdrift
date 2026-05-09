"""Exponential decay scorer for anomaly events.

Older anomalies are down-weighted so that recent activity has more
influence on the running risk score for a given field/value pair.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple


class DecayError(Exception):
    """Raised for invalid decay configuration."""


@dataclass
class DecayConfig:
    half_life: float = 60.0   # seconds
    min_score: float = 0.0

    def __post_init__(self) -> None:
        if self.half_life <= 0:
            raise DecayError("half_life must be positive")
        if not (0.0 <= self.min_score < 1.0):
            raise DecayError("min_score must be in [0, 1)")


@dataclass
class DecayedScore:
    field: str
    value: str
    score: float
    last_updated: float

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"DecayedScore(field={self.field!r}, value={self.value!r}, "
            f"score={self.score:.4f})"
        )


class DecayScorer:
    """Maintains per-(field, value) exponentially decayed scores."""

    def __init__(self, config: DecayConfig | None = None) -> None:
        self._cfg = config or DecayConfig()
        self._lambda: float = math.log(2) / self._cfg.half_life
        # key -> (score, timestamp)
        self._state: Dict[Tuple[str, str], Tuple[float, float]] = {}

    # ------------------------------------------------------------------
    def _decay(self, score: float, elapsed: float) -> float:
        return score * math.exp(-self._lambda * elapsed)

    def update(self, field: str, value: str, new_score: float) -> DecayedScore:
        """Decay the existing score then add *new_score*."""
        now = time.monotonic()
        key = (field, value)
        if key in self._state:
            prev_score, prev_ts = self._state[key]
            decayed = self._decay(prev_score, now - prev_ts)
        else:
            decayed = 0.0
        combined = max(self._cfg.min_score, decayed + new_score)
        self._state[key] = (combined, now)
        return DecayedScore(field=field, value=value, score=combined, last_updated=now)

    def get(self, field: str, value: str) -> DecayedScore | None:
        """Return current decayed score without modifying state."""
        now = time.monotonic()
        key = (field, value)
        if key not in self._state:
            return None
        prev_score, prev_ts = self._state[key]
        score = max(self._cfg.min_score, self._decay(prev_score, now - prev_ts))
        return DecayedScore(field=field, value=value, score=score, last_updated=prev_ts)

    def clear(self) -> None:
        self._state.clear()
