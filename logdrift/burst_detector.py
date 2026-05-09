"""Burst detector: flags when event volume spikes above a rolling baseline."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from time import monotonic
from typing import Deque, List, Optional


class BurstError(Exception):
    """Raised for invalid BurstDetector configuration."""


@dataclass
class BurstConfig:
    window_seconds: float = 60.0
    cooldown_seconds: float = 30.0
    multiplier: float = 3.0
    min_baseline_periods: int = 3

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise BurstError("window_seconds must be positive")
        if self.cooldown_seconds < 0:
            raise BurstError("cooldown_seconds must be non-negative")
        if self.multiplier <= 1.0:
            raise BurstError("multiplier must be greater than 1.0")
        if self.min_baseline_periods < 1:
            raise BurstError("min_baseline_periods must be at least 1")


@dataclass
class BurstAlert:
    field: str
    value: str
    current_rate: float
    baseline_rate: float
    multiplier_observed: float
    timestamp: float = field(default_factory=monotonic)

    def __repr__(self) -> str:
        return (
            f"BurstAlert(field={self.field!r}, value={self.value!r}, "
            f"current_rate={self.current_rate:.2f}, "
            f"baseline_rate={self.baseline_rate:.2f}, "
            f"multiplier={self.multiplier_observed:.2f}x)"
        )


class BurstDetector:
    """Detects sudden spikes in event rate for a given (field, value) pair."""

    def __init__(self, field: str, value: str, config: Optional[BurstConfig] = None) -> None:
        if not field:
            raise BurstError("field must not be empty")
        if not value:
            raise BurstError("value must not be empty")
        self.field = field
        self.value = value
        self.config = config or BurstConfig()
        self._buckets: Deque[tuple[float, int]] = deque()  # (timestamp, count)
        self._current_count: int = 0
        self._window_start: float = monotonic()
        self._last_alert_at: Optional[float] = None

    def observe(self, record: dict, now: Optional[float] = None) -> Optional[BurstAlert]:
        """Feed a record; return BurstAlert if a burst is detected, else None."""
        ts = now if now is not None else monotonic()
        if record.get(self.field) == self.value:
            self._current_count += 1

        # Roll over window bucket every window_seconds
        elapsed = ts - self._window_start
        if elapsed >= self.config.window_seconds:
            rate = self._current_count / max(elapsed, 1e-9)
            self._buckets.append((self._window_start, rate))
            self._current_count = 0
            self._window_start = ts
            self._evict_old(ts)

        if len(self._buckets) < self.config.min_baseline_periods:
            return None

        baseline = sum(r for _, r in self._buckets) / len(self._buckets)
        if baseline == 0:
            return None

        current_rate = self._current_count / max(ts - self._window_start, 1e-9)
        ratio = current_rate / baseline

        if ratio < self.config.multiplier:
            return None

        if self._last_alert_at is not None and (ts - self._last_alert_at) < self.config.cooldown_seconds:
            return None

        self._last_alert_at = ts
        return BurstAlert(
            field=self.field,
            value=self.value,
            current_rate=current_rate,
            baseline_rate=baseline,
            multiplier_observed=ratio,
            timestamp=ts,
        )

    def _evict_old(self, now: float) -> None:
        cutoff = now - self.config.window_seconds * (self.config.min_baseline_periods + 1)
        while self._buckets and self._buckets[0][0] < cutoff:
            self._buckets.popleft()
