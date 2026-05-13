"""Cadence detector – flags fields whose inter-arrival intervals deviate
from a learned baseline (e.g. a heartbeat that suddenly speeds up or stops)."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional


class CadenceError(ValueError):
    """Raised for invalid cadence configuration."""


@dataclass
class CadenceConfig:
    field: str
    window: int = 60          # seconds of history to keep
    min_periods: int = 4      # minimum intervals before alerting
    z_threshold: float = 3.0  # how many std-devs counts as anomalous

    def __post_init__(self) -> None:
        if not self.field:
            raise CadenceError("field must not be empty")
        if self.window <= 0:
            raise CadenceError("window must be positive")
        if self.min_periods < 2:
            raise CadenceError("min_periods must be >= 2")
        if self.z_threshold <= 0:
            raise CadenceError("z_threshold must be positive")


@dataclass
class CadenceAnomaly:
    field: str
    value: str
    interval: float
    mean: float
    stddev: float
    z_score: float
    ts: float = field(default_factory=time.time)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CadenceAnomaly(field={self.field!r}, value={self.value!r}, "
            f"z={self.z_score:.2f})"
        )


class CadenceDetector:
    """Tracks per-value inter-arrival times and emits anomalies."""

    def __init__(self, config: CadenceConfig) -> None:
        self.config = config
        # value -> deque of (timestamp,)
        self._last_seen: dict[str, float] = {}
        self._intervals: dict[str, Deque[float]] = {}

    def observe(self, record: dict, ts: Optional[float] = None) -> Optional[CadenceAnomaly]:
        now = ts if ts is not None else time.time()
        raw = record.get(self.config.field)
        if raw is None:
            return None
        value = str(raw)
        cutoff = now - self.config.window

        if value not in self._intervals:
            self._intervals[value] = deque()

        intervals = self._intervals[value]

        if value in self._last_seen:
            gap = now - self._last_seen[value]
            intervals.append(gap)

        # evict stale intervals (approximate: drop oldest until sum fits window)
        while len(intervals) > 1 and sum(list(intervals)[:-1]) > self.config.window:
            intervals.popleft()

        self._last_seen[value] = now

        if len(intervals) < self.config.min_periods:
            return None

        data = list(intervals)
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        stddev = variance ** 0.5

        if stddev == 0:
            return None

        latest = data[-1]
        z = abs(latest - mean) / stddev
        if z >= self.config.z_threshold:
            return CadenceAnomaly(
                field=self.config.field,
                value=value,
                interval=latest,
                mean=mean,
                stddev=stddev,
                z_score=z,
                ts=now,
            )
        return None
