"""Trend detector: flags fields whose value frequency changes significantly over time."""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional


class TrendError(Exception):
    """Raised for invalid Trend configuration."""


@dataclass
class TrendConfig:
    field: str
    window_seconds: float = 60.0
    min_periods: int = 2
    spike_factor: float = 2.0

    def __post_init__(self) -> None:
        if not self.field:
            raise TrendError("field must not be empty")
        if self.window_seconds <= 0:
            raise TrendError("window_seconds must be positive")
        if self.min_periods < 2:
            raise TrendError("min_periods must be at least 2")
        if self.spike_factor <= 1.0:
            raise TrendError("spike_factor must be greater than 1.0")


@dataclass
class TrendPoint:
    timestamp: float
    value: str
    count: int = 1


@dataclass
class TrendAnomaly:
    field: str
    value: str
    previous_rate: float
    current_rate: float
    spike_factor: float

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"TrendAnomaly(field={self.field!r}, value={self.value!r}, "
            f"prev={self.previous_rate:.3f}, curr={self.current_rate:.3f})"
        )


class Trend:
    """Sliding-window trend detector for a single field."""

    def __init__(self, config: TrendConfig) -> None:
        self._cfg = config
        self._buckets: Dict[str, Deque[float]] = defaultdict(deque)

    def observe(self, timestamp: float, value: str) -> Optional[TrendAnomaly]:
        """Record an observation; return TrendAnomaly if a spike is detected."""
        cfg = self._cfg
        bucket = self._buckets[value]
        cutoff = timestamp - cfg.window_seconds

        # purge stale timestamps
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        # split into two half-windows to compare rates
        half = cfg.window_seconds / 2.0
        mid = timestamp - half
        old_count = sum(1 for t in bucket if t < mid)
        new_count = sum(1 for t in bucket if t >= mid)

        bucket.append(timestamp)

        total_periods = old_count + new_count
        if total_periods < cfg.min_periods or old_count == 0:
            return None

        prev_rate = old_count / half
        curr_rate = new_count / half
        if prev_rate > 0 and curr_rate / prev_rate >= cfg.spike_factor:
            return TrendAnomaly(
                field=cfg.field,
                value=value,
                previous_rate=prev_rate,
                current_rate=curr_rate,
                spike_factor=curr_rate / prev_rate,
            )
        return None
