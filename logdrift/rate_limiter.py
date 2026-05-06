"""Rate limiter: tracks per-field event rates and flags when they exceed a threshold."""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional


class RateLimiterError(ValueError):
    """Raised when RateLimiterConfig receives invalid arguments."""


@dataclass
class RateLimiterConfig:
    field: str
    window_seconds: float = 60.0
    max_events: int = 100

    def __post_init__(self) -> None:
        if not self.field:
            raise RateLimiterError("field must not be empty")
        if self.window_seconds <= 0:
            raise RateLimiterError("window_seconds must be positive")
        if self.max_events < 1:
            raise RateLimiterError("max_events must be at least 1")


class RateLimiter:
    """Sliding-window rate limiter keyed on a record field value."""

    def __init__(self, config: RateLimiterConfig) -> None:
        self._config = config
        # value -> deque of timestamps
        self._buckets: Dict[str, Deque[float]] = defaultdict(deque)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def observe(self, record: dict, *, _now: Optional[float] = None) -> bool:
        """Record an event and return True if the rate limit is exceeded."""
        ts = _now if _now is not None else time.monotonic()
        value = str(record.get(self._config.field, ""))
        bucket = self._buckets[value]
        self._purge(bucket, ts)
        bucket.append(ts)
        return len(bucket) > self._config.max_events

    def current_count(self, value: str, *, _now: Optional[float] = None) -> int:
        """Return the number of events for *value* within the current window."""
        ts = _now if _now is not None else time.monotonic()
        bucket = self._buckets.get(value)
        if bucket is None:
            return 0
        self._purge(bucket, ts)
        return len(bucket)

    def reset(self, value: Optional[str] = None) -> None:
        """Clear counts for *value*, or all values if None."""
        if value is None:
            self._buckets.clear()
        else:
            self._buckets.pop(value, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _purge(self, bucket: Deque[float], now: float) -> None:
        cutoff = now - self._config.window_seconds
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
