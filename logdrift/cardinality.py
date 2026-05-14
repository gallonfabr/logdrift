"""Cardinality tracker — alerts when a field's distinct value count exceeds a threshold."""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class CardinalityError(Exception):
    """Raised for invalid cardinality configuration."""


@dataclass
class CardinalityConfig:
    field: str
    max_distinct: int = 100
    window_seconds: float = 60.0
    min_samples: int = 5

    def __post_init__(self) -> None:
        if not self.field:
            raise CardinalityError("field must not be empty")
        if self.max_distinct < 1:
            raise CardinalityError("max_distinct must be >= 1")
        if self.window_seconds <= 0:
            raise CardinalityError("window_seconds must be positive")
        if self.min_samples < 1:
            raise CardinalityError("min_samples must be >= 1")


@dataclass
class CardinalityAnomaly:
    field: str
    distinct_count: int
    max_distinct: int
    sample_values: List[str]
    timestamp: float = field(default_factory=time.time)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CardinalityAnomaly(field={self.field!r}, "
            f"distinct={self.distinct_count}, max={self.max_distinct})"
        )


class CardinalityTracker:
    """Tracks distinct values for a field within a sliding time window."""

    def __init__(self, config: CardinalityConfig) -> None:
        self.config = config
        # value -> list of timestamps
        self._buckets: Dict[str, List[float]] = defaultdict(list)

    def observe(self, record: dict, ts: Optional[float] = None) -> Optional[CardinalityAnomaly]:
        now = ts if ts is not None else time.time()
        value = record.get(self.config.field)
        if value is None:
            return None
        key = str(value)
        self._buckets[key].append(now)
        self._purge(now)
        total = sum(len(v) for v in self._buckets.values())
        if total < self.config.min_samples:
            return None
        distinct = len(self._buckets)
        if distinct > self.config.max_distinct:
            sample = list(self._buckets.keys())[:10]
            return CardinalityAnomaly(
                field=self.config.field,
                distinct_count=distinct,
                max_distinct=self.config.max_distinct,
                sample_values=sample,
                timestamp=now,
            )
        return None

    def _purge(self, now: float) -> None:
        cutoff = now - self.config.window_seconds
        empty = [k for k, ts_list in self._buckets.items()
                 if not [t for t in ts_list if t >= cutoff]]
        for k in empty:
            del self._buckets[k]
        for k in list(self._buckets):
            self._buckets[k] = [t for t in self._buckets[k] if t >= cutoff]

    def distinct_count(self) -> int:
        return len(self._buckets)
