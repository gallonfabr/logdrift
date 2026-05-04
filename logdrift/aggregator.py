"""Time-window aggregation of anomaly events for reporting and throttling."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from logdrift.detector import AnomalyEvent


@dataclass
class WindowStats:
    """Aggregated statistics for a single field within a time window."""

    field_name: str
    count: int = 0
    max_score: float = 0.0
    values: List[str] = field(default_factory=list)

    def update(self, event: AnomalyEvent) -> None:
        self.count += 1
        if event.score > self.max_score:
            self.max_score = event.score
        self.values.append(str(event.value))

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"WindowStats(field={self.field_name!r}, count={self.count}, "
            f"max_score={self.max_score:.3f})"
        )


class Aggregator:
    """Collect AnomalyEvents and expose per-field stats over a rolling window."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = window_seconds
        self._events: List[tuple[float, AnomalyEvent]] = []

    def add(self, event: AnomalyEvent, ts: Optional[float] = None) -> None:
        """Record an anomaly event, optionally with an explicit timestamp."""
        self._events.append((ts if ts is not None else time.monotonic(), event))

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        self._events = [(t, e) for t, e in self._events if t >= cutoff]

    def stats(self, now: Optional[float] = None) -> Dict[str, WindowStats]:
        """Return per-field WindowStats for events still inside the window."""
        if now is None:
            now = time.monotonic()
        self._prune(now)
        result: Dict[str, WindowStats] = defaultdict(lambda: WindowStats(""))
        for _, event in self._events:
            if event.field not in result:
                result[event.field] = WindowStats(event.field)
            result[event.field].update(event)
        return dict(result)

    def total(self, now: Optional[float] = None) -> int:
        """Return total number of events still inside the window."""
        if now is None:
            now = time.monotonic()
        self._prune(now)
        return len(self._events)
