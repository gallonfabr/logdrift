"""Track which fields appear in log records over a sliding time window.

Useful for detecting when expected fields go missing or unexpected fields
suddenly appear in structured logs.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


class FieldTrackerError(ValueError):
    """Raised for invalid FieldTracker configuration."""


@dataclass
class FieldTrackerConfig:
    window_seconds: float = 300.0
    expected_fields: Optional[Set[str]] = None

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise FieldTrackerError(
                f"window_seconds must be positive, got {self.window_seconds}"
            )
        if self.expected_fields is not None and not self.expected_fields:
            raise FieldTrackerError("expected_fields must not be empty when provided")


@dataclass
class FieldAppearance:
    field_name: str
    first_seen: float
    last_seen: float
    count: int

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"FieldAppearance(field={self.field_name!r}, count={self.count}, "
            f"last_seen={self.last_seen:.3f})"
        )


@dataclass
class FieldDriftAnomaly:
    kind: str  # 'missing' or 'unexpected'
    field_name: str
    ts: float

    def __repr__(self) -> str:  # pragma: no cover
        return f"FieldDriftAnomaly(kind={self.kind!r}, field={self.field_name!r})"


class FieldTracker:
    def __init__(self, config: FieldTrackerConfig) -> None:
        self._cfg = config
        # field_name -> list of timestamps
        self._events: Dict[str, List[float]] = defaultdict(list)

    def observe(self, record: dict, ts: Optional[float] = None) -> None:
        """Record which fields are present in *record* at time *ts*."""
        now = ts if ts is not None else time.time()
        for key in record:
            self._events[key].append(now)
        self._purge(now)

    def _purge(self, now: float) -> None:
        cutoff = now - self._cfg.window_seconds
        for key in list(self._events):
            self._events[key] = [t for t in self._events[key] if t >= cutoff]
            if not self._events[key]:
                del self._events[key]

    def active_fields(self, ts: Optional[float] = None) -> Set[str]:
        """Return the set of fields seen within the current window."""
        now = ts if ts is not None else time.time()
        self._purge(now)
        return set(self._events.keys())

    def appearance(self, field_name: str, ts: Optional[float] = None) -> Optional[FieldAppearance]:
        now = ts if ts is not None else time.time()
        self._purge(now)
        times = self._events.get(field_name)
        if not times:
            return None
        return FieldAppearance(
            field_name=field_name,
            first_seen=times[0],
            last_seen=times[-1],
            count=len(times),
        )

    def anomalies(self, record: dict, ts: Optional[float] = None) -> List[FieldDriftAnomaly]:
        """Return drift anomalies relative to *expected_fields* for *record*."""
        if self._cfg.expected_fields is None:
            return []
        now = ts if ts is not None else time.time()
        present = set(record.keys())
        result: List[FieldDriftAnomaly] = []
        for f in self._cfg.expected_fields - present:
            result.append(FieldDriftAnomaly(kind="missing", field_name=f, ts=now))
        for f in present - self._cfg.expected_fields:
            result.append(FieldDriftAnomaly(kind="unexpected", field_name=f, ts=now))
        return result
