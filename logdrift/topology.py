"""Field co-occurrence topology: tracks which field-value pairs appear together."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


class TopologyError(Exception):
    """Raised when topology configuration or usage is invalid."""


@dataclass
class TopologyConfig:
    fields: List[str]
    min_support: int = 5

    def __post_init__(self) -> None:
        if len(self.fields) < 2:
            raise TopologyError("fields must contain at least two entries")
        if any(not f for f in self.fields):
            raise TopologyError("field names must be non-empty strings")
        if self.min_support < 1:
            raise TopologyError("min_support must be >= 1")


@dataclass
class EdgeStats:
    a_value: str
    b_value: str
    count: int = 0

    def __repr__(self) -> str:  # pragma: no cover
        return f"EdgeStats({self.a_value!r} <-> {self.b_value!r}, count={self.count})"


class Topology:
    """Counts co-occurrences between two field values across observed records."""

    def __init__(self, config: TopologyConfig) -> None:
        self.config = config
        self._field_a = config.fields[0]
        self._field_b = config.fields[1]
        # (a_value, b_value) -> EdgeStats
        self._edges: Dict[Tuple[str, str], EdgeStats] = {}
        self._a_totals: Dict[str, int] = defaultdict(int)

    def observe(self, record: dict) -> None:
        """Update co-occurrence counts from a log record."""
        a_val = str(record.get(self._field_a, ""))
        b_val = str(record.get(self._field_b, ""))
        if not a_val or not b_val:
            return
        key = (a_val, b_val)
        if key not in self._edges:
            self._edges[key] = EdgeStats(a_value=a_val, b_value=b_val)
        self._edges[key].count += 1
        self._a_totals[a_val] += 1

    def edges(self, min_support: int | None = None) -> List[EdgeStats]:
        """Return edges meeting the support threshold."""
        threshold = min_support if min_support is not None else self.config.min_support
        return [e for e in self._edges.values() if e.count >= threshold]

    def rare_edges(self, record: dict) -> List[EdgeStats]:
        """Return edges for values in *record* that are below min_support."""
        a_val = str(record.get(self._field_a, ""))
        b_val = str(record.get(self._field_b, ""))
        if not a_val or not b_val:
            return []
        key = (a_val, b_val)
        edge = self._edges.get(key)
        if edge is None or edge.count < self.config.min_support:
            count = edge.count if edge else 0
            return [EdgeStats(a_value=a_val, b_value=b_val, count=count)]
        return []
