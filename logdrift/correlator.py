"""Field correlation tracker: detects when two fields co-occur unexpectedly."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Tuple


class CorrelatorError(Exception):
    """Raised when the Correlator is misconfigured."""


@dataclass
class PairStats:
    """Counts for a (field_a_value, field_b_value) pair."""
    count: int = 0
    total_seen: int = 0

    @property
    def frequency(self) -> float:
        if self.total_seen == 0:
            return 0.0
        return self.count / self.total_seen

    def __repr__(self) -> str:  # pragma: no cover
        return f"PairStats(count={self.count}, freq={self.frequency:.3f})"


class Correlator:
    """Tracks co-occurrence frequency of values across two log fields.

    After a warm-up period (``min_samples`` observations of *field_a*),
    any pair whose frequency falls below ``threshold`` is flagged as
    anomalous.
    """

    def __init__(self, field_a: str, field_b: str, threshold: float = 0.01,
                 min_samples: int = 30) -> None:
        if not field_a or not field_b:
            raise CorrelatorError("field_a and field_b must be non-empty strings")
        if not 0.0 < threshold < 1.0:
            raise CorrelatorError("threshold must be between 0 and 1 (exclusive)")
        if min_samples < 1:
            raise CorrelatorError("min_samples must be >= 1")

        self.field_a = field_a
        self.field_b = field_b
        self.threshold = threshold
        self.min_samples = min_samples

        # counts[val_a][val_b] -> PairStats
        self._counts: Dict[str, Dict[str, PairStats]] = defaultdict(lambda: defaultdict(PairStats))
        self._totals: Dict[str, int] = defaultdict(int)

    def observe(self, record: dict) -> None:
        """Record a co-occurrence from *record* if both fields are present."""
        val_a = record.get(self.field_a)
        val_b = record.get(self.field_b)
        if val_a is None or val_b is None:
            return
        val_a, val_b = str(val_a), str(val_b)
        self._totals[val_a] += 1
        bucket = self._counts[val_a]
        for stats in bucket.values():
            stats.total_seen += 1
        ps = bucket[val_b]
        ps.count += 1
        ps.total_seen = self._totals[val_a]

    def is_anomalous(self, val_a: str, val_b: str) -> bool:
        """Return True if the pair is below threshold after warm-up."""
        val_a, val_b = str(val_a), str(val_b)
        if self._totals.get(val_a, 0) < self.min_samples:
            return False
        ps = self._counts[val_a].get(val_b)
        if ps is None:
            return True
        return ps.frequency < self.threshold

    def stats(self, val_a: str, val_b: str) -> PairStats:
        """Return the PairStats for a specific pair (creates a blank entry if missing)."""
        return self._counts[str(val_a)][str(val_b)]
