"""Pattern counter: tracks how often each unique field-value pattern appears
within a rolling time window and flags rare combinations."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from time import monotonic
from typing import Dict, Iterable, List, Optional, Tuple


class PatternCounterError(ValueError):
    """Raised when PatternCounter is misconfigured."""


@dataclass
class PatternCounterConfig:
    fields: List[str]
    window_seconds: float = 60.0
    min_count: int = 2

    def __post_init__(self) -> None:
        if not self.fields:
            raise PatternCounterError("fields must not be empty")
        if self.window_seconds <= 0:
            raise PatternCounterError("window_seconds must be positive")
        if self.min_count < 1:
            raise PatternCounterError("min_count must be at least 1")


@dataclass
class PatternHit:
    pattern: Tuple[str, ...]
    count: int
    is_rare: bool

    def __repr__(self) -> str:  # pragma: no cover
        return f"PatternHit(pattern={self.pattern!r}, count={self.count}, rare={self.is_rare})"


class PatternCounter:
    """Counts field-value patterns within a sliding time window."""

    def __init__(self, config: PatternCounterConfig) -> None:
        self._cfg = config
        # maps pattern -> deque of timestamps
        self._buckets: Dict[Tuple[str, ...], deque] = defaultdict(deque)

    def _purge(self, now: float) -> None:
        cutoff = now - self._cfg.window_seconds
        for dq in self._buckets.values():
            while dq and dq[0] < cutoff:
                dq.popleft()

    def observe(self, record: dict, now: Optional[float] = None) -> PatternHit:
        """Record a pattern extracted from *record* and return its hit info."""
        ts = now if now is not None else monotonic()
        self._purge(ts)
        pattern = tuple(str(record.get(f, "")) for f in self._cfg.fields)
        self._buckets[pattern].append(ts)
        count = len(self._buckets[pattern])
        return PatternHit(pattern=pattern, count=count, is_rare=count < self._cfg.min_count)

    def counts(self) -> Dict[Tuple[str, ...], int]:
        """Return current (post-purge) counts for all known patterns."""
        now = monotonic()
        self._purge(now)
        return {p: len(dq) for p, dq in self._buckets.items() if dq}

    def rare_patterns(self) -> List[Tuple[Tuple[str, ...], int]]:
        """Return patterns whose count is below *min_count*."""
        return [(p, c) for p, c in self.counts().items() if c < self._cfg.min_count]
