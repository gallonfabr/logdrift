"""Sequence anomaly detector — flags unexpected transitions between field values."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


class SequenceError(Exception):
    """Raised when a Sequence is misconfigured."""


@dataclass
class SequenceConfig:
    field: str
    min_support: int = 5
    window: int = 300  # seconds, informational only

    def __post_init__(self) -> None:
        if not self.field:
            raise SequenceError("field must not be empty")
        if self.min_support < 1:
            raise SequenceError("min_support must be >= 1")
        if self.window <= 0:
            raise SequenceError("window must be positive")


@dataclass
class TransitionAnomaly:
    field: str
    from_value: str
    to_value: str
    count: int
    min_support: int

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"TransitionAnomaly(field={self.field!r}, "
            f"{self.from_value!r} -> {self.to_value!r}, "
            f"count={self.count}, min_support={self.min_support})"
        )


class Sequence:
    """Tracks field-value transitions and surfaces rare ones."""

    def __init__(self, config: SequenceConfig) -> None:
        self._cfg = config
        # counts[(from, to)] -> int
        self._counts: Dict[Tuple[str, str], int] = defaultdict(int)
        self._prev: Optional[str] = None

    @property
    def config(self) -> SequenceConfig:
        return self._cfg

    def observe(self, value: str) -> Optional[TransitionAnomaly]:
        """Record a new value and return an anomaly if the transition is rare."""
        if value is None:
            return None
        value = str(value)
        anomaly: Optional[TransitionAnomaly] = None
        if self._prev is not None:
            pair = (self._prev, value)
            self._counts[pair] += 1
            if self._counts[pair] < self._cfg.min_support:
                anomaly = TransitionAnomaly(
                    field=self._cfg.field,
                    from_value=self._prev,
                    to_value=value,
                    count=self._counts[pair],
                    min_support=self._cfg.min_support,
                )
        self._prev = value
        return anomaly

    def transition_count(self, from_value: str, to_value: str) -> int:
        return self._counts.get((from_value, to_value), 0)

    def reset(self) -> None:
        """Clear all state."""
        self._counts.clear()
        self._prev = None
