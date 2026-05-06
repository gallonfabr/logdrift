"""Filter that keeps only ScoredEvents whose combined score meets a threshold."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from logdrift.scorer import ScoredEvent


class ScoreFilterError(Exception):
    """Raised when the filter is misconfigured."""


@dataclass
class ScoreFilterConfig:
    min_score: float = 0.5

    def __post_init__(self) -> None:
        if not (0.0 <= self.min_score <= 1.0):
            raise ScoreFilterError(
                f"min_score must be in [0, 1], got {self.min_score}"
            )


class ScoreFilter:
    """Passes through only events whose combined_score >= min_score."""

    def __init__(self, config: ScoreFilterConfig | None = None) -> None:
        self._config = config or ScoreFilterConfig()

    @property
    def min_score(self) -> float:
        return self._config.min_score

    def apply(self, events: Iterable[ScoredEvent]) -> List[ScoredEvent]:
        """Return a list of events that pass the score threshold."""
        return [
            ev for ev in events
            if ev.combined_score >= self._config.min_score
        ]

    def passes(self, event: ScoredEvent) -> bool:
        """Return True when *event* meets the threshold."""
        return event.combined_score >= self._config.min_score
