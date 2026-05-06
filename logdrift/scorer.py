"""Anomaly scoring: combines detector output and baseline frequency into a
normalised 0-1 score that downstream components can threshold on."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from logdrift.detector import AnomalyEvent
from logdrift.baseline import Baseline


class ScorerError(Exception):
    """Raised when the scorer is misconfigured."""


@dataclass
class ScorerConfig:
    detector_weight: float = 0.6
    baseline_weight: float = 0.4

    def __post_init__(self) -> None:
        for name, val in (("detector_weight", self.detector_weight),
                          ("baseline_weight", self.baseline_weight)):
            if not (0.0 <= val <= 1.0):
                raise ScorerError(f"{name} must be in [0, 1], got {val}")
        total = self.detector_weight + self.baseline_weight
        if abs(total - 1.0) > 1e-9:
            raise ScorerError(
                f"Weights must sum to 1.0, got {total:.4f}"
            )


@dataclass
class ScoredEvent:
    event: AnomalyEvent
    detector_score: float
    baseline_score: float
    combined_score: float

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ScoredEvent(field={self.event.field!r}, "
            f"value={self.event.value!r}, "
            f"combined={self.combined_score:.3f})"
        )


class Scorer:
    """Combines detector anomaly score with inverse baseline frequency."""

    def __init__(
        self,
        config: Optional[ScorerConfig] = None,
        baseline: Optional[Baseline] = None,
    ) -> None:
        self._config = config or ScorerConfig()
        self._baseline = baseline

    def score(self, event: AnomalyEvent) -> ScoredEvent:
        """Return a ScoredEvent with a combined 0-1 anomaly score."""
        det_score = min(1.0, max(0.0, event.score))

        if self._baseline is not None:
            total = self._baseline.total(event.field)
            count = self._baseline.get_count(event.field, str(event.value))
            if total > 0:
                freq = count / total
                base_score = 1.0 - freq
            else:
                base_score = 1.0
        else:
            base_score = det_score

        combined = (
            self._config.detector_weight * det_score
            + self._config.baseline_weight * base_score
        )
        combined = min(1.0, max(0.0, combined))
        return ScoredEvent(
            event=event,
            detector_score=det_score,
            baseline_score=base_score,
            combined_score=combined,
        )
