"""Labeller – attach human-readable severity labels to scored events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from logdrift.scorer import ScoredEvent


class LabelError(Exception):
    """Raised when labeller configuration is invalid."""


@dataclass
class LabelRule:
    """Map a score range [min_score, max_score) to a label string."""

    label: str
    min_score: float
    max_score: float

    def __post_init__(self) -> None:
        if not self.label:
            raise LabelError("label must be a non-empty string")
        if self.min_score < 0.0 or self.max_score < 0.0:
            raise LabelError("min_score and max_score must be non-negative")
        if self.min_score >= self.max_score:
            raise LabelError("min_score must be less than max_score")

    def matches(self, score: float) -> bool:
        return self.min_score <= score < self.max_score


@dataclass
class Labeller:
    """Assigns a label to a ScoredEvent based on its composite score."""

    _rules: List[LabelRule] = field(default_factory=list, init=False)
    default_label: str = "unknown"

    def add_rule(self, rule: LabelRule) -> None:
        self._rules.append(rule)

    def label(self, event: ScoredEvent) -> Tuple[ScoredEvent, str]:
        """Return (event, label) for the given scored event."""
        for rule in self._rules:
            if rule.matches(event.score):
                return event, rule.label
        return event, self.default_label

    def label_all(
        self, events: List[ScoredEvent]
    ) -> List[Tuple[ScoredEvent, str]]:
        return [self.label(e) for e in events]


def build_default_labeller() -> Labeller:
    """Return a Labeller pre-configured with low / medium / high / critical bands."""
    lb = Labeller(default_label="unknown")
    lb.add_rule(LabelRule(label="low", min_score=0.0, max_score=0.4))
    lb.add_rule(LabelRule(label="medium", min_score=0.4, max_score=0.7))
    lb.add_rule(LabelRule(label="high", min_score=0.7, max_score=0.9))
    lb.add_rule(LabelRule(label="critical", min_score=0.9, max_score=1.01))
    return lb
