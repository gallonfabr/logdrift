"""Convenience helpers for building and using DecayScorer instances."""
from __future__ import annotations

from typing import Dict, Iterable, List

from logdrift.decay import DecayConfig, DecayScorer, DecayedScore
from logdrift.detector import AnomalyEvent


def build_scorer(half_life: float = 60.0, min_score: float = 0.0) -> DecayScorer:
    """Create a :class:`DecayScorer` from plain keyword arguments."""
    return DecayScorer(DecayConfig(half_life=half_life, min_score=min_score))


def update_from_event(scorer: DecayScorer, event: AnomalyEvent) -> DecayedScore:
    """Feed a single :class:`AnomalyEvent` into *scorer* and return the result."""
    return scorer.update(
        field=event.field,
        value=str(event.value),
        new_score=event.score,
    )


def update_from_events(
    scorer: DecayScorer,
    events: Iterable[AnomalyEvent],
) -> List[DecayedScore]:
    """Feed multiple events and return one :class:`DecayedScore` per event."""
    return [update_from_event(scorer, ev) for ev in events]


def top_scores(
    scorer: DecayScorer,
    fields: Iterable[str],
    values: Dict[str, str],
    n: int = 5,
) -> List[DecayedScore]:
    """Return up to *n* highest current scores for the given field/value pairs.

    *values* maps field name -> value string.
    """
    results: List[DecayedScore] = []
    for f in fields:
        v = values.get(f)
        if v is None:
            continue
        ds = scorer.get(f, v)
        if ds is not None:
            results.append(ds)
    results.sort(key=lambda d: d.score, reverse=True)
    return results[:n]
