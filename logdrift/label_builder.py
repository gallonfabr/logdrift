"""Helpers to build a Labeller from a list of config dicts."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from logdrift.label import LabelError, LabelRule, Labeller
from logdrift.scorer import ScoredEvent


def build_labeller(
    rules: List[Dict[str, Any]],
    default_label: str = "unknown",
) -> Labeller:
    """Construct a :class:`Labeller` from a list of rule dicts.

    Each dict must contain ``label`` (str), ``min_score`` (float), and
    ``max_score`` (float).

    Example::

        build_labeller([
            {"label": "low",    "min_score": 0.0, "max_score": 0.4},
            {"label": "high",   "min_score": 0.7, "max_score": 1.01},
        ])
    """
    lb = Labeller(default_label=default_label)
    for idx, entry in enumerate(rules):
        try:
            rule = LabelRule(
                label=entry["label"],
                min_score=float(entry["min_score"]),
                max_score=float(entry["max_score"]),
            )
        except KeyError as exc:
            raise LabelError(
                f"rule[{idx}] is missing required key {exc}"
            ) from exc
        lb.add_rule(rule)
    return lb


def label_events(
    events: List[ScoredEvent],
    labeller: Labeller,
) -> List[Tuple[ScoredEvent, str]]:
    """Convenience wrapper – label a sequence of scored events."""
    return labeller.label_all(events)
