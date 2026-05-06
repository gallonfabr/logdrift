"""Convenience factory for building a Scorer from plain config dicts."""
from __future__ import annotations

from typing import Any, Dict, Optional

from logdrift.scorer import Scorer, ScorerConfig
from logdrift.baseline import Baseline


def build_scorer(
    config: Optional[Dict[str, Any]] = None,
    baseline: Optional[Baseline] = None,
) -> Scorer:
    """Build a :class:`Scorer` from an optional mapping and baseline.

    *config* keys mirror :class:`ScorerConfig` field names::

        {
            "detector_weight": 0.7,
            "baseline_weight": 0.3,
        }

    Missing keys fall back to :class:`ScorerConfig` defaults.
    """
    cfg_dict: Dict[str, Any] = config or {}
    scorer_config = ScorerConfig(
        detector_weight=cfg_dict.get("detector_weight", 0.6),
        baseline_weight=cfg_dict.get("baseline_weight", 0.4),
    )
    return Scorer(config=scorer_config, baseline=baseline)


def score_events(
    events: list,
    scorer: Scorer,
) -> list:
    """Score a list of :class:`AnomalyEvent` objects, returning ScoredEvents."""
    return [scorer.score(ev) for ev in events]
