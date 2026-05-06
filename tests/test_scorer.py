"""Tests for logdrift.scorer and logdrift.scorer_builder."""
from __future__ import annotations

import pytest

from logdrift.detector import AnomalyEvent
from logdrift.baseline import Baseline
from logdrift.scorer import Scorer, ScorerConfig, ScoredEvent, ScorerError
from logdrift.scorer_builder import build_scorer, score_events


def _event(score: float = 0.8, field: str = "status", value: str = "500") -> AnomalyEvent:
    return AnomalyEvent(field=field, value=value, score=score, count=1, total=100)


# ---------------------------------------------------------------------------
# ScorerConfig validation
# ---------------------------------------------------------------------------

class TestScorerConfig:
    def test_default_weights_sum_to_one(self):
        cfg = ScorerConfig()
        assert abs(cfg.detector_weight + cfg.baseline_weight - 1.0) < 1e-9

    def test_invalid_weight_raises(self):
        with pytest.raises(ScorerError):
            ScorerConfig(detector_weight=1.5, baseline_weight=-0.5)

    def test_weights_not_summing_to_one_raises(self):
        with pytest.raises(ScorerError):
            ScorerConfig(detector_weight=0.3, baseline_weight=0.3)


# ---------------------------------------------------------------------------
# Scorer without baseline
# ---------------------------------------------------------------------------

class TestScorerNoBaseline:
    def test_returns_scored_event(self):
        scorer = Scorer()
        result = scorer.score(_event(score=0.9))
        assert isinstance(result, ScoredEvent)

    def test_combined_score_clamped_to_one(self):
        scorer = Scorer()
        result = scorer.score(_event(score=1.0))
        assert result.combined_score <= 1.0

    def test_combined_score_non_negative(self):
        scorer = Scorer()
        result = scorer.score(_event(score=0.0))
        assert result.combined_score >= 0.0

    def test_detector_score_reflects_event_score(self):
        scorer = Scorer()
        result = scorer.score(_event(score=0.75))
        assert result.detector_score == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# Scorer with baseline
# ---------------------------------------------------------------------------

class TestScorerWithBaseline:
    def _baseline(self) -> Baseline:
        bl = Baseline()
        for _ in range(90):
            bl.record({"status": "200"})
        for _ in range(10):
            bl.record({"status": "500"})
        return bl

    def test_common_value_lowers_baseline_score(self):
        scorer = Scorer(baseline=self._baseline())
        ev = _event(score=0.5, value="200")
        result = scorer.score(ev)
        assert result.baseline_score < 0.5

    def test_rare_value_raises_baseline_score(self):
        scorer = Scorer(baseline=self._baseline())
        ev = _event(score=0.5, value="500")
        result = scorer.score(ev)
        assert result.baseline_score > 0.5

    def test_unknown_field_gives_full_baseline_score(self):
        scorer = Scorer(baseline=self._baseline())
        ev = _event(score=0.5, field="unknown_field", value="x")
        result = scorer.score(ev)
        assert result.baseline_score == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# scorer_builder
# ---------------------------------------------------------------------------

class TestScorerBuilder:
    def test_build_scorer_defaults(self):
        scorer = build_scorer()
        assert isinstance(scorer, Scorer)

    def test_build_scorer_custom_weights(self):
        scorer = build_scorer({"detector_weight": 0.7, "baseline_weight": 0.3})
        result = scorer.score(_event(score=1.0))
        assert result.combined_score == pytest.approx(1.0)

    def test_score_events_returns_list(self):
        scorer = build_scorer()
        events = [_event(score=0.5), _event(score=0.9)]
        results = score_events(events, scorer)
        assert len(results) == 2
        assert all(isinstance(r, ScoredEvent) for r in results)
