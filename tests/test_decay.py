"""Tests for logdrift.decay and logdrift.decay_builder."""
from __future__ import annotations

import time
import pytest

from logdrift.decay import DecayConfig, DecayError, DecayScorer, DecayedScore
from logdrift.decay_builder import build_scorer, update_from_events, top_scores
from logdrift.detector import AnomalyEvent


# ---------------------------------------------------------------------------
# DecayConfig validation
# ---------------------------------------------------------------------------
class TestDecayConfig:
    def test_valid_config_created(self):
        cfg = DecayConfig(half_life=30.0, min_score=0.1)
        assert cfg.half_life == 30.0
        assert cfg.min_score == 0.1

    def test_non_positive_half_life_raises(self):
        with pytest.raises(DecayError, match="half_life"):
            DecayConfig(half_life=0.0)

    def test_negative_half_life_raises(self):
        with pytest.raises(DecayError, match="half_life"):
            DecayConfig(half_life=-5.0)

    def test_min_score_gte_one_raises(self):
        with pytest.raises(DecayError, match="min_score"):
            DecayConfig(min_score=1.0)

    def test_negative_min_score_raises(self):
        with pytest.raises(DecayError, match="min_score"):
            DecayConfig(min_score=-0.1)


# ---------------------------------------------------------------------------
# DecayScorer behaviour
# ---------------------------------------------------------------------------
class TestDecayScorer:
    def test_first_update_returns_new_score(self):
        scorer = DecayScorer()
        ds = scorer.update("status", "500", 0.8)
        assert isinstance(ds, DecayedScore)
        assert ds.score == pytest.approx(0.8)

    def test_get_unknown_key_returns_none(self):
        scorer = DecayScorer()
        assert scorer.get("status", "200") is None

    def test_score_accumulates_on_repeated_updates(self):
        scorer = DecayScorer(DecayConfig(half_life=3600.0))
        scorer.update("status", "500", 0.5)
        ds = scorer.update("status", "500", 0.3)
        # Very little time has passed so decay is negligible
        assert ds.score > 0.7

    def test_score_decays_over_time(self, monkeypatch):
        base = 1000.0
        calls = [base]

        def fake_monotonic():
            return calls.pop(0)

        monkeypatch.setattr(time, "monotonic", fake_monotonic)
        scorer = DecayScorer(DecayConfig(half_life=1.0))
        # First call inside update (initial insert)
        calls.extend([base, base + 2.0, base + 2.0])
        scorer.update("level", "error", 1.0)
        ds = scorer.update("level", "error", 0.0)
        # After 2 seconds with half_life=1 score should be ~0.25
        assert ds.score == pytest.approx(0.25, abs=0.01)

    def test_clear_removes_all_state(self):
        scorer = DecayScorer()
        scorer.update("x", "y", 1.0)
        scorer.clear()
        assert scorer.get("x", "y") is None


# ---------------------------------------------------------------------------
# decay_builder helpers
# ---------------------------------------------------------------------------
def _event(field: str, value: str, score: float) -> AnomalyEvent:
    return AnomalyEvent(field=field, value=value, score=score, count=1, total=10)


class TestDecayBuilder:
    def test_build_scorer_returns_instance(self):
        s = build_scorer(half_life=120.0)
        assert isinstance(s, DecayScorer)

    def test_update_from_events_returns_list(self):
        s = build_scorer()
        events = [_event("status", "500", 0.9), _event("host", "db1", 0.4)]
        results = update_from_events(s, events)
        assert len(results) == 2
        assert all(isinstance(r, DecayedScore) for r in results)

    def test_top_scores_respects_n(self):
        s = build_scorer(half_life=3600.0)
        update_from_events(s, [
            _event("f", "a", 0.9),
            _event("f", "b", 0.5),
            _event("f", "c", 0.1),
        ])
        # Asking for top 2 from 3 known keys
        top = top_scores(s, ["f"], {"f": "a"}, n=2)
        assert len(top) <= 2

    def test_top_scores_sorted_descending(self):
        s = build_scorer(half_life=3600.0)
        s.update("status", "500", 0.9)
        s.update("level", "error", 0.4)
        top = top_scores(s, ["status", "level"], {"status": "500", "level": "error"})
        scores = [d.score for d in top]
        assert scores == sorted(scores, reverse=True)
