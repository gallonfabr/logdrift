"""Tests for logdrift.score_filter."""
from __future__ import annotations

import pytest

from logdrift.detector import AnomalyEvent
from logdrift.scorer import ScoredEvent
from logdrift.score_filter import ScoreFilter, ScoreFilterConfig, ScoreFilterError


def _scored(combined: float) -> ScoredEvent:
    ev = AnomalyEvent(field="level", value="ERROR", score=combined, count=1, total=50)
    return ScoredEvent(
        event=ev,
        detector_score=combined,
        baseline_score=combined,
        combined_score=combined,
    )


class TestScoreFilterConfig:
    def test_default_min_score(self):
        cfg = ScoreFilterConfig()
        assert cfg.min_score == 0.5

    def test_invalid_min_score_raises(self):
        with pytest.raises(ScoreFilterError):
            ScoreFilterConfig(min_score=1.5)

    def test_negative_min_score_raises(self):
        with pytest.raises(ScoreFilterError):
            ScoreFilterConfig(min_score=-0.1)


class TestScoreFilter:
    def test_passes_event_above_threshold(self):
        sf = ScoreFilter(ScoreFilterConfig(min_score=0.5))
        assert sf.passes(_scored(0.8)) is True

    def test_blocks_event_below_threshold(self):
        sf = ScoreFilter(ScoreFilterConfig(min_score=0.5))
        assert sf.passes(_scored(0.3)) is False

    def test_passes_event_equal_to_threshold(self):
        sf = ScoreFilter(ScoreFilterConfig(min_score=0.5))
        assert sf.passes(_scored(0.5)) is True

    def test_apply_filters_list(self):
        sf = ScoreFilter(ScoreFilterConfig(min_score=0.6))
        events = [_scored(0.9), _scored(0.4), _scored(0.7), _scored(0.1)]
        result = sf.apply(events)
        assert len(result) == 2
        assert all(e.combined_score >= 0.6 for e in result)

    def test_apply_empty_list(self):
        sf = ScoreFilter()
        assert sf.apply([]) == []

    def test_min_score_zero_passes_all(self):
        sf = ScoreFilter(ScoreFilterConfig(min_score=0.0))
        events = [_scored(0.0), _scored(0.5), _scored(1.0)]
        assert len(sf.apply(events)) == 3

    def test_min_score_one_passes_only_perfect(self):
        sf = ScoreFilter(ScoreFilterConfig(min_score=1.0))
        events = [_scored(0.9), _scored(1.0)]
        result = sf.apply(events)
        assert len(result) == 1
        assert result[0].combined_score == pytest.approx(1.0)
