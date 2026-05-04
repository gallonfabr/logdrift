"""Tests for logdrift.aggregator."""

import pytest

from logdrift.aggregator import Aggregator, WindowStats
from logdrift.detector import AnomalyEvent


def _event(field: str = "status", value: object = "500", score: float = 0.9) -> AnomalyEvent:
    return AnomalyEvent(field=field, value=value, score=score, frequency=0.01)


class TestAggregator:
    def test_invalid_window_raises(self):
        with pytest.raises(ValueError):
            Aggregator(window_seconds=0)

    def test_empty_stats(self):
        agg = Aggregator(window_seconds=60)
        assert agg.stats() == {}
        assert agg.total() == 0

    def test_add_and_retrieve(self):
        agg = Aggregator(window_seconds=60)
        agg.add(_event(), ts=0.0)
        stats = agg.stats(now=1.0)
        assert "status" in stats
        assert stats["status"].count == 1
        assert stats["status"].max_score == pytest.approx(0.9)

    def test_events_expire_after_window(self):
        agg = Aggregator(window_seconds=10)
        agg.add(_event(), ts=0.0)
        agg.add(_event(), ts=5.0)
        # At t=11 the first event should be pruned
        assert agg.total(now=11.0) == 1
        # At t=16 both should be pruned
        assert agg.total(now=16.0) == 0

    def test_multiple_fields(self):
        agg = Aggregator(window_seconds=60)
        agg.add(_event(field="status", value="500", score=0.8), ts=0.0)
        agg.add(_event(field="level", value="ERROR", score=0.95), ts=1.0)
        stats = agg.stats(now=2.0)
        assert set(stats.keys()) == {"status", "level"}
        assert stats["level"].max_score == pytest.approx(0.95)

    def test_max_score_tracks_highest(self):
        agg = Aggregator(window_seconds=60)
        agg.add(_event(score=0.5), ts=0.0)
        agg.add(_event(score=0.99), ts=1.0)
        agg.add(_event(score=0.7), ts=2.0)
        stats = agg.stats(now=3.0)
        assert stats["status"].max_score == pytest.approx(0.99)
        assert stats["status"].count == 3

    def test_values_list_populated(self):
        agg = Aggregator(window_seconds=60)
        agg.add(_event(value="404"), ts=0.0)
        agg.add(_event(value="500"), ts=1.0)
        stats = agg.stats(now=2.0)
        assert "404" in stats["status"].values
        assert "500" in stats["status"].values
