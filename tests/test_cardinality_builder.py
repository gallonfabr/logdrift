"""Tests for logdrift.cardinality_builder."""
import pytest
from logdrift.cardinality import CardinalityAnomaly, CardinalityError, CardinalityTracker
from logdrift.cardinality_builder import (
    anomalies_for_record,
    build_tracker,
    build_trackers,
    observe_all,
)


class TestBuildTracker:
    def test_returns_tracker_instance(self):
        t = build_tracker({"field": "env"})
        assert isinstance(t, CardinalityTracker)

    def test_defaults_applied(self):
        t = build_tracker({"field": "env"})
        assert t.config.max_distinct == 100
        assert t.config.window_seconds == 60.0
        assert t.config.min_samples == 5

    def test_custom_values_applied(self):
        t = build_tracker({"field": "env", "max_distinct": 10, "window_seconds": 30.0})
        assert t.config.max_distinct == 10
        assert t.config.window_seconds == 30.0

    def test_invalid_config_propagates(self):
        with pytest.raises(CardinalityError):
            build_tracker({"field": ""})

    def test_build_multiple(self):
        trackers = build_trackers([
            {"field": "host"},
            {"field": "service", "max_distinct": 20},
        ])
        assert len(trackers) == 2
        assert trackers[1].config.max_distinct == 20


class TestObserveAll:
    def test_observe_all_feeds_all_trackers(self):
        t1 = build_tracker({"field": "host", "min_samples": 1, "max_distinct": 1000})
        t2 = build_tracker({"field": "svc", "min_samples": 1, "max_distinct": 1000})
        observe_all([t1, t2], {"host": "h1", "svc": "s1"}, ts=1000.0)
        assert t1.distinct_count() == 1
        assert t2.distinct_count() == 1

    def test_anomalies_for_record_returns_list(self):
        trackers = build_trackers([
            {"field": "code", "max_distinct": 1, "min_samples": 1},
        ])
        trackers[0].observe({"code": "a"}, ts=1000.0)
        anomalies = anomalies_for_record(trackers, {"code": "b"}, ts=1001.0)
        assert len(anomalies) == 1
        assert isinstance(anomalies[0], CardinalityAnomaly)

    def test_no_anomaly_returns_empty_list(self):
        trackers = build_trackers([{"field": "code", "max_distinct": 50}])
        result = anomalies_for_record(trackers, {"code": "x"}, ts=1000.0)
        assert result == []
