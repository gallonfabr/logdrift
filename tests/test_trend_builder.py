"""Unit tests for logdrift.trend_builder."""
import pytest
from logdrift.trend import TrendError
from logdrift.trend_builder import (
    anomalies_for_records,
    build_trend,
    build_trends,
    observe_record,
)


class TestBuildTrend:
    def test_returns_trend_instance(self):
        from logdrift.trend import Trend
        t = build_trend("status", window_seconds=30.0)
        assert isinstance(t, Trend)

    def test_invalid_config_propagates(self):
        with pytest.raises(TrendError):
            build_trend("")


class TestBuildTrends:
    def test_builds_multiple(self):
        specs = [
            {"field": "status", "window_seconds": 20.0},
            {"field": "level", "spike_factor": 3.0},
        ]
        trends = build_trends(specs)
        assert len(trends) == 2
        assert trends[0]._cfg.field == "status"
        assert trends[1]._cfg.field == "level"

    def test_empty_list_returns_empty(self):
        assert build_trends([]) == []


class TestObserveRecord:
    def _trends(self):
        return build_trends([{"field": "status", "window_seconds": 10.0, "spike_factor": 2.0}])

    def test_missing_field_skipped(self):
        trends = self._trends()
        result = observe_record(trends, {"other": "x"}, 0.0)
        assert result == []

    def test_present_field_observed(self):
        trends = self._trends()
        # single observation — no anomaly yet
        result = observe_record(trends, {"status": "200"}, 0.0)
        assert result == []

    def test_spike_triggers_anomaly(self):
        trends = self._trends()
        # seed old half
        observe_record(trends, {"status": "500"}, 0.0)
        # flood new half
        for ts in [6.0, 6.5, 7.0, 7.5, 8.0, 8.5]:
            observe_record(trends, {"status": "500"}, ts)
        result = observe_record(trends, {"status": "500"}, 9.9)
        assert len(result) == 1
        assert result[0].value == "500"


class TestAnomaliesForRecords:
    def test_processes_iterable(self):
        trends = build_trends([{"field": "level", "window_seconds": 10.0}])
        pairs = [(float(i), {"level": "ERROR"}) for i in range(5)]
        results = anomalies_for_records(trends, pairs)
        # no spike — just checking it doesn't crash
        assert isinstance(results, list)

    def test_empty_records_returns_empty(self):
        trends = build_trends([{"field": "level"}])
        assert anomalies_for_records(trends, []) == []
