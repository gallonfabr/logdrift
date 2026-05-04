"""Tests for logdrift.report."""

import json

import pytest

from logdrift.aggregator import Aggregator
from logdrift.detector import AnomalyEvent
from logdrift.report import FieldReport, Report, build_report, _top_values


def _add(agg: Aggregator, field: str, value: str, score: float, ts: float) -> None:
    agg.add(AnomalyEvent(field=field, value=value, score=score, frequency=0.01), ts=ts)


class TestBuildReport:
    def test_empty_aggregator(self):
        agg = Aggregator(window_seconds=30)
        report = build_report(agg)
        assert report.total_anomalies == 0
        assert report.fields == []

    def test_single_field(self):
        agg = Aggregator(window_seconds=60)
        _add(agg, "status", "500", 0.9, 0.0)
        _add(agg, "status", "503", 0.85, 1.0)
        report = build_report(agg)
        assert report.total_anomalies == 2
        assert len(report.fields) == 1
        fr = report.fields[0]
        assert fr.field == "status"
        assert fr.anomaly_count == 2
        assert fr.max_score == pytest.approx(0.9)

    def test_window_seconds_preserved(self):
        agg = Aggregator(window_seconds=120)
        report = build_report(agg)
        assert report.window_seconds == 120

    def test_to_json_valid(self):
        agg = Aggregator(window_seconds=60)
        _add(agg, "level", "ERROR", 0.95, 0.0)
        report = build_report(agg)
        parsed = json.loads(report.to_json())
        assert parsed["total_anomalies"] == 1
        assert parsed["fields"][0]["field"] == "level"

    def test_to_text_contains_field(self):
        agg = Aggregator(window_seconds=60)
        _add(agg, "method", "DELETE", 0.88, 0.0)
        text = build_report(agg).to_text()
        assert "method" in text
        assert "DELETE" in text


class TestTopValues:
    def test_returns_most_frequent(self):
        values = ["a", "b", "a", "c", "a", "b"]
        assert _top_values(values, n=2) == ["a", "b"]

    def test_respects_n(self):
        values = ["x"] * 5 + ["y"] * 3 + ["z"] * 1
        assert _top_values(values, n=2) == ["x", "y"]

    def test_empty_list(self):
        assert _top_values([]) == []
