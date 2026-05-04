"""Snapshot-style tests for Report.to_text() formatting."""

from logdrift.aggregator import Aggregator
from logdrift.detector import AnomalyEvent
from logdrift.report import build_report


def _add(agg, field, value, score, ts):
    agg.add(AnomalyEvent(field=field, value=value, score=score, frequency=0.01), ts=ts)


def test_header_line_present():
    agg = Aggregator(window_seconds=30)
    text = build_report(agg).to_text()
    assert "Anomaly Report" in text
    assert "window=30" in text


def test_fields_sorted_by_score_descending():
    agg = Aggregator(window_seconds=60)
    _add(agg, "low", "v", 0.5, 0.0)
    _add(agg, "high", "v", 0.99, 1.0)
    text = build_report(agg).to_text()
    assert text.index("high") < text.index("low")


def test_separator_line_present():
    agg = Aggregator(window_seconds=60)
    text = build_report(agg).to_text()
    assert "---" in text


def test_multiple_values_shown():
    agg = Aggregator(window_seconds=60)
    for v in ["404", "500", "503"]:
        _add(agg, "status", v, 0.8, 0.0)
    text = build_report(agg).to_text()
    assert "404" in text
    assert "500" in text
