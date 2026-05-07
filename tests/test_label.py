"""Tests for logdrift.label."""

from __future__ import annotations

import pytest

from logdrift.label import (
    LabelError,
    LabelRule,
    Labeller,
    build_default_labeller,
)
from logdrift.scorer import ScoredEvent
from logdrift.detector import AnomalyEvent


def _scored(score: float) -> ScoredEvent:
    ae = AnomalyEvent(field="status", value="500", score=score)
    return ScoredEvent(event=ae, score=score, breakdown={"anomaly": score})


# ---------------------------------------------------------------------------
# LabelRule
# ---------------------------------------------------------------------------

class TestLabelRule:
    def test_valid_rule_created(self):
        rule = LabelRule(label="high", min_score=0.7, max_score=0.9)
        assert rule.label == "high"

    def test_empty_label_raises(self):
        with pytest.raises(LabelError, match="non-empty"):
            LabelRule(label="", min_score=0.0, max_score=0.5)

    def test_min_gte_max_raises(self):
        with pytest.raises(LabelError, match="less than"):
            LabelRule(label="x", min_score=0.5, max_score=0.5)

    def test_negative_score_raises(self):
        with pytest.raises(LabelError, match="non-negative"):
            LabelRule(label="x", min_score=-0.1, max_score=0.5)

    def test_matches_inside_range(self):
        rule = LabelRule(label="medium", min_score=0.4, max_score=0.7)
        assert rule.matches(0.55)

    def test_no_match_outside_range(self):
        rule = LabelRule(label="medium", min_score=0.4, max_score=0.7)
        assert not rule.matches(0.8)

    def test_inclusive_lower_bound(self):
        rule = LabelRule(label="low", min_score=0.0, max_score=0.4)
        assert rule.matches(0.0)

    def test_exclusive_upper_bound(self):
        rule = LabelRule(label="low", min_score=0.0, max_score=0.4)
        assert not rule.matches(0.4)


# ---------------------------------------------------------------------------
# Labeller
# ---------------------------------------------------------------------------

class TestLabeller:
    def test_default_label_when_no_rules(self):
        lb = Labeller(default_label="none")
        _, lbl = lb.label(_scored(0.5))
        assert lbl == "none"

    def test_correct_rule_applied(self):
        lb = build_default_labeller()
        _, lbl = lb.label(_scored(0.75))
        assert lbl == "high"

    def test_low_label(self):
        lb = build_default_labeller()
        _, lbl = lb.label(_scored(0.1))
        assert lbl == "low"

    def test_critical_label(self):
        lb = build_default_labeller()
        _, lbl = lb.label(_scored(0.95))
        assert lbl == "critical"

    def test_label_all_returns_correct_count(self):
        lb = build_default_labeller()
        events = [_scored(s) for s in (0.1, 0.5, 0.8, 0.95)]
        results = lb.label_all(events)
        assert len(results) == 4

    def test_label_all_labels_match(self):
        lb = build_default_labeller()
        events = [_scored(s) for s in (0.1, 0.5, 0.8, 0.95)]
        labels = [lbl for _, lbl in lb.label_all(events)]
        assert labels == ["low", "medium", "high", "critical"]
