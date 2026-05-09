"""Tests for logdrift.burst_detector and logdrift.burst_builder."""
from __future__ import annotations

import pytest

from logdrift.burst_detector import BurstAlert, BurstConfig, BurstDetector, BurstError
from logdrift.burst_builder import build_detector, build_detectors, observe_all


# ---------------------------------------------------------------------------
# BurstConfig validation
# ---------------------------------------------------------------------------

class TestBurstConfig:
    def test_valid_config_created(self):
        cfg = BurstConfig(window_seconds=30.0, multiplier=2.0, min_baseline_periods=2)
        assert cfg.window_seconds == 30.0

    def test_non_positive_window_raises(self):
        with pytest.raises(BurstError, match="window_seconds"):
            BurstConfig(window_seconds=0)

    def test_negative_cooldown_raises(self):
        with pytest.raises(BurstError, match="cooldown_seconds"):
            BurstConfig(cooldown_seconds=-1)

    def test_multiplier_lte_one_raises(self):
        with pytest.raises(BurstError, match="multiplier"):
            BurstConfig(multiplier=1.0)

    def test_min_baseline_periods_below_one_raises(self):
        with pytest.raises(BurstError, match="min_baseline_periods"):
            BurstConfig(min_baseline_periods=0)


# ---------------------------------------------------------------------------
# BurstDetector construction
# ---------------------------------------------------------------------------

class TestBurstDetectorInit:
    def test_empty_field_raises(self):
        with pytest.raises(BurstError, match="field"):
            BurstDetector(field="", value="error")

    def test_empty_value_raises(self):
        with pytest.raises(BurstError, match="value"):
            BurstDetector(field="level", value="")

    def test_default_config_applied(self):
        det = BurstDetector(field="level", value="error")
        assert det.config.multiplier == 3.0


# ---------------------------------------------------------------------------
# BurstDetector behaviour
# ---------------------------------------------------------------------------

def _make_det(min_baseline_periods: int = 2, multiplier: float = 2.0) -> BurstDetector:
    cfg = BurstConfig(
        window_seconds=10.0,
        cooldown_seconds=0.0,
        multiplier=multiplier,
        min_baseline_periods=min_baseline_periods,
    )
    return BurstDetector(field="level", value="error", config=cfg)


class TestBurstDetectorObserve:
    def test_no_alert_before_min_baseline_periods(self):
        det = _make_det(min_baseline_periods=3)
        rec = {"level": "error"}
        # Feed two complete windows — still one short of min_baseline_periods
        for i in range(2):
            for _ in range(5):
                det.observe(rec, now=float(i * 10 + 1))
            det.observe(rec, now=float((i + 1) * 10))  # trigger bucket rollover
        alert = det.observe(rec, now=20.5)
        assert alert is None

    def test_alert_on_burst(self):
        det = _make_det(min_baseline_periods=2, multiplier=2.0)
        rec = {"level": "error"}
        # Window 0–10: 1 event  → rate ≈ 0.1
        det.observe(rec, now=1.0)
        det.observe({}, now=10.0)  # rollover bucket 1
        # Window 10–20: 1 event → rate ≈ 0.1
        det.observe(rec, now=11.0)
        det.observe({}, now=20.0)  # rollover bucket 2  (baseline now available)
        # Burst: 100 events in window 20–20.5
        alert = None
        for _ in range(100):
            alert = det.observe(rec, now=20.1)
        assert isinstance(alert, BurstAlert)
        assert alert.field == "level"
        assert alert.value == "error"
        assert alert.multiplier_observed > 2.0

    def test_cooldown_suppresses_repeated_alerts(self):
        cfg = BurstConfig(window_seconds=10.0, cooldown_seconds=60.0, multiplier=2.0, min_baseline_periods=2)
        det = BurstDetector("level", "error", config=cfg)
        rec = {"level": "error"}
        det.observe(rec, now=1.0)
        det.observe({}, now=10.0)
        det.observe(rec, now=11.0)
        det.observe({}, now=20.0)
        first = None
        for _ in range(100):
            first = det.observe(rec, now=20.1)
        second = det.observe(rec, now=21.0)  # within cooldown
        assert isinstance(first, BurstAlert)
        assert second is None


# ---------------------------------------------------------------------------
# burst_builder helpers
# ---------------------------------------------------------------------------

class TestBurstBuilder:
    def test_build_detector_returns_instance(self):
        det = build_detector("status", "500", window_seconds=30.0)
        assert isinstance(det, BurstDetector)
        assert det.config.window_seconds == 30.0

    def test_build_detectors_multiple(self):
        specs = [
            {"field": "level", "value": "error"},
            {"field": "status", "value": "500", "multiplier": 4.0},
        ]
        dets = build_detectors(specs)
        assert len(dets) == 2
        assert dets[1].config.multiplier == 4.0

    def test_observe_all_returns_list(self):
        dets = build_detectors([
            {"field": "level", "value": "error"},
            {"field": "status", "value": "200"},
        ])
        alerts = observe_all(dets, {"level": "error", "status": "200"}, now=1.0)
        assert isinstance(alerts, list)
