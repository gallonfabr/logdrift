"""Tests for logdrift.cadence and logdrift.cadence_builder."""

from __future__ import annotations

import pytest

from logdrift.cadence import CadenceAnomaly, CadenceConfig, CadenceDetector, CadenceError
from logdrift.cadence_builder import (
    anomalies_for_record,
    build_detector,
    build_detectors,
    observe_all,
)


# ---------------------------------------------------------------------------
# CadenceConfig validation
# ---------------------------------------------------------------------------

class TestCadenceConfig:
    def test_valid_config_created(self):
        cfg = CadenceConfig(field="service", window=30, min_periods=3, z_threshold=2.5)
        assert cfg.field == "service"

    def test_empty_field_raises(self):
        with pytest.raises(CadenceError, match="field"):
            CadenceConfig(field="")

    def test_non_positive_window_raises(self):
        with pytest.raises(CadenceError, match="window"):
            CadenceConfig(field="x", window=0)

    def test_min_periods_below_two_raises(self):
        with pytest.raises(CadenceError, match="min_periods"):
            CadenceConfig(field="x", min_periods=1)

    def test_z_threshold_zero_raises(self):
        with pytest.raises(CadenceError, match="z_threshold"):
            CadenceConfig(field="x", z_threshold=0.0)


# ---------------------------------------------------------------------------
# CadenceDetector behaviour
# ---------------------------------------------------------------------------

def _make_detector(**kw) -> CadenceDetector:
    defaults = dict(field="svc", window=120, min_periods=3, z_threshold=3.0)
    defaults.update(kw)
    return CadenceDetector(CadenceConfig(**defaults))


class TestCadenceDetector:
    def test_no_anomaly_before_min_periods(self):
        det = _make_detector(min_periods=4)
        for i in range(4):          # 3 intervals, need 4
            result = det.observe({"svc": "auth"}, ts=float(i * 10))
        assert result is None

    def test_missing_field_returns_none(self):
        det = _make_detector()
        assert det.observe({"other": "val"}, ts=0.0) is None

    def test_stable_cadence_no_anomaly(self):
        det = _make_detector(min_periods=3, z_threshold=2.0)
        for i in range(6):
            result = det.observe({"svc": "auth"}, ts=float(i * 10))
        assert result is None

    def test_spike_triggers_anomaly(self):
        det = _make_detector(min_periods=3, z_threshold=2.0)
        # establish regular 10-second cadence
        for i in range(5):
            det.observe({"svc": "auth"}, ts=float(i * 10))
        # sudden 200-second gap
        result = det.observe({"svc": "auth"}, ts=5 * 10 + 200.0)
        assert isinstance(result, CadenceAnomaly)
        assert result.field == "svc"
        assert result.value == "auth"
        assert result.z_score >= 2.0

    def test_anomaly_repr(self):
        a = CadenceAnomaly(field="f", value="v", interval=5.0,
                           mean=1.0, stddev=0.5, z_score=8.0, ts=0.0)
        assert "CadenceAnomaly" in repr(a)


# ---------------------------------------------------------------------------
# cadence_builder helpers
# ---------------------------------------------------------------------------

class TestCadenceBuilder:
    def test_build_detector_returns_instance(self):
        det = build_detector({"field": "host"})
        assert isinstance(det, CadenceDetector)

    def test_defaults_applied(self):
        det = build_detector({"field": "host"})
        assert det.config.window == 60
        assert det.config.min_periods == 4
        assert det.config.z_threshold == 3.0

    def test_custom_values_applied(self):
        det = build_detector({"field": "host", "window": "30", "z_threshold": "2.5"})
        assert det.config.window == 30
        assert det.config.z_threshold == 2.5

    def test_invalid_config_propagates(self):
        with pytest.raises(CadenceError):
            build_detector({"field": ""})

    def test_build_multiple(self):
        dets = build_detectors([{"field": "a"}, {"field": "b"}])
        assert len(dets) == 2

    def test_observe_all_no_errors(self):
        dets = build_detectors([{"field": "svc"}, {"field": "host"}])
        observe_all(dets, {"svc": "api", "host": "h1"}, ts=0.0)

    def test_anomalies_for_record_yields_results(self):
        dets = [_make_detector(field="svc", min_periods=3, z_threshold=2.0)]
        for i in range(5):
            list(anomalies_for_record(dets, {"svc": "x"}, ts=float(i * 10)))
        results = list(anomalies_for_record(dets, {"svc": "x"}, ts=5 * 10 + 200.0))
        assert len(results) == 1
        assert isinstance(results[0], CadenceAnomaly)
