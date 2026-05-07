"""Tests for logdrift.replay."""

from __future__ import annotations

import pytest

from logdrift.detector import Detector
from logdrift.replay import ReplayConfig, ReplayError, ReplayResult, replay


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_detector(field: str = "status", min_samples: int = 3) -> Detector:
    return Detector(fields=[field], min_samples=min_samples)


def _records(n: int, status: str = "ok") -> list[dict]:
    return [{"status": status, "ts": float(i)} for i in range(n)]


# ---------------------------------------------------------------------------
# ReplayConfig
# ---------------------------------------------------------------------------

class TestReplayConfig:
    def test_defaults(self):
        cfg = ReplayConfig()
        assert cfg.speed_factor == 1.0
        assert cfg.timestamp_field == "ts"
        assert cfg.max_records is None

    def test_negative_speed_raises(self):
        with pytest.raises(ReplayError, match="speed_factor"):
            ReplayConfig(speed_factor=-0.1)

    def test_empty_timestamp_field_raises(self):
        with pytest.raises(ReplayError, match="timestamp_field"):
            ReplayConfig(timestamp_field="")

    def test_zero_max_records_raises(self):
        with pytest.raises(ReplayError, match="max_records"):
            ReplayConfig(max_records=0)

    def test_valid_custom_config(self):
        cfg = ReplayConfig(speed_factor=0, max_records=100)
        assert cfg.speed_factor == 0
        assert cfg.max_records == 100


# ---------------------------------------------------------------------------
# replay()
# ---------------------------------------------------------------------------

class TestReplay:
    def test_records_processed_count(self):
        det = _make_detector()
        result = replay(_records(5), [det], config=ReplayConfig(speed_factor=0))
        assert result.records_processed == 5

    def test_no_anomaly_before_min_samples(self):
        det = _make_detector(min_samples=10)
        result = replay(_records(5), [det], config=ReplayConfig(speed_factor=0))
        assert result.anomalies_found == 0
        assert result.events == []

    def test_anomaly_detected_for_rare_value(self):
        det = _make_detector(min_samples=3)
        common = _records(20, status="ok")
        rare = [{"status": "CRITICAL", "ts": 20.0}]
        result = replay(
            common + rare,
            [det],
            config=ReplayConfig(speed_factor=0),
        )
        assert result.anomalies_found >= 1

    def test_on_anomaly_callback_invoked(self):
        det = _make_detector(min_samples=3)
        common = _records(20, status="ok")
        rare = [{"status": "CRITICAL", "ts": 20.0}]
        collected = []
        replay(
            common + rare,
            [det],
            config=ReplayConfig(speed_factor=0),
            on_anomaly=collected.append,
        )
        assert len(collected) >= 1

    def test_max_records_limits_processing(self):
        det = _make_detector()
        cfg = ReplayConfig(speed_factor=0, max_records=3)
        result = replay(_records(10), [det], config=cfg)
        assert result.records_processed == 3

    def test_empty_records_returns_zero_counts(self):
        det = _make_detector()
        result = replay([], [det], config=ReplayConfig(speed_factor=0))
        assert result.records_processed == 0
        assert result.anomalies_found == 0

    def test_result_repr_contains_counts(self):
        r = ReplayResult(records_processed=7, anomalies_found=2)
        text = repr(r)
        assert "7" in text
        assert "2" in text
