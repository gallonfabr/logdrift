"""Tests for logdrift.field_tracker."""
import pytest

from logdrift.field_tracker import (
    FieldDriftAnomaly,
    FieldTracker,
    FieldTrackerConfig,
    FieldTrackerError,
)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestFieldTrackerConfig:
    def test_default_window(self):
        cfg = FieldTrackerConfig()
        assert cfg.window_seconds == 300.0

    def test_non_positive_window_raises(self):
        with pytest.raises(FieldTrackerError, match="window_seconds"):
            FieldTrackerConfig(window_seconds=0)

    def test_negative_window_raises(self):
        with pytest.raises(FieldTrackerError):
            FieldTrackerConfig(window_seconds=-1.0)

    def test_empty_expected_fields_raises(self):
        with pytest.raises(FieldTrackerError, match="expected_fields"):
            FieldTrackerConfig(expected_fields=set())

    def test_valid_expected_fields_accepted(self):
        cfg = FieldTrackerConfig(expected_fields={"level", "msg"})
        assert "level" in cfg.expected_fields


# ---------------------------------------------------------------------------
# Core behaviour
# ---------------------------------------------------------------------------

class TestFieldTracker:
    def _tracker(self, window=60.0, expected=None):
        return FieldTracker(FieldTrackerConfig(window_seconds=window, expected_fields=expected))

    def test_active_fields_empty_initially(self):
        t = self._tracker()
        assert t.active_fields(ts=0.0) == set()

    def test_observe_adds_fields(self):
        t = self._tracker()
        t.observe({"level": "info", "msg": "hello"}, ts=100.0)
        assert t.active_fields(ts=100.0) == {"level", "msg"}

    def test_old_events_purged(self):
        t = self._tracker(window=10.0)
        t.observe({"level": "info"}, ts=0.0)
        # advance past the window
        assert t.active_fields(ts=11.0) == set()

    def test_recent_events_retained(self):
        t = self._tracker(window=10.0)
        t.observe({"level": "info"}, ts=0.0)
        t.observe({"msg": "hi"}, ts=5.0)
        # at ts=9 the first event is still within window
        assert "level" in t.active_fields(ts=9.0)

    def test_appearance_returns_none_for_unknown_field(self):
        t = self._tracker()
        assert t.appearance("missing", ts=0.0) is None

    def test_appearance_returns_correct_count(self):
        t = self._tracker()
        t.observe({"level": "info"}, ts=1.0)
        t.observe({"level": "warn"}, ts=2.0)
        ap = t.appearance("level", ts=2.0)
        assert ap is not None
        assert ap.count == 2
        assert ap.first_seen == pytest.approx(1.0)
        assert ap.last_seen == pytest.approx(2.0)

    def test_no_anomalies_when_no_expected_fields(self):
        t = self._tracker()
        result = t.anomalies({"level": "info"}, ts=1.0)
        assert result == []

    def test_missing_field_anomaly(self):
        t = self._tracker(expected={"level", "msg", "ts"})
        result = t.anomalies({"level": "info"}, ts=1.0)
        kinds = {a.kind for a in result}
        fields = {a.field_name for a in result}
        assert "missing" in kinds
        assert {"msg", "ts"} == fields

    def test_unexpected_field_anomaly(self):
        t = self._tracker(expected={"level"})
        result = t.anomalies({"level": "info", "secret": "x"}, ts=1.0)
        unexpected = [a for a in result if a.kind == "unexpected"]
        assert len(unexpected) == 1
        assert unexpected[0].field_name == "secret"

    def test_no_anomaly_when_record_matches_expected(self):
        t = self._tracker(expected={"level", "msg"})
        result = t.anomalies({"level": "info", "msg": "ok"}, ts=1.0)
        assert result == []

    def test_anomaly_has_timestamp(self):
        t = self._tracker(expected={"level"})
        result = t.anomalies({}, ts=42.0)
        assert all(isinstance(a, FieldDriftAnomaly) for a in result)
        assert all(a.ts == pytest.approx(42.0) for a in result)
