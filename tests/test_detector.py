"""Tests for logdrift.detector."""

import pytest
from logdrift.detector import AnomalyEvent, Detector


DEFAULT_RECORD = {"level": "INFO", "service": "api", "status": "200"}


def _warm_up(detector: Detector, record: dict, n: int) -> None:
    """Feed *record* into *detector* exactly *n* times to build up baseline statistics."""
    for _ in range(n):
        detector.feed(record)


class TestDetector:
    def test_no_anomaly_before_min_samples(self):
        det = Detector(min_samples=5)
        for _ in range(4):
            events = det.feed(DEFAULT_RECORD)
            assert events == []

    def test_no_anomaly_for_common_value(self):
        det = Detector(min_samples=5, alert_threshold=0.05)
        _warm_up(det, DEFAULT_RECORD, 10)
        events = det.feed(DEFAULT_RECORD)
        assert events == []

    def test_anomaly_for_rare_value(self):
        det = Detector(min_samples=5, alert_threshold=0.05)
        _warm_up(det, DEFAULT_RECORD, 20)
        rare = {"level": "INFO", "service": "api", "status": "503"}
        events = det.feed(rare)
        assert len(events) == 1
        assert events[0].field == "status"
        assert "503" in events[0].reason

    def test_anomaly_for_unseen_field(self):
        det = Detector(min_samples=3, alert_threshold=0.05)
        _warm_up(det, DEFAULT_RECORD, 5)
        record_with_new_field = {**DEFAULT_RECORD, "host": "web-01"}
        events = det.feed(record_with_new_field)
        fields = [e.field for e in events]
        assert "host" in fields

    def test_scan_yields_anomaly_events(self):
        det = Detector(min_samples=5, alert_threshold=0.05)
        records = [DEFAULT_RECORD] * 20 + [{"level": "DEBUG", "service": "api", "status": "500"}]
        events = list(det.scan(iter(records)))
        assert any(e.field == "level" for e in events)

    def test_anomaly_event_repr(self):
        event = AnomalyEvent({}, "test reason", field="level")
        assert "level" in repr(event)
        assert "test reason" in repr(event)

    def test_multiple_anomalous_fields_reported(self):
        """A record with several rare values should produce one event per anomalous field."""
        det = Detector(min_samples=5, alert_threshold=0.05)
        _warm_up(det, DEFAULT_RECORD, 20)
        rare = {"level": "CRITICAL", "service": "worker", "status": "503"}
        events = det.feed(rare)
        anomalous_fields = {e.field for e in events}
        # All three field values are new/rare, so each should trigger an event.
        assert anomalous_fields == {"level", "service", "status"}
