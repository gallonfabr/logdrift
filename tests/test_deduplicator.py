"""Tests for logdrift.deduplicator."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from logdrift.deduplicator import Deduplicator, DeduplicatorConfig
from logdrift.detector import AnomalyEvent


def _event(field: str = "status", value: str = "500", score: float = 0.9) -> AnomalyEvent:
    return AnomalyEvent(field=field, value=value, score=score, count=1, total=100)


# ---------------------------------------------------------------------------
# DeduplicatorConfig
# ---------------------------------------------------------------------------

class TestDeduplicatorConfig:
    def test_default_values(self):
        cfg = DeduplicatorConfig()
        assert cfg.window_seconds == 60.0
        assert "field" in cfg.key_fields

    def test_invalid_window_raises(self):
        with pytest.raises(ValueError, match="window_seconds"):
            DeduplicatorConfig(window_seconds=0)

    def test_empty_key_fields_raises(self):
        with pytest.raises(ValueError, match="key_fields"):
            DeduplicatorConfig(key_fields=())


# ---------------------------------------------------------------------------
# Deduplicator
# ---------------------------------------------------------------------------

class TestDeduplicator:
    def test_first_occurrence_not_duplicate(self):
        d = Deduplicator()
        assert d.is_duplicate(_event()) is False

    def test_second_occurrence_is_duplicate(self):
        d = Deduplicator()
        e = _event()
        d.is_duplicate(e)
        assert d.is_duplicate(e) is True

    def test_different_value_not_duplicate(self):
        d = Deduplicator()
        d.is_duplicate(_event(value="500"))
        assert d.is_duplicate(_event(value="404")) is False

    def test_different_field_not_duplicate(self):
        d = Deduplicator()
        d.is_duplicate(_event(field="status"))
        assert d.is_duplicate(_event(field="method")) is False

    def test_reset_clears_state(self):
        d = Deduplicator()
        e = _event()
        d.is_duplicate(e)
        d.reset()
        assert d.is_duplicate(e) is False

    def test_len_reflects_tracked_events(self):
        d = Deduplicator()
        d.is_duplicate(_event(value="500"))
        d.is_duplicate(_event(value="404"))
        assert len(d) == 2

    def test_expired_events_are_purged(self):
        cfg = DeduplicatorConfig(window_seconds=1.0)
        d = Deduplicator(cfg)
        e = _event()
        with patch("logdrift.deduplicator.time.monotonic", return_value=0.0):
            d.is_duplicate(e)
        # Advance time past the window
        with patch("logdrift.deduplicator.time.monotonic", return_value=2.0):
            assert d.is_duplicate(e) is False

    def test_len_decreases_after_reset(self):
        """len(d) should return 0 after reset, even if events were tracked."""
        d = Deduplicator()
        d.is_duplicate(_event(value="500"))
        d.is_duplicate(_event(value="404"))
        assert len(d) == 2
        d.reset()
        assert len(d) == 0

    def test_expired_event_not_counted_in_len(self):
        """Expired events should not contribute to len after they are purged."""
        cfg = DeduplicatorConfig(window_seconds=1.0)
        d = Deduplicator(cfg)
        with patch("logdrift.deduplicator.time.monotonic", return_value=0.0):
            d.is_duplicate(_event(value="500"))
            d.is_duplicate(_event(value="404"))
        # Trigger purge by checking a new event after the window expires
        with patch("logdrift.deduplicator.time.monotonic", return_value=2.0):
            d.is_duplicate(_event(value="503"))
            assert len(d) == 1
