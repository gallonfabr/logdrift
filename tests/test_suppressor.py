"""Tests for logdrift.suppressor."""

from __future__ import annotations

import pytest

from logdrift.detector import AnomalyEvent
from logdrift.suppressor import Suppressor, SuppressorError


def _event(field_name: str, value: str, score: float = 0.9) -> AnomalyEvent:
    return AnomalyEvent(field_name=field_name, value=value, score=score)


class TestSuppressor:
    def test_empty_suppressor_passes_all(self):
        s = Suppressor()
        event = _event("level", "DEBUG")
        assert not s.is_suppressed(event)

    def test_suppresses_registered_value(self):
        s = Suppressor()
        s.add("level", "DEBUG")
        assert s.is_suppressed(_event("level", "DEBUG"))

    def test_does_not_suppress_other_value(self):
        s = Suppressor()
        s.add("level", "DEBUG")
        assert not s.is_suppressed(_event("level", "ERROR"))

    def test_does_not_suppress_other_field(self):
        s = Suppressor()
        s.add("level", "DEBUG")
        assert not s.is_suppressed(_event("status", "DEBUG"))

    def test_multiple_values_same_field(self):
        s = Suppressor()
        s.add("level", "DEBUG")
        s.add("level", "INFO")
        assert s.is_suppressed(_event("level", "DEBUG"))
        assert s.is_suppressed(_event("level", "INFO"))
        assert not s.is_suppressed(_event("level", "ERROR"))

    def test_remove_rule(self):
        s = Suppressor()
        s.add("level", "DEBUG")
        s.remove("level", "DEBUG")
        assert not s.is_suppressed(_event("level", "DEBUG"))

    def test_remove_nonexistent_is_noop(self):
        s = Suppressor()
        s.remove("level", "DEBUG")  # should not raise

    def test_rule_count(self):
        s = Suppressor()
        assert s.rule_count() == 0
        s.add("level", "DEBUG")
        s.add("level", "INFO")
        s.add("status", "200")
        assert s.rule_count() == 3

    def test_filter_yields_non_suppressed(self):
        s = Suppressor()
        s.add("level", "DEBUG")
        events = [
            _event("level", "DEBUG"),
            _event("level", "ERROR"),
            _event("status", "500"),
        ]
        result = list(s.filter(events))
        assert len(result) == 2
        assert all(e.value != "DEBUG" for e in result)

    def test_empty_field_name_raises(self):
        s = Suppressor()
        with pytest.raises(SuppressorError):
            s.add("", "DEBUG")

    def test_value_coerced_to_str(self):
        s = Suppressor()
        s.add("status", "200")
        assert s.is_suppressed(_event("status", "200"))
