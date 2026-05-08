"""Tests for logdrift.pattern_counter_builder."""
import pytest
from logdrift.pattern_counter import PatternCounter, PatternCounterError
from logdrift.pattern_counter_builder import (
    build_counter,
    build_counters,
    observe_all,
    rare_hits_for_record,
)


class TestBuildCounter:
    def test_returns_pattern_counter(self):
        c = build_counter({"fields": ["level"]})
        assert isinstance(c, PatternCounter)

    def test_defaults_applied(self):
        c = build_counter({"fields": ["level"]})
        assert c._cfg.window_seconds == 60.0
        assert c._cfg.min_count == 2

    def test_custom_values_applied(self):
        c = build_counter({"fields": ["level"], "window_seconds": 30, "min_count": 5})
        assert c._cfg.window_seconds == 30.0
        assert c._cfg.min_count == 5

    def test_invalid_config_propagates(self):
        with pytest.raises(PatternCounterError):
            build_counter({"fields": []})


class TestBuildCounters:
    def test_builds_multiple(self):
        counters = build_counters([
            {"fields": ["level"]},
            {"fields": ["service", "status"]},
        ])
        assert len(counters) == 2

    def test_empty_list_returns_empty(self):
        assert build_counters([]) == []


class TestObserveAll:
    def test_returns_one_hit_per_counter(self):
        counters = build_counters([
            {"fields": ["level"]},
            {"fields": ["service"]},
        ])
        hits = observe_all(counters, {"level": "INFO", "service": "auth"}, now=0.0)
        assert len(hits) == 2

    def test_empty_counters_returns_empty(self):
        assert observe_all([], {"level": "INFO"}) == []


class TestRareHitsForRecord:
    def test_returns_only_rare_hits(self):
        counters = build_counters([
            {"fields": ["level"], "min_count": 2},
        ])
        # first observation → rare
        hits = rare_hits_for_record(counters, {"level": "ERROR"}, now=0.0)
        assert len(hits) == 1
        assert hits[0].is_rare is True

    def test_common_hit_excluded(self):
        counters = build_counters([
            {"fields": ["level"], "min_count": 2},
        ])
        observe_all(counters, {"level": "INFO"}, now=0.0)
        observe_all(counters, {"level": "INFO"}, now=0.1)
        hits = rare_hits_for_record(counters, {"level": "INFO"}, now=0.2)
        assert hits == []
