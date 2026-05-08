"""Tests for logdrift.pattern_counter."""
import pytest
from logdrift.pattern_counter import (
    PatternCounter,
    PatternCounterConfig,
    PatternCounterError,
)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestPatternCounterConfig:
    def test_valid_config_created(self):
        cfg = PatternCounterConfig(fields=["level", "service"])
        assert cfg.window_seconds == 60.0
        assert cfg.min_count == 2

    def test_empty_fields_raises(self):
        with pytest.raises(PatternCounterError, match="fields"):
            PatternCounterConfig(fields=[])

    def test_non_positive_window_raises(self):
        with pytest.raises(PatternCounterError, match="window_seconds"):
            PatternCounterConfig(fields=["level"], window_seconds=0)

    def test_min_count_below_one_raises(self):
        with pytest.raises(PatternCounterError, match="min_count"):
            PatternCounterConfig(fields=["level"], min_count=0)


# ---------------------------------------------------------------------------
# Behaviour
# ---------------------------------------------------------------------------

def _counter(min_count: int = 3, window: float = 60.0) -> PatternCounter:
    cfg = PatternCounterConfig(fields=["level", "service"], min_count=min_count, window_seconds=window)
    return PatternCounter(cfg)


class TestPatternCounter:
    def test_first_observation_is_rare(self):
        c = _counter(min_count=2)
        hit = c.observe({"level": "ERROR", "service": "auth"})
        assert hit.count == 1
        assert hit.is_rare is True

    def test_reaches_min_count_not_rare(self):
        c = _counter(min_count=2)
        c.observe({"level": "ERROR", "service": "auth"}, now=0.0)
        hit = c.observe({"level": "ERROR", "service": "auth"}, now=1.0)
        assert hit.count == 2
        assert hit.is_rare is False

    def test_different_patterns_tracked_independently(self):
        c = _counter(min_count=2)
        c.observe({"level": "ERROR", "service": "auth"}, now=0.0)
        hit = c.observe({"level": "WARN", "service": "auth"}, now=1.0)
        assert hit.count == 1
        assert hit.is_rare is True

    def test_old_observations_expire(self):
        c = _counter(min_count=2, window=5.0)
        c.observe({"level": "ERROR", "service": "auth"}, now=0.0)
        # second observation is outside the window
        hit = c.observe({"level": "ERROR", "service": "auth"}, now=10.0)
        assert hit.count == 1
        assert hit.is_rare is True

    def test_counts_returns_active_patterns(self):
        c = _counter(min_count=2)
        c.observe({"level": "INFO", "service": "web"}, now=0.0)
        c.observe({"level": "INFO", "service": "web"}, now=1.0)
        counts = c.counts()
        assert counts[("INFO", "web")] == 2

    def test_rare_patterns_filters_correctly(self):
        c = _counter(min_count=3)
        c.observe({"level": "ERROR", "service": "db"}, now=0.0)
        c.observe({"level": "INFO", "service": "web"}, now=0.1)
        c.observe({"level": "INFO", "service": "web"}, now=0.2)
        c.observe({"level": "INFO", "service": "web"}, now=0.3)
        rare = c.rare_patterns()
        patterns = [p for p, _ in rare]
        assert ("ERROR", "db") in patterns
        assert ("INFO", "web") not in patterns

    def test_missing_field_uses_empty_string(self):
        c = _counter(min_count=2)
        hit = c.observe({"level": "DEBUG"})  # 'service' missing
        assert hit.pattern == ("DEBUG", "")
