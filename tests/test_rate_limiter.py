"""Tests for logdrift.rate_limiter."""

import pytest

from logdrift.rate_limiter import RateLimiter, RateLimiterConfig, RateLimiterError


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestRateLimiterConfig:
    def test_empty_field_raises(self):
        with pytest.raises(RateLimiterError, match="field"):
            RateLimiterConfig(field="")

    def test_non_positive_window_raises(self):
        with pytest.raises(RateLimiterError, match="window_seconds"):
            RateLimiterConfig(field="level", window_seconds=0)

    def test_zero_max_events_raises(self):
        with pytest.raises(RateLimiterError, match="max_events"):
            RateLimiterConfig(field="level", max_events=0)

    def test_valid_config_created(self):
        cfg = RateLimiterConfig(field="level", window_seconds=30.0, max_events=5)
        assert cfg.field == "level"
        assert cfg.window_seconds == 30.0
        assert cfg.max_events == 5


# ---------------------------------------------------------------------------
# RateLimiter behaviour
# ---------------------------------------------------------------------------

class TestRateLimiter:
    def _make(self, max_events: int = 3, window: float = 60.0) -> RateLimiter:
        return RateLimiter(RateLimiterConfig(field="level", window_seconds=window, max_events=max_events))

    def _record(self, value: str = "error") -> dict:
        return {"level": value, "msg": "test"}

    def test_below_limit_returns_false(self):
        rl = self._make(max_events=3)
        for i in range(3):
            exceeded = rl.observe(self._record(), _now=float(i))
        assert not exceeded

    def test_exceeds_limit_returns_true(self):
        rl = self._make(max_events=3)
        for i in range(3):
            rl.observe(self._record(), _now=float(i))
        assert rl.observe(self._record(), _now=3.0)

    def test_old_events_slide_out(self):
        rl = self._make(max_events=3, window=10.0)
        for i in range(3):
            rl.observe(self._record(), _now=float(i))
        # advance time beyond window — old events should be purged
        assert not rl.observe(self._record(), _now=100.0)

    def test_different_values_tracked_independently(self):
        rl = self._make(max_events=2)
        rl.observe({"level": "error"}, _now=1.0)
        rl.observe({"level": "error"}, _now=2.0)
        # error is at limit; info should not be
        assert not rl.observe({"level": "info"}, _now=3.0)

    def test_current_count_reflects_window(self):
        rl = self._make(max_events=10, window=10.0)
        for i in range(5):
            rl.observe(self._record(), _now=float(i))
        assert rl.current_count("error", _now=5.0) == 5
        # after sliding window, old events gone
        assert rl.current_count("error", _now=20.0) == 0

    def test_reset_specific_value(self):
        rl = self._make(max_events=10)
        rl.observe(self._record("error"), _now=1.0)
        rl.observe(self._record("warn"), _now=1.0)
        rl.reset("error")
        assert rl.current_count("error", _now=2.0) == 0
        assert rl.current_count("warn", _now=2.0) == 1

    def test_reset_all(self):
        rl = self._make(max_events=10)
        rl.observe(self._record("error"), _now=1.0)
        rl.observe(self._record("warn"), _now=1.0)
        rl.reset()
        assert rl.current_count("error", _now=2.0) == 0
        assert rl.current_count("warn", _now=2.0) == 0

    def test_missing_field_uses_empty_string_key(self):
        rl = self._make(max_events=2)
        rl.observe({"msg": "no level"}, _now=1.0)
        assert rl.current_count("", _now=2.0) == 1
