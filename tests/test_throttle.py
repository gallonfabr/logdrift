"""Tests for logdrift.throttle."""
from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from logdrift.throttle import Throttle, ThrottleConfig


class TestThrottleConfig:
    def test_default_values(self):
        cfg = ThrottleConfig()
        assert cfg.window_seconds == 60.0
        assert cfg.max_alerts == 3

    def test_invalid_window_raises(self):
        with pytest.raises(ValueError, match="window_seconds"):
            ThrottleConfig(window_seconds=0)

    def test_invalid_max_alerts_raises(self):
        with pytest.raises(ValueError, match="max_alerts"):
            ThrottleConfig(max_alerts=0)


class TestThrottle:
    def _throttle(self, window: float = 60.0, max_alerts: int = 3) -> Throttle:
        return Throttle(ThrottleConfig(window_seconds=window, max_alerts=max_alerts))

    def test_first_alert_allowed(self):
        t = self._throttle()
        assert t.allow("status", "500") is True

    def test_allows_up_to_max(self):
        t = self._throttle(max_alerts=3)
        for _ in range(3):
            assert t.allow("status", "500") is True

    def test_blocks_after_max(self):
        t = self._throttle(max_alerts=2)
        t.allow("status", "500")
        t.allow("status", "500")
        assert t.allow("status", "500") is False

    def test_different_keys_are_independent(self):
        t = self._throttle(max_alerts=1)
        assert t.allow("status", "500") is True
        assert t.allow("status", "404") is True
        assert t.allow("level", "error") is True

    def test_window_expiry_re_allows(self):
        t = self._throttle(window=1.0, max_alerts=1)
        t.allow("status", "500")
        assert t.allow("status", "500") is False
        # Simulate time passing beyond the window
        with patch("logdrift.throttle.time.monotonic", return_value=time.monotonic() + 2.0):
            assert t.allow("status", "500") is True

    def test_reset_specific_key(self):
        t = self._throttle(max_alerts=1)
        t.allow("status", "500")
        assert t.allow("status", "500") is False
        t.reset("status", "500")
        assert t.allow("status", "500") is True

    def test_reset_all_clears_history(self):
        t = self._throttle(max_alerts=1)
        t.allow("status", "500")
        t.allow("level", "error")
        t.reset_all()
        assert t.allow("status", "500") is True
        assert t.allow("level", "error") is True
