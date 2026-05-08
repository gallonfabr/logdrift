"""Tests for logdrift.watchdog."""
import time
import pytest

from logdrift.watchdog import Watchdog, WatchdogAlert, WatchdogConfig, WatchdogError


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestWatchdogConfig:
    def test_valid_config_created(self):
        cfg = WatchdogConfig(window_seconds=30, min_rate=1.0, max_rate=100.0, name="w")
        assert cfg.window_seconds == 30
        assert cfg.name == "w"

    def test_non_positive_window_raises(self):
        with pytest.raises(WatchdogError, match="window_seconds"):
            WatchdogConfig(window_seconds=0)

    def test_negative_min_rate_raises(self):
        with pytest.raises(WatchdogError, match="min_rate"):
            WatchdogConfig(min_rate=-1.0)

    def test_negative_max_rate_raises(self):
        with pytest.raises(WatchdogError, match="max_rate"):
            WatchdogConfig(max_rate=-0.1)

    def test_min_exceeds_max_raises(self):
        with pytest.raises(WatchdogError, match="min_rate must not exceed"):
            WatchdogConfig(min_rate=10.0, max_rate=5.0)


# ---------------------------------------------------------------------------
# Rate calculation
# ---------------------------------------------------------------------------

class TestWatchdog:
    def _wd(self, **kw) -> Watchdog:
        return Watchdog(WatchdogConfig(**kw))

    def test_empty_rate_is_zero(self):
        wd = self._wd(window_seconds=10)
        assert wd.current_rate() == 0.0

    def test_rate_counts_within_window(self):
        wd = self._wd(window_seconds=10)
        base = 1_000.0
        for i in range(5):
            wd.record(base + i)
        assert wd.current_rate(base + 9) == pytest.approx(5 / 10)

    def test_old_events_purged(self):
        wd = self._wd(window_seconds=10)
        base = 1_000.0
        wd.record(base)          # will be outside window after 10 s
        wd.record(base + 5)      # inside
        rate = wd.current_rate(base + 10.1)
        assert rate == pytest.approx(1 / 10)

    def test_no_alert_when_no_thresholds(self):
        wd = self._wd(window_seconds=10)
        wd.record(1_000.0)
        assert wd.check(1_005.0) is None

    def test_below_min_alert(self):
        wd = self._wd(window_seconds=10, min_rate=2.0)
        # only 1 event → rate = 0.1 < 2.0
        wd.record(1_000.0)
        alert = wd.check(1_005.0)
        assert isinstance(alert, WatchdogAlert)
        assert alert.kind == "below_min"
        assert alert.threshold == 2.0

    def test_above_max_alert(self):
        wd = self._wd(window_seconds=10, max_rate=0.5)
        base = 1_000.0
        for i in range(10):
            wd.record(base + i)
        alert = wd.check(base + 9)
        assert isinstance(alert, WatchdogAlert)
        assert alert.kind == "above_max"

    def test_alert_repr(self):
        a = WatchdogAlert("w", 0.1, 2.0, "below_min", 0.0)
        assert "below_min" in repr(a)
        assert "w" in repr(a)
