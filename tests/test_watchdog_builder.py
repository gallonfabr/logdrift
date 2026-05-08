"""Tests for logdrift.watchdog_builder."""
import pytest

from logdrift.watchdog import WatchdogError
from logdrift.watchdog_builder import (
    alerts_for_tick,
    build_watchdog,
    build_watchdogs,
    observe_record,
)


class TestBuildWatchdog:
    def test_defaults_applied(self):
        wd = build_watchdog({})
        assert wd._cfg.window_seconds == 60.0
        assert wd._cfg.min_rate == 0.0
        assert wd._cfg.name == "default"

    def test_custom_values(self):
        wd = build_watchdog({"window_seconds": 30, "min_rate": 1.0, "name": "api"})
        assert wd._cfg.window_seconds == 30.0
        assert wd._cfg.name == "api"

    def test_invalid_config_propagates(self):
        with pytest.raises(WatchdogError):
            build_watchdog({"window_seconds": -1})

    def test_build_multiple(self):
        wds = build_watchdogs([{"name": "a"}, {"name": "b"}])
        assert len(wds) == 2
        assert wds[0]._cfg.name == "a"
        assert wds[1]._cfg.name == "b"


class TestObserveAndAlerts:
    def test_observe_record_increments_all(self):
        wds = build_watchdogs([{"window_seconds": 10}, {"window_seconds": 10}])
        base = 1_000.0
        observe_record(wds, base)
        for wd in wds:
            assert wd.current_rate(base + 1) == pytest.approx(1 / 10)

    def test_no_alerts_when_thresholds_unset(self):
        wds = build_watchdogs([{"window_seconds": 10}])
        observe_record(wds, 1_000.0)
        assert alerts_for_tick(wds, 1_005.0) == []

    def test_alert_returned_for_breach(self):
        wds = build_watchdogs([{"window_seconds": 10, "min_rate": 5.0, "name": "x"}])
        # no events → rate = 0 < 5
        alerts = alerts_for_tick(wds, 1_000.0)
        assert len(alerts) == 1
        assert alerts[0].name == "x"
        assert alerts[0].kind == "below_min"

    def test_multiple_watchdogs_independent_alerts(self):
        wds = build_watchdogs([
            {"window_seconds": 10, "min_rate": 5.0, "name": "slow"},
            {"window_seconds": 10, "max_rate": 0.01, "name": "fast"},
        ])
        base = 1_000.0
        # add 5 events → rate 0.5; too slow for first, too fast for second
        for i in range(5):
            observe_record(wds, base + i)
        alerts = alerts_for_tick(wds, base + 9)
        kinds = {a.name: a.kind for a in alerts}
        assert kinds["slow"] == "below_min"
        assert kinds["fast"] == "above_max"
