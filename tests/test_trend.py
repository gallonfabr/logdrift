"""Unit tests for logdrift.trend."""
import pytest
from logdrift.trend import Trend, TrendAnomaly, TrendConfig, TrendError


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestTrendConfig:
    def test_valid_config_created(self):
        cfg = TrendConfig(field="status", window_seconds=30.0, spike_factor=3.0)
        assert cfg.field == "status"

    def test_empty_field_raises(self):
        with pytest.raises(TrendError, match="field"):
            TrendConfig(field="")

    def test_non_positive_window_raises(self):
        with pytest.raises(TrendError, match="window_seconds"):
            TrendConfig(field="f", window_seconds=0)

    def test_min_periods_below_two_raises(self):
        with pytest.raises(TrendError, match="min_periods"):
            TrendConfig(field="f", min_periods=1)

    def test_spike_factor_at_one_raises(self):
        with pytest.raises(TrendError, match="spike_factor"):
            TrendConfig(field="f", spike_factor=1.0)


# ---------------------------------------------------------------------------
# Trend.observe behaviour
# ---------------------------------------------------------------------------

class TestTrend:
    def _make(self, **kwargs) -> Trend:
        return Trend(TrendConfig(field="level", window_seconds=10.0, spike_factor=2.0, **kwargs))

    def test_no_anomaly_before_min_periods(self):
        t = self._make()
        result = t.observe(0.0, "ERROR")
        assert result is None

    def test_no_anomaly_for_steady_rate(self):
        t = self._make()
        # spread observations evenly across the window
        for i in range(10):
            t.observe(float(i), "ERROR")
        result = t.observe(10.0, "ERROR")
        assert result is None

    def test_spike_detected(self):
        t = self._make(min_periods=2, spike_factor=2.0)
        # one event in the first half
        t.observe(0.0, "ERROR")
        # flood the second half
        for i in range(6):
            t.observe(6.0 + i * 0.5, "ERROR")
        result = t.observe(9.9, "ERROR")
        assert isinstance(result, TrendAnomaly)
        assert result.field == "level"
        assert result.value == "ERROR"
        assert result.current_rate > result.previous_rate

    def test_stale_events_purged(self):
        t = self._make()
        t.observe(0.0, "WARN")
        # after the window has passed the old event should be gone
        result = t.observe(100.0, "WARN")
        assert result is None

    def test_different_values_tracked_independently(self):
        t = self._make()
        t.observe(0.0, "INFO")
        result = t.observe(5.0, "ERROR")  # ERROR has no history yet
        assert result is None

    def test_anomaly_repr_contains_field(self):
        a = TrendAnomaly(field="status", value="500", previous_rate=1.0, current_rate=5.0, spike_factor=5.0)
        assert "status" in repr(a)
