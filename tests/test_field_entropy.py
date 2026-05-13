"""Tests for logdrift.field_entropy."""
import pytest

from logdrift.field_entropy import (
    EntropyAnomaly,
    EntropyError,
    FieldEntropy,
    FieldEntropyConfig,
)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------
class TestFieldEntropyConfig:
    def test_valid_config_created(self):
        cfg = FieldEntropyConfig(field_name="status", window_seconds=30.0, min_samples=5)
        assert cfg.field_name == "status"

    def test_empty_field_raises(self):
        with pytest.raises(EntropyError, match="field_name"):
            FieldEntropyConfig(field_name="")

    def test_non_positive_window_raises(self):
        with pytest.raises(EntropyError, match="window_seconds"):
            FieldEntropyConfig(field_name="x", window_seconds=0)

    def test_min_samples_below_two_raises(self):
        with pytest.raises(EntropyError, match="min_samples"):
            FieldEntropyConfig(field_name="x", min_samples=1)

    def test_inverted_thresholds_raise(self):
        with pytest.raises(EntropyError, match="thresholds"):
            FieldEntropyConfig(field_name="x", low_entropy_threshold=0.8, high_entropy_threshold=0.5)

    def test_equal_thresholds_raise(self):
        with pytest.raises(EntropyError, match="thresholds"):
            FieldEntropyConfig(field_name="x", low_entropy_threshold=0.5, high_entropy_threshold=0.5)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _tracker(low=0.2, high=0.95, min_samples=5, window=60.0):
    cfg = FieldEntropyConfig(
        field_name="level",
        window_seconds=window,
        min_samples=min_samples,
        low_entropy_threshold=low,
        high_entropy_threshold=high,
    )
    return FieldEntropy(cfg)


# ---------------------------------------------------------------------------
# Behaviour tests
# ---------------------------------------------------------------------------
class TestFieldEntropy:
    def test_no_anomaly_before_min_samples(self):
        tracker = _tracker(min_samples=10)
        for i in range(9):
            result = tracker.observe("INFO", float(i))
        assert result is None

    def test_no_anomaly_for_balanced_distribution(self):
        tracker = _tracker(low=0.1, high=0.99, min_samples=4)
        values = ["A", "B", "C", "D"]
        result = None
        for i, v in enumerate(values):
            result = tracker.observe(v, float(i))
        # perfectly uniform → entropy ≈ 1.0, but threshold is 0.99 so no alert
        assert result is None

    def test_low_entropy_anomaly_when_one_value_dominates(self):
        tracker = _tracker(low=0.3, high=0.99, min_samples=5)
        result = None
        for i in range(5):
            result = tracker.observe("ERROR", float(i))  # all same value → entropy 0
        assert result is not None
        assert isinstance(result, EntropyAnomaly)
        assert result.kind == "low"
        assert result.normalised_entropy == 0.0
        assert "ERROR" in result.top_values

    def test_high_entropy_anomaly_when_all_unique(self):
        tracker = _tracker(low=0.05, high=0.7, min_samples=5)
        result = None
        for i in range(5):
            result = tracker.observe(f"val_{i}", float(i))  # all unique → max entropy
        assert result is not None
        assert result.kind == "high"
        assert result.normalised_entropy > 0.7

    def test_old_events_evicted_from_window(self):
        tracker = _tracker(low=0.3, high=0.99, min_samples=5, window=10.0)
        # seed with all-ERROR observations at t=0..4
        for i in range(5):
            tracker.observe("ERROR", float(i))
        # now observe diverse values far in the future so old ones are purged
        result = None
        for i, v in enumerate(["A", "B", "C", "D", "E"]):
            result = tracker.observe(v, 100.0 + i)
        # window should contain only the new diverse values → no low-entropy alert
        assert result is None or result.kind != "low"

    def test_sample_count_matches_window_size(self):
        tracker = _tracker(low=0.3, high=0.99, min_samples=5)
        for i in range(5):
            result = tracker.observe("X", float(i))
        assert result is not None
        assert result.sample_count == 5
