"""Tests for logdrift.cardinality."""
import pytest
from logdrift.cardinality import (
    CardinalityAnomaly,
    CardinalityConfig,
    CardinalityError,
    CardinalityTracker,
)


class TestCardinalityConfig:
    def test_valid_config_created(self):
        cfg = CardinalityConfig(field="user_id", max_distinct=50, window_seconds=30.0)
        assert cfg.field == "user_id"
        assert cfg.max_distinct == 50

    def test_empty_field_raises(self):
        with pytest.raises(CardinalityError, match="field"):
            CardinalityConfig(field="")

    def test_non_positive_window_raises(self):
        with pytest.raises(CardinalityError, match="window_seconds"):
            CardinalityConfig(field="x", window_seconds=0)

    def test_max_distinct_below_one_raises(self):
        with pytest.raises(CardinalityError, match="max_distinct"):
            CardinalityConfig(field="x", max_distinct=0)

    def test_min_samples_below_one_raises(self):
        with pytest.raises(CardinalityError, match="min_samples"):
            CardinalityConfig(field="x", min_samples=0)


class TestCardinalityTracker:
    def _tracker(self, max_distinct=3, min_samples=3):
        cfg = CardinalityConfig(
            field="status",
            max_distinct=max_distinct,
            window_seconds=60.0,
            min_samples=min_samples,
        )
        return CardinalityTracker(cfg)

    def test_no_anomaly_before_min_samples(self):
        t = self._tracker(max_distinct=1, min_samples=5)
        for i in range(4):
            result = t.observe({"status": str(i)}, ts=1000.0 + i)
        assert result is None

    def test_no_anomaly_within_limit(self):
        t = self._tracker(max_distinct=5, min_samples=3)
        for i in range(5):
            t.observe({"status": str(i)}, ts=1000.0 + i)
        result = t.observe({"status": "0"}, ts=1005.0)
        assert result is None

    def test_anomaly_when_exceeds_max(self):
        t = self._tracker(max_distinct=2, min_samples=2)
        t.observe({"status": "a"}, ts=1000.0)
        t.observe({"status": "b"}, ts=1001.0)
        result = t.observe({"status": "c"}, ts=1002.0)
        assert isinstance(result, CardinalityAnomaly)
        assert result.field == "status"
        assert result.distinct_count == 3
        assert result.max_distinct == 2

    def test_missing_field_returns_none(self):
        t = self._tracker()
        result = t.observe({"other": "val"}, ts=1000.0)
        assert result is None

    def test_old_values_purged(self):
        t = self._tracker(max_distinct=2, min_samples=2)
        t.observe({"status": "a"}, ts=1000.0)
        t.observe({"status": "b"}, ts=1001.0)
        # advance beyond window so old entries expire
        result = t.observe({"status": "c"}, ts=1200.0)
        assert result is None

    def test_sample_values_included_in_anomaly(self):
        t = self._tracker(max_distinct=1, min_samples=1)
        t.observe({"status": "x"}, ts=1000.0)
        result = t.observe({"status": "y"}, ts=1001.0)
        assert isinstance(result, CardinalityAnomaly)
        assert len(result.sample_values) >= 1
