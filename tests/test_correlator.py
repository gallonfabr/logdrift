"""Tests for logdrift.correlator."""
import pytest
from logdrift.correlator import Correlator, CorrelatorError, PairStats


def _warm_up(correlator: Correlator, val_a: str, val_b: str, n: int) -> None:
    for _ in range(n):
        correlator.observe({correlator.field_a: val_a, correlator.field_b: val_b})


class TestCorrelatorConfig:
    def test_empty_field_a_raises(self):
        with pytest.raises(CorrelatorError):
            Correlator("", "status")

    def test_empty_field_b_raises(self):
        with pytest.raises(CorrelatorError):
            Correlator("method", "")

    def test_invalid_threshold_zero_raises(self):
        with pytest.raises(CorrelatorError):
            Correlator("method", "status", threshold=0.0)

    def test_invalid_threshold_one_raises(self):
        with pytest.raises(CorrelatorError):
            Correlator("method", "status", threshold=1.0)

    def test_invalid_min_samples_raises(self):
        with pytest.raises(CorrelatorError):
            Correlator("method", "status", min_samples=0)


class TestCorrelator:
    def setup_method(self):
        self.c = Correlator("method", "status", threshold=0.05, min_samples=10)

    def test_no_anomaly_before_min_samples(self):
        _warm_up(self.c, "GET", "200", 5)
        assert self.c.is_anomalous("GET", "500") is False

    def test_no_anomaly_for_common_pair(self):
        _warm_up(self.c, "GET", "200", 20)
        assert self.c.is_anomalous("GET", "200") is False

    def test_anomaly_for_unseen_pair_after_warmup(self):
        _warm_up(self.c, "GET", "200", 20)
        assert self.c.is_anomalous("GET", "500") is True

    def test_anomaly_for_rare_pair_after_warmup(self):
        _warm_up(self.c, "POST", "201", 19)
        self.c.observe({"method": "POST", "status": "500"})
        # 500 frequency = 1/20 = 0.05, equal to threshold -> not anomalous
        assert self.c.is_anomalous("POST", "500") is False

    def test_anomaly_when_below_threshold(self):
        _warm_up(self.c, "DELETE", "204", 19)
        self.c.observe({"method": "DELETE", "status": "403"})
        # 403 count=1, total=20, freq=0.05 — exactly at threshold, not anomalous
        # drop one more to push it below
        _warm_up(self.c, "DELETE", "204", 1)  # total now 21, 403 freq = 1/21 < 0.05
        assert self.c.is_anomalous("DELETE", "403") is True

    def test_missing_field_skips_observe(self):
        self.c.observe({"method": "GET"})  # missing 'status'
        assert self.c._totals.get("GET", 0) == 0

    def test_stats_returns_pair_stats(self):
        _warm_up(self.c, "GET", "200", 10)
        ps = self.c.stats("GET", "200")
        assert isinstance(ps, PairStats)
        assert ps.count == 10

    def test_pair_stats_frequency(self):
        ps = PairStats(count=3, total_seen=10)
        assert ps.frequency == pytest.approx(0.3)

    def test_pair_stats_zero_total(self):
        ps = PairStats()
        assert ps.frequency == 0.0
