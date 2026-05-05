"""Tests for logdrift.sampler."""
import pytest

from logdrift.sampler import Sampler, SamplerConfig, SamplerError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _records(n: int, level: str = "info"):
    return [{"level": level, "msg": f"msg-{i}"} for i in range(n)]


# ---------------------------------------------------------------------------
# SamplerConfig
# ---------------------------------------------------------------------------

class TestSamplerConfig:
    def test_default_rate_is_one(self):
        cfg = SamplerConfig()
        assert cfg.rate == 1

    def test_invalid_rate_raises(self):
        with pytest.raises(SamplerError):
            SamplerConfig(rate=0)

    def test_negative_rate_raises(self):
        with pytest.raises(SamplerError):
            SamplerConfig(rate=-5)


# ---------------------------------------------------------------------------
# Sampler.should_keep
# ---------------------------------------------------------------------------

class TestSampler:
    def test_rate_one_keeps_all(self):
        s = Sampler(SamplerConfig(rate=1))
        results = [s.should_keep(r) for r in _records(5)]
        assert all(results)

    def test_rate_two_keeps_every_second(self):
        s = Sampler(SamplerConfig(rate=2))
        results = [s.should_keep(r) for r in _records(6)]
        # positions 2,4,6 (1-indexed) are kept
        assert results == [False, True, False, True, False, True]

    def test_always_include_overrides_rate(self):
        s = Sampler(SamplerConfig(
            rate=100,
            always_include=lambda r: r.get("level") == "error",
        ))
        error_rec = {"level": "error", "msg": "boom"}
        info_rec = {"level": "info", "msg": "ok"}
        assert s.should_keep(error_rec) is True
        assert s.should_keep(info_rec) is False

    def test_stats_updated(self):
        s = Sampler(SamplerConfig(rate=2))
        list(s.filter(_records(4)))
        assert s.stats == {"seen": 4, "kept": 2}

    def test_reset_clears_counters(self):
        s = Sampler(SamplerConfig(rate=1))
        list(s.filter(_records(3)))
        s.reset()
        assert s.stats == {"seen": 0, "kept": 0}

    def test_filter_yields_correct_records(self):
        s = Sampler(SamplerConfig(rate=3))
        records = _records(9)
        kept = list(s.filter(iter(records)))
        assert len(kept) == 3
        assert kept == [records[2], records[5], records[8]]
