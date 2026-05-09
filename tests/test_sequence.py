"""Tests for logdrift.sequence."""

import pytest

from logdrift.sequence import (
    Sequence,
    SequenceConfig,
    SequenceError,
    TransitionAnomaly,
)


# ---------------------------------------------------------------------------
# SequenceConfig
# ---------------------------------------------------------------------------

class TestSequenceConfig:
    def test_valid_config_created(self):
        cfg = SequenceConfig(field="status", min_support=3, window=60)
        assert cfg.field == "status"
        assert cfg.min_support == 3
        assert cfg.window == 60

    def test_empty_field_raises(self):
        with pytest.raises(SequenceError, match="field"):
            SequenceConfig(field="")

    def test_min_support_below_one_raises(self):
        with pytest.raises(SequenceError, match="min_support"):
            SequenceConfig(field="status", min_support=0)

    def test_non_positive_window_raises(self):
        with pytest.raises(SequenceError, match="window"):
            SequenceConfig(field="status", window=0)

    def test_defaults(self):
        cfg = SequenceConfig(field="level")
        assert cfg.min_support == 5
        assert cfg.window == 300


# ---------------------------------------------------------------------------
# Sequence
# ---------------------------------------------------------------------------

@pytest.fixture()
def seq():
    return Sequence(SequenceConfig(field="status", min_support=3))


class TestSequence:
    def test_first_observation_returns_no_anomaly(self, seq):
        assert seq.observe("ok") is None

    def test_rare_transition_returns_anomaly(self, seq):
        seq.observe("ok")
        result = seq.observe("error")
        assert isinstance(result, TransitionAnomaly)
        assert result.from_value == "ok"
        assert result.to_value == "error"
        assert result.count == 1

    def test_common_transition_returns_no_anomaly(self, seq):
        for _ in range(3):
            seq.observe("ok")
            seq.observe("done")
        # 4th time count == min_support, should not flag
        seq.observe("ok")
        result = seq.observe("done")
        assert result is None

    def test_transition_count_tracked(self, seq):
        seq.observe("a")
        seq.observe("b")
        seq.observe("a")
        seq.observe("b")
        assert seq.transition_count("a", "b") == 2

    def test_transition_count_missing_pair_is_zero(self, seq):
        assert seq.transition_count("x", "y") == 0

    def test_none_value_skipped(self, seq):
        seq.observe("ok")
        result = seq.observe(None)
        assert result is None

    def test_reset_clears_state(self, seq):
        seq.observe("ok")
        seq.observe("err")
        seq.reset()
        assert seq.transition_count("ok", "err") == 0
        # after reset, first observation again has no previous
        assert seq.observe("ok") is None

    def test_anomaly_fields_correct(self, seq):
        seq.observe("start")
        anomaly = seq.observe("stop")
        assert anomaly.field == "status"
        assert anomaly.min_support == 3
