"""Tests for logdrift.context_window."""
import pytest

from logdrift.context_window import (
    ContextWindow,
    ContextWindowConfig,
    ContextWindowError,
    ContextSnapshot,
)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestContextWindowConfig:
    def test_default_size(self):
        cfg = ContextWindowConfig()
        assert cfg.size == 5

    def test_custom_size(self):
        cfg = ContextWindowConfig(size=10)
        assert cfg.size == 10

    def test_size_zero_raises(self):
        with pytest.raises(ContextWindowError):
            ContextWindowConfig(size=0)

    def test_size_negative_raises(self):
        with pytest.raises(ContextWindowError):
            ContextWindowConfig(size=-3)

    def test_size_too_large_raises(self):
        with pytest.raises(ContextWindowError):
            ContextWindowConfig(size=501)

    def test_size_at_max_boundary(self):
        cfg = ContextWindowConfig(size=500)
        assert cfg.size == 500


# ---------------------------------------------------------------------------
# ContextWindow behaviour
# ---------------------------------------------------------------------------

def _rec(n: int):
    return {"id": n, "msg": f"record-{n}"}


class TestContextWindow:
    def test_buffer_empty_initially(self):
        cw = ContextWindow(ContextWindowConfig(size=3))
        assert cw.buffer == []

    def test_buffer_grows_with_observations(self):
        cw = ContextWindow(ContextWindowConfig(size=3))
        cw.observe(_rec(1))
        cw.observe(_rec(2))
        assert len(cw.buffer) == 2

    def test_buffer_capped_at_size(self):
        cw = ContextWindow(ContextWindowConfig(size=3))
        for i in range(10):
            cw.observe(_rec(i))
        assert len(cw.buffer) == 3

    def test_capture_before_records(self):
        cw = ContextWindow(ContextWindowConfig(size=3))
        cw.observe(_rec(1))
        cw.observe(_rec(2))
        trigger = _rec(99)
        pc = cw.capture(trigger)
        snap = pc.snapshot()
        assert snap.trigger == trigger
        assert [r["id"] for r in snap.before] == [1, 2]

    def test_capture_after_records(self):
        cw = ContextWindow(ContextWindowConfig(size=2))
        trigger = _rec(99)
        pc = cw.capture(trigger)
        cw.observe(_rec(10))
        cw.observe(_rec(11))
        snap = pc.snapshot()
        assert [r["id"] for r in snap.after] == [10, 11]

    def test_pending_resolved_after_enough_records(self):
        cw = ContextWindow(ContextWindowConfig(size=2))
        pc = cw.capture(_rec(0))
        assert not pc.done
        cw.observe(_rec(1))
        assert not pc.done
        cw.observe(_rec(2))
        assert pc.done

    def test_snapshot_to_dict(self):
        snap = ContextSnapshot(trigger={"id": 0}, before=[{"id": -1}], after=[{"id": 1}])
        d = snap.to_dict()
        assert d["trigger"] == {"id": 0}
        assert d["before"] == [{"id": -1}]
        assert d["after"] == [{"id": 1}]

    def test_multiple_captures_independent(self):
        cw = ContextWindow(ContextWindowConfig(size=2))
        pc1 = cw.capture(_rec(100))
        cw.observe(_rec(1))
        pc2 = cw.capture(_rec(200))
        cw.observe(_rec(2))
        cw.observe(_rec(3))
        assert [r["id"] for r in pc1.snapshot().after] == [1, 2]
        assert [r["id"] for r in pc2.snapshot().after] == [2, 3]
