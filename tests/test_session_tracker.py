"""Tests for logdrift.session_tracker and logdrift.session_builder."""
from __future__ import annotations

import pytest

from logdrift.session_tracker import SessionConfig, SessionError, SessionTracker
from logdrift.session_builder import build_tracker, build_trackers, observe_all, summaries_for_records


# ---------------------------------------------------------------------------
# SessionConfig
# ---------------------------------------------------------------------------

class TestSessionConfig:
    def test_valid_config_created(self):
        cfg = SessionConfig(key_field="user_id", timestamp_field="ts", timeout=60.0)
        assert cfg.key_field == "user_id"
        assert cfg.timeout == 60.0

    def test_empty_key_field_raises(self):
        with pytest.raises(SessionError, match="key_field"):
            SessionConfig(key_field="", timestamp_field="ts")

    def test_empty_timestamp_field_raises(self):
        with pytest.raises(SessionError, match="timestamp_field"):
            SessionConfig(key_field="uid", timestamp_field="")

    def test_non_positive_timeout_raises(self):
        with pytest.raises(SessionError, match="timeout"):
            SessionConfig(key_field="uid", timestamp_field="ts", timeout=0)

    def test_negative_timeout_raises(self):
        with pytest.raises(SessionError, match="timeout"):
            SessionConfig(key_field="uid", timestamp_field="ts", timeout=-5.0)


# ---------------------------------------------------------------------------
# SessionTracker
# ---------------------------------------------------------------------------

def _tracker(timeout: float = 30.0) -> SessionTracker:
    cfg = SessionConfig(key_field="user", timestamp_field="ts", timeout=timeout)
    return SessionTracker(cfg)


class TestSessionTracker:
    def test_returns_none_while_session_open(self):
        t = _tracker()
        assert t.observe({"user": "alice", "ts": 0}) is None
        assert t.observe({"user": "alice", "ts": 10}) is None

    def test_closes_session_on_timeout(self):
        t = _tracker(timeout=30.0)
        t.observe({"user": "alice", "ts": 0})
        t.observe({"user": "alice", "ts": 20})
        summary = t.observe({"user": "alice", "ts": 60})  # gap > 30 s
        assert summary is not None
        assert summary.key == "alice"
        assert summary.record_count == 2
        assert summary.duration == pytest.approx(20.0)

    def test_flush_returns_all_open_sessions(self):
        t = _tracker()
        t.observe({"user": "alice", "ts": 0})
        t.observe({"user": "bob", "ts": 5})
        summaries = t.flush()
        assert len(summaries) == 2
        keys = {s.key for s in summaries}
        assert keys == {"alice", "bob"}

    def test_missing_key_field_ignored(self):
        t = _tracker()
        result = t.observe({"ts": 0})
        assert result is None

    def test_missing_timestamp_field_ignored(self):
        t = _tracker()
        result = t.observe({"user": "alice"})
        assert result is None

    def test_fields_seen_populated(self):
        t = _tracker()
        t.observe({"user": "alice", "ts": 0, "status": "ok"})
        t.observe({"user": "alice", "ts": 5, "status": "ok"})
        summaries = t.flush()
        assert summaries[0].fields_seen.get("status") == 2


# ---------------------------------------------------------------------------
# session_builder helpers
# ---------------------------------------------------------------------------

class TestSessionBuilder:
    def test_build_tracker_from_dict(self):
        t = build_tracker({"key_field": "uid", "timestamp_field": "ts", "timeout": 60})
        assert isinstance(t, SessionTracker)

    def test_build_trackers_returns_list(self):
        cfgs = [
            {"key_field": "uid", "timestamp_field": "ts"},
            {"key_field": "ip", "timestamp_field": "ts", "timeout": 10},
        ]
        trackers = build_trackers(cfgs)
        assert len(trackers) == 2

    def test_observe_all_collects_closed(self):
        t1 = build_tracker({"key_field": "user", "timestamp_field": "ts", "timeout": 5})
        t2 = build_tracker({"key_field": "user", "timestamp_field": "ts", "timeout": 5})
        t1.observe({"user": "alice", "ts": 0})
        t2.observe({"user": "alice", "ts": 0})
        closed = observe_all([t1, t2], {"user": "alice", "ts": 10})
        assert len(closed) == 2

    def test_summaries_for_records_includes_flush(self):
        t = build_tracker({"key_field": "user", "timestamp_field": "ts", "timeout": 100})
        records = [
            {"user": "alice", "ts": 0},
            {"user": "alice", "ts": 5},
        ]
        summaries = summaries_for_records(t, records)
        # no timeout triggered, but flush at end should yield one summary
        assert len(summaries) == 1
        assert summaries[0].record_count == 2
