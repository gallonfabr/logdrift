"""Session tracker – groups log records into sessions by a key field.

A session is a sequence of records sharing the same key value where
consecutive records arrive within *timeout* seconds of each other.
Once a gap larger than *timeout* is seen the session is closed and a
SessionSummary is emitted.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


class SessionError(Exception):
    """Raised for invalid SessionTracker configuration."""


@dataclass
class SessionConfig:
    key_field: str
    timestamp_field: str
    timeout: float = 300.0  # seconds

    def __post_init__(self) -> None:
        if not self.key_field:
            raise SessionError("key_field must not be empty")
        if not self.timestamp_field:
            raise SessionError("timestamp_field must not be empty")
        if self.timeout <= 0:
            raise SessionError("timeout must be positive")


@dataclass
class SessionSummary:
    key: str
    start_ts: float
    end_ts: float
    record_count: int
    fields_seen: Dict[str, int] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        return self.end_ts - self.start_ts

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"SessionSummary(key={self.key!r}, duration={self.duration:.1f}s, "
            f"records={self.record_count})"
        )


class SessionTracker:
    """Tracks open sessions and closes them when a timeout elapses."""

    def __init__(self, config: SessionConfig) -> None:
        self._cfg = config
        self._sessions: Dict[str, dict] = {}

    # ------------------------------------------------------------------
    def observe(self, record: dict) -> Optional[SessionSummary]:
        """Feed a record; returns a closed SessionSummary if one closed."""
        key = record.get(self._cfg.key_field)
        ts = record.get(self._cfg.timestamp_field)
        if key is None or ts is None:
            return None
        ts = float(ts)
        closed: Optional[SessionSummary] = None
        if key in self._sessions:
            sess = self._sessions[key]
            if ts - sess["last_ts"] > self._cfg.timeout:
                closed = self._close(key)
                self._sessions[key] = self._new_session(key, ts)
            else:
                sess["last_ts"] = ts
                sess["end_ts"] = ts
                sess["count"] += 1
                self._update_fields(sess, record)
        else:
            self._sessions[key] = self._new_session(key, ts)
            self._update_fields(self._sessions[key], record)
        return closed

    def flush(self) -> List[SessionSummary]:
        """Close all open sessions and return their summaries."""
        summaries = [self._close(k) for k in list(self._sessions)]
        return summaries

    # ------------------------------------------------------------------
    def _new_session(self, key: str, ts: float) -> dict:
        return {"key": key, "start_ts": ts, "end_ts": ts, "last_ts": ts, "count": 1, "fields": {}}

    def _update_fields(self, sess: dict, record: dict) -> None:
        for k in record:
            sess["fields"][k] = sess["fields"].get(k, 0) + 1

    def _close(self, key: str) -> SessionSummary:
        sess = self._sessions.pop(key)
        return SessionSummary(
            key=sess["key"],
            start_ts=sess["start_ts"],
            end_ts=sess["end_ts"],
            record_count=sess["count"],
            fields_seen=dict(sess["fields"]),
        )
