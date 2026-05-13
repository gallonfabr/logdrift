"""Convenience helpers for building and using SessionTracker instances."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from logdrift.session_tracker import SessionConfig, SessionSummary, SessionTracker


def build_tracker(config: Dict[str, Any]) -> SessionTracker:
    """Build a SessionTracker from a plain config dict.

    Expected keys: ``key_field``, ``timestamp_field``, and optionally
    ``timeout`` (seconds, default 300).
    """
    cfg = SessionConfig(
        key_field=config["key_field"],
        timestamp_field=config["timestamp_field"],
        timeout=float(config.get("timeout", 300.0)),
    )
    return SessionTracker(cfg)


def build_trackers(configs: Iterable[Dict[str, Any]]) -> List[SessionTracker]:
    """Build multiple SessionTracker instances from a list of config dicts."""
    return [build_tracker(c) for c in configs]


def observe_all(
    trackers: Iterable[SessionTracker], record: dict
) -> List[SessionSummary]:
    """Feed *record* to every tracker; return all closed summaries."""
    closed: List[SessionSummary] = []
    for tracker in trackers:
        result = tracker.observe(record)
        if result is not None:
            closed.append(result)
    return closed


def summaries_for_records(
    tracker: SessionTracker, records: Iterable[dict]
) -> List[SessionSummary]:
    """Replay an iterable of records through a single tracker.

    Flushes the tracker at the end so all open sessions are returned.
    """
    summaries: List[SessionSummary] = []
    for record in records:
        result = tracker.observe(record)
        if result is not None:
            summaries.append(result)
    summaries.extend(tracker.flush())
    return summaries
