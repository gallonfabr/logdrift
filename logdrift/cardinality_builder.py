"""Builder helpers for CardinalityTracker instances."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from logdrift.cardinality import CardinalityAnomaly, CardinalityConfig, CardinalityTracker


def build_tracker(config: Dict[str, Any]) -> CardinalityTracker:
    """Build a CardinalityTracker from a plain config dict."""
    cfg = CardinalityConfig(
        field=config["field"],
        max_distinct=config.get("max_distinct", 100),
        window_seconds=config.get("window_seconds", 60.0),
        min_samples=config.get("min_samples", 5),
    )
    return CardinalityTracker(cfg)


def build_trackers(configs: Iterable[Dict[str, Any]]) -> List[CardinalityTracker]:
    """Build multiple CardinalityTracker instances."""
    return [build_tracker(c) for c in configs]


def observe_all(
    trackers: List[CardinalityTracker],
    record: dict,
    ts: Optional[float] = None,
) -> None:
    """Feed a record to every tracker (discards results)."""
    for tracker in trackers:
        tracker.observe(record, ts=ts)


def anomalies_for_record(
    trackers: List[CardinalityTracker],
    record: dict,
    ts: Optional[float] = None,
) -> List[CardinalityAnomaly]:
    """Feed a record to every tracker and return all anomalies produced."""
    results: List[CardinalityAnomaly] = []
    for tracker in trackers:
        anomaly = tracker.observe(record, ts=ts)
        if anomaly is not None:
            results.append(anomaly)
    return results
