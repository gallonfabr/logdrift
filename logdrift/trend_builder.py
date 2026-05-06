"""Helpers to build Trend detectors and process records."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from logdrift.trend import Trend, TrendAnomaly, TrendConfig


def build_trend(field: str, **kwargs) -> Trend:
    """Convenience factory: build a Trend from keyword arguments."""
    return Trend(TrendConfig(field=field, **kwargs))


def build_trends(specs: List[Dict]) -> List[Trend]:
    """Build multiple Trend detectors from a list of config dicts.

    Each dict must contain at least ``field``; remaining keys are forwarded
    to :class:`TrendConfig`.
    """
    trends: List[Trend] = []
    for spec in specs:
        spec = dict(spec)  # copy so we don't mutate caller's data
        field = spec.pop("field")
        trends.append(build_trend(field, **spec))
    return trends


def observe_record(
    trends: List[Trend],
    record: Dict,
    timestamp: float,
) -> List[TrendAnomaly]:
    """Feed *record* into every Trend whose field is present; collect anomalies."""
    anomalies: List[TrendAnomaly] = []
    for trend in trends:
        field = trend._cfg.field
        raw = record.get(field)
        if raw is None:
            continue
        result = trend.observe(timestamp, str(raw))
        if result is not None:
            anomalies.append(result)
    return anomalies


def anomalies_for_records(
    trends: List[Trend],
    records: Iterable[Tuple[float, Dict]],
) -> List[TrendAnomaly]:
    """Process an iterable of (timestamp, record) pairs; return all anomalies."""
    all_anomalies: List[TrendAnomaly] = []
    for timestamp, record in records:
        all_anomalies.extend(observe_record(trends, record, timestamp))
    return all_anomalies
