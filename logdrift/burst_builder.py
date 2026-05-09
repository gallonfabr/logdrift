"""Helpers for constructing and running BurstDetector instances."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from logdrift.burst_detector import BurstAlert, BurstConfig, BurstDetector, BurstError


def build_detector(
    field: str,
    value: str,
    *,
    window_seconds: float = 60.0,
    cooldown_seconds: float = 30.0,
    multiplier: float = 3.0,
    min_baseline_periods: int = 3,
) -> BurstDetector:
    """Construct a BurstDetector from keyword arguments."""
    config = BurstConfig(
        window_seconds=window_seconds,
        cooldown_seconds=cooldown_seconds,
        multiplier=multiplier,
        min_baseline_periods=min_baseline_periods,
    )
    return BurstDetector(field=field, value=value, config=config)


def build_detectors(
    specs: Iterable[Dict],
) -> List[BurstDetector]:
    """Build multiple detectors from a list of spec dicts.

    Each dict must contain 'field' and 'value'; all other keys are optional
    and forwarded to BurstConfig.
    """
    detectors: List[BurstDetector] = []
    for spec in specs:
        spec = dict(spec)
        field = spec.pop("field")
        value = spec.pop("value")
        detectors.append(build_detector(field, value, **spec))
    return detectors


def observe_all(
    detectors: List[BurstDetector],
    record: dict,
    now: Optional[float] = None,
) -> List[BurstAlert]:
    """Feed *record* to every detector; return all alerts produced."""
    alerts: List[BurstAlert] = []
    for det in detectors:
        alert = det.observe(record, now=now)
        if alert is not None:
            alerts.append(alert)
    return alerts
