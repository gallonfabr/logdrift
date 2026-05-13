"""Helpers to build and run CadenceDetector instances from plain config dicts."""

from __future__ import annotations

from typing import Iterable, Iterator, List

from logdrift.cadence import CadenceAnomaly, CadenceConfig, CadenceDetector


def build_detector(cfg: dict) -> CadenceDetector:
    """Build a CadenceDetector from a plain dict (e.g. loaded from YAML)."""
    config = CadenceConfig(
        field=cfg["field"],
        window=int(cfg.get("window", 60)),
        min_periods=int(cfg.get("min_periods", 4)),
        z_threshold=float(cfg.get("z_threshold", 3.0)),
    )
    return CadenceDetector(config)


def build_detectors(cfgs: Iterable[dict]) -> List[CadenceDetector]:
    """Build multiple detectors from a list of config dicts."""
    return [build_detector(c) for c in cfgs]


def observe_all(
    detectors: Iterable[CadenceDetector],
    record: dict,
    ts: float | None = None,
) -> None:
    """Feed a record to every detector (side-effects only)."""
    for det in detectors:
        det.observe(record, ts=ts)


def anomalies_for_record(
    detectors: Iterable[CadenceDetector],
    record: dict,
    ts: float | None = None,
) -> Iterator[CadenceAnomaly]:
    """Yield any cadence anomalies produced by each detector for *record*."""
    for det in detectors:
        result = det.observe(record, ts=ts)
        if result is not None:
            yield result
