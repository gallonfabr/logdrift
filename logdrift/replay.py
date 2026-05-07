"""Replay recorded log events through a detector pipeline for offline analysis."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator, List, Optional

from logdrift.detector import AnomalyEvent, Detector


class ReplayError(Exception):
    """Raised when replay configuration or execution fails."""


@dataclass
class ReplayConfig:
    """Configuration for a replay session."""

    speed_factor: float = 1.0  # 1.0 = real-time, 0 = no delay
    timestamp_field: str = "ts"
    max_records: Optional[int] = None

    def __post_init__(self) -> None:
        if self.speed_factor < 0:
            raise ReplayError("speed_factor must be >= 0")
        if not self.timestamp_field:
            raise ReplayError("timestamp_field must not be empty")
        if self.max_records is not None and self.max_records < 1:
            raise ReplayError("max_records must be >= 1 when set")


@dataclass
class ReplayResult:
    """Summary of a completed replay session."""

    records_processed: int = 0
    anomalies_found: int = 0
    events: List[AnomalyEvent] = field(default_factory=list)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ReplayResult(records={self.records_processed}, "
            f"anomalies={self.anomalies_found})"
        )


def replay(
    records: Iterable[dict],
    detectors: List[Detector],
    config: Optional[ReplayConfig] = None,
    on_anomaly: Optional[Callable[[AnomalyEvent], None]] = None,
) -> ReplayResult:
    """Replay *records* through *detectors* and return a summary.

    Parameters
    ----------
    records:
        Iterable of parsed log records (dicts).
    detectors:
        One or more :class:`~logdrift.detector.Detector` instances.
    config:
        Optional :class:`ReplayConfig`; defaults are used when ``None``.
    on_anomaly:
        Optional callback invoked for every anomaly event produced.
    """
    if config is None:
        config = ReplayConfig()

    result = ReplayResult()
    prev_ts: Optional[float] = None

    for record in _bounded(records, config.max_records):
        result.records_processed += 1

        if config.speed_factor > 0:
            ts = _extract_ts(record, config.timestamp_field)
            if ts is not None and prev_ts is not None and ts > prev_ts:
                delay = (ts - prev_ts) / config.speed_factor
                time.sleep(delay)
            prev_ts = ts

        for detector in detectors:
            for field_name, value in record.items():
                event = detector.observe(field_name, value)
                if event is not None:
                    result.anomalies_found += 1
                    result.events.append(event)
                    if on_anomaly is not None:
                        on_anomaly(event)

    return result


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _bounded(records: Iterable[dict], limit: Optional[int]) -> Iterator[dict]:
    for i, record in enumerate(records):
        if limit is not None and i >= limit:
            break
        yield record


def _extract_ts(record: dict, field_name: str) -> Optional[float]:
    raw = record.get(field_name)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
