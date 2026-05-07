"""Convenience builders for constructing replay sessions from config dicts."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from logdrift.detector import Detector
from logdrift.replay import ReplayConfig, ReplayError, ReplayResult, replay


def build_replay_config(cfg: Dict[str, Any]) -> ReplayConfig:
    """Build a :class:`~logdrift.replay.ReplayConfig` from a plain dict.

    Accepted keys mirror the dataclass fields:
    ``speed_factor``, ``timestamp_field``, ``max_records``.

    Unknown keys are silently ignored so callers can pass a broader
    config dict without pre-filtering it.
    """
    kwargs: Dict[str, Any] = {}
    if "speed_factor" in cfg:
        kwargs["speed_factor"] = float(cfg["speed_factor"])
    if "timestamp_field" in cfg:
        kwargs["timestamp_field"] = str(cfg["timestamp_field"])
    if "max_records" in cfg:
        raw = cfg["max_records"]
        kwargs["max_records"] = int(raw) if raw is not None else None
    try:
        return ReplayConfig(**kwargs)
    except ReplayError:
        raise
    except (TypeError, ValueError) as exc:
        raise ReplayError(f"Invalid replay config value: {exc}") from exc


def run_replay(
    records: List[dict],
    detector_fields: List[str],
    *,
    min_samples: int = 30,
    config: Optional[ReplayConfig] = None,
    on_anomaly: Optional[Callable] = None,
) -> ReplayResult:
    """High-level helper: build detectors from *detector_fields* and run replay.

    Parameters
    ----------
    records:
        Pre-parsed log records.
    detector_fields:
        Field names to monitor; one :class:`~logdrift.detector.Detector`
        is created per field.
    min_samples:
        Minimum observations before anomaly scoring begins.
    config:
        Optional :class:`~logdrift.replay.ReplayConfig`.
    on_anomaly:
        Optional callback for each anomaly event.
    """
    if not detector_fields:
        raise ReplayError("detector_fields must not be empty")

    detectors = [
        Detector(fields=[f], min_samples=min_samples)
        for f in detector_fields
    ]
    return replay(records, detectors, config=config, on_anomaly=on_anomaly)
