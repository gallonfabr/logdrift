"""Convenience helpers to build Correlator instances from config dicts."""
from __future__ import annotations

from typing import Any, Dict, List

from logdrift.correlator import Correlator, CorrelatorError


def build_correlator(config: Dict[str, Any]) -> Correlator:
    """Build a :class:`Correlator` from a plain dictionary.

    Expected keys:
        - ``field_a`` (str, required)
        - ``field_b`` (str, required)
        - ``threshold`` (float, optional, default 0.01)
        - ``min_samples`` (int, optional, default 30)

    Raises:
        CorrelatorError: if required keys are missing or values are invalid.
    """
    try:
        field_a = config["field_a"]
        field_b = config["field_b"]
    except KeyError as exc:
        raise CorrelatorError(f"Missing required config key: {exc}") from exc

    return Correlator(
        field_a=field_a,
        field_b=field_b,
        threshold=config.get("threshold", 0.01),
        min_samples=config.get("min_samples", 30),
    )


def build_correlators(configs: List[Dict[str, Any]]) -> List[Correlator]:
    """Build multiple :class:`Correlator` instances from a list of config dicts."""
    return [build_correlator(cfg) for cfg in configs]


def observe_all(correlators: List[Correlator], record: dict) -> None:
    """Feed *record* into every correlator in *correlators*."""
    for correlator in correlators:
        correlator.observe(record)


def anomalies_for_record(
    correlators: List[Correlator], record: dict
) -> List[Dict[str, Any]]:
    """Return a list of anomaly dicts for any flagged correlator pairs.

    Each entry contains:
        - ``field_a``, ``field_b``: the correlated field names
        - ``val_a``, ``val_b``: the observed values
        - ``frequency``: observed co-occurrence frequency
    """
    results = []
    for correlator in correlators:
        val_a = record.get(correlator.field_a)
        val_b = record.get(correlator.field_b)
        if val_a is None or val_b is None:
            continue
        if correlator.is_anomalous(str(val_a), str(val_b)):
            ps = correlator.stats(str(val_a), str(val_b))
            results.append({
                "field_a": correlator.field_a,
                "field_b": correlator.field_b,
                "val_a": str(val_a),
                "val_b": str(val_b),
                "frequency": ps.frequency,
            })
    return results
