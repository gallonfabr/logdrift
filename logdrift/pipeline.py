"""End-to-end pipeline: read → sample → filter → validate → detect → aggregate."""
from __future__ import annotations

import sys
from typing import Any, Dict, Iterable, List, Optional

from logdrift.aggregator import Aggregator
from logdrift.detector import Detector
from logdrift.enricher import Enricher
from logdrift.filter import Filter
from logdrift.reader import read_records
from logdrift.sampler import Sampler
from logdrift.schema import Schema
from logdrift.throttle import Throttle


class ValidationError(Exception):
    """Raised when a record fails schema validation."""


def run_pipeline(
    path: str,
    *,
    schema: Optional[Schema] = None,
    detector: Optional[Detector] = None,
    aggregator: Optional[Aggregator] = None,
    sampler: Optional[Sampler] = None,
    throttle: Optional[Throttle] = None,
    enricher: Optional[Enricher] = None,
    record_filter: Optional[Filter] = None,
    skip_invalid: bool = False,
) -> List[Dict[str, Any]]:
    """Process a log file and return a list of emitted anomaly events.

    Parameters
    ----------
    path:
        Path to the log file.
    schema:
        Optional schema for record validation.
    detector:
        Anomaly detector; events are collected when provided.
    aggregator:
        Aggregator that accumulates field statistics.
    sampler:
        If given, only a fraction of records are processed.
    throttle:
        Throttle applied to emitted anomaly events.
    enricher:
        Enricher that adds derived fields before detection.
    record_filter:
        Filter applied *before* validation and detection.
    skip_invalid:
        When ``True``, validation errors are printed to stderr and the
        offending record is skipped instead of raising.
    """
    events: List[Dict[str, Any]] = []
    records: Iterable[Dict[str, Any]] = read_records(path)

    if sampler is not None:
        records = sampler.sample(list(records))

    for record in records:
        # 1. Filter
        if record_filter is not None and not record_filter.apply(record):
            continue

        # 2. Enrich
        if enricher is not None:
            record = enricher.enrich(record)

        # 3. Validate
        if schema is not None:
            errors = schema.validate(record)
            if errors:
                msg = f"validation errors: {errors} in record {record}"
                if skip_invalid:
                    print(msg, file=sys.stderr)
                    continue
                raise ValidationError(msg)

        # 4. Aggregate
        if aggregator is not None:
            for key, value in record.items():
                aggregator.add(key, value)

        # 5. Detect
        if detector is not None:
            for key, value in record.items():
                event = detector.process(key, str(value))
                if event is None:
                    continue
                if throttle is not None and not throttle.allow(event):
                    continue
                events.append({"field": key, "value": str(value), "score": event.score})

    return events


def summarise(events: List[Dict[str, Any]]) -> str:
    """Return a human-readable summary of pipeline anomaly events."""
    if not events:
        return "No anomalies detected."
    lines = [f"Anomalies detected: {len(events)}"]
    for ev in events:
        lines.append(f"  field={ev['field']} value={ev['value']} score={ev['score']:.4f}")
    return "\n".join(lines)
