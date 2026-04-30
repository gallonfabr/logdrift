"""High-level pipeline: read → parse → validate → detect."""

from typing import Iterator, List, Optional

from logdrift.detector import AnomalyEvent, Detector
from logdrift.reader import read_records
from logdrift.schema import Schema, validate


class ValidationError(Exception):
    """Raised when a record fails schema validation in strict mode."""


def run_pipeline(
    path: str,
    schema: Optional[Schema] = None,
    detector: Optional[Detector] = None,
    strict: bool = False,
) -> Iterator[AnomalyEvent]:
    """
    Run the full logdrift pipeline on a file.

    Args:
        path: Path to the log file.
        schema: Optional Schema to validate records against.
        detector: Optional Detector instance; a default one is created if None.
        strict: If True, raise ValidationError on schema violations instead of skipping.

    Yields:
        AnomalyEvent instances for each detected anomaly.
    """
    if detector is None:
        detector = Detector()

    for record in read_records(path):
        if schema is not None:
            errors: List[str] = validate(schema, record)
            if errors:
                if strict:
                    raise ValidationError(f"Record failed validation: {errors}")
                continue

        yield from detector.feed(record)


def summarise(events: List[AnomalyEvent]) -> dict:
    """Return a simple summary dict from a list of anomaly events."""
    by_field: dict = {}
    for event in events:
        key = event.field or "<unknown>"
        by_field.setdefault(key, [])
        by_field[key].append(event.reason)
    return {
        "total_anomalies": len(events),
        "by_field": {k: len(v) for k, v in by_field.items()},
    }
