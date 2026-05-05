"""High-level pipeline: parse → validate → sample → detect → aggregate."""
from __future__ import annotations

from typing import Dict, Iterator, List, Optional

from logdrift.aggregator import Aggregator
from logdrift.alerts import AlertConfig, dispatch
from logdrift.detector import Detector
from logdrift.parser import ParseError, parse_line
from logdrift.sampler import Sampler, SamplerConfig
from logdrift.schema import Schema, validate


class ValidationError(Exception):
    """Raised when a record fails schema validation inside the pipeline."""


def run_pipeline(
    lines: Iterator[str],
    schema: Optional[Schema] = None,
    detector: Optional[Detector] = None,
    aggregator: Optional[Aggregator] = None,
    alert_config: Optional[AlertConfig] = None,
    sampler_config: Optional[SamplerConfig] = None,
    skip_invalid: bool = True,
) -> List[Dict]:
    """Process *lines* through the full logdrift pipeline.

    Returns the list of records that were kept after sampling.
    """
    sampler = Sampler(sampler_config or SamplerConfig())
    anomalies: List[Dict] = []
    alert_handler = dispatch(alert_config) if alert_config else None

    for raw in lines:
        # 1. parse
        try:
            record = parse_line(raw)
        except ParseError:
            continue

        # 2. validate
        if schema is not None:
            errors = validate(schema, record)
            if errors:
                if not skip_invalid:
                    raise ValidationError(errors)
                continue

        # 3. sample
        if not sampler.should_keep(record):
            continue

        # 4. detect
        if detector is not None:
            for field, value in record.items():
                event = detector.observe(field, str(value))
                if event is not None:
                    anomalies.append(event)
                    if alert_handler is not None:
                        alert_handler(event)

        # 5. aggregate
        if aggregator is not None:
            aggregator.add(record)

    return anomalies


def summarise(anomalies: List[Dict]) -> str:
    """Return a brief human-readable summary of detected anomalies."""
    if not anomalies:
        return "No anomalies detected."
    lines = [f"Anomalies detected: {len(anomalies)}"]
    for ev in anomalies:
        lines.append(f"  field={ev.field!r} value={ev.value!r} score={ev.score:.3f}")
    return "\n".join(lines)
