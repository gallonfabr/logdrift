"""Alert emission for anomaly events."""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Callable, IO, List, Optional

from logdrift.detector import AnomalyEvent
from logdrift.throttle import Throttle, ThrottleConfig


@dataclass
class AlertConfig:
    """Configuration for the alert system."""

    min_score: float = 0.0
    destination: str = "stderr"  # "stderr" | "stdout"
    throttle: Optional[ThrottleConfig] = None


def _make_stderr_handler(config: AlertConfig) -> Callable[[AnomalyEvent], None]:
    return _make_stream_handler(config, sys.stderr)


def _make_stdout_handler(config: AlertConfig) -> Callable[[AnomalyEvent], None]:
    return _make_stream_handler(config, sys.stdout)


def _make_stream_handler(
    config: AlertConfig, stream: IO[str]
) -> Callable[[AnomalyEvent], None]:
    throttle = Throttle(config.throttle) if config.throttle else None

    def _handle(event: AnomalyEvent) -> None:
        if event.score < config.min_score:
            return
        if throttle and not throttle.allow(event.field, str(event.value)):
            return
        payload = {
            "field": event.field,
            "value": event.value,
            "score": round(event.score, 6),
        }
        stream.write(json.dumps(payload) + "\n")
        stream.flush()

    return _handle


def build_handler(config: AlertConfig) -> Callable[[AnomalyEvent], None]:
    """Return an alert handler based on *config*."""
    if config.destination == "stdout":
        return _make_stdout_handler(config)
    return _make_stderr_handler(config)


def emit_alerts(
    events: List[AnomalyEvent],
    config: Optional[AlertConfig] = None,
) -> None:
    """Emit *events* using a handler built from *config*."""
    cfg = config or AlertConfig()
    handler = build_handler(cfg)
    for event in events:
        handler(event)
