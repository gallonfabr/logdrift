"""Alert channels for logdrift anomaly notifications."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from logdrift.detector import AnomalyEvent


@dataclass
class AlertConfig:
    """Configuration for an alert channel."""

    channel: str  # 'stderr', 'stdout', 'json_file'
    path: Optional[str] = None  # used when channel == 'json_file'
    min_severity: float = 0.0  # only emit alerts with score >= this value


AlertHandler = Callable[[AnomalyEvent], None]


def _make_stderr_handler() -> AlertHandler:
    def _handle(event: AnomalyEvent) -> None:
        print(
            f"[logdrift] ANOMALY field={event.field!r} "
            f"value={event.value!r} score={event.score:.4f}",
            file=sys.stderr,
        )

    return _handle


def _make_stdout_handler() -> AlertHandler:
    def _handle(event: AnomalyEvent) -> None:
        print(
            f"[logdrift] ANOMALY field={event.field!r} "
            f"value={event.value!r} score={event.score:.4f}"
        )

    return _handle


def _make_json_file_handler(path: str) -> AlertHandler:
    def _handle(event: AnomalyEvent) -> None:
        record = {
            "field": event.field,
            "value": event.value,
            "score": event.score,
        }
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    return _handle


def build_handler(config: AlertConfig) -> AlertHandler:
    """Return an AlertHandler that respects *min_severity* filtering."""
    if config.channel == "stderr":
        base = _make_stderr_handler()
    elif config.channel == "stdout":
        base = _make_stdout_handler()
    elif config.channel == "json_file":
        if not config.path:
            raise ValueError("AlertConfig.path is required for 'json_file' channel")
        base = _make_json_file_handler(config.path)
    else:
        raise ValueError(f"Unknown alert channel: {config.channel!r}")

    threshold = config.min_severity

    def _filtered(event: AnomalyEvent) -> None:
        if event.score >= threshold:
            base(event)

    return _filtered


def dispatch(event: AnomalyEvent, handlers: List[AlertHandler]) -> None:
    """Send *event* to every registered handler."""
    for handler in handlers:
        handler(event)
