"""Human-readable and JSON-serialisable reports from Aggregator window stats."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Dict, List

from logdrift.aggregator import Aggregator, WindowStats


@dataclass
class FieldReport:
    field: str
    anomaly_count: int
    max_score: float
    top_values: List[str]


@dataclass
class Report:
    total_anomalies: int
    window_seconds: float
    fields: List[FieldReport]

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def to_text(self) -> str:
        lines = [
            f"Anomaly Report  (window={self.window_seconds}s,"
            f" total={self.total_anomalies})",
            "-" * 50,
        ]
        for fr in sorted(self.fields, key=lambda f: f.max_score, reverse=True):
            top = ", ".join(fr.top_values[:5]) or "—"
            lines.append(
                f"  {fr.field:<20} count={fr.anomaly_count:<4}"
                f" max_score={fr.max_score:.3f}  values=[{top}]"
            )
        return "\n".join(lines)


def build_report(aggregator: Aggregator) -> Report:
    """Snapshot the aggregator and produce a Report."""
    stats: Dict[str, WindowStats] = aggregator.stats()
    field_reports = [
        FieldReport(
            field=ws.field_name,
            anomaly_count=ws.count,
            max_score=ws.max_score,
            top_values=_top_values(ws.values),
        )
        for ws in stats.values()
    ]
    return Report(
        total_anomalies=sum(fr.anomaly_count for fr in field_reports),
        window_seconds=aggregator.window_seconds,
        fields=field_reports,
    )


def _top_values(values: List[str], n: int = 5) -> List[str]:
    """Return the *n* most-frequent values, preserving insertion order on ties."""
    counts: Dict[str, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    return [v for v, _ in sorted(counts.items(), key=lambda x: -x[1])][:n]
