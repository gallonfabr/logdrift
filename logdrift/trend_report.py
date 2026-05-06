"""Summarise TrendAnomaly results into a human-readable or dict form."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from logdrift.trend import TrendAnomaly


@dataclass
class TrendSummary:
    """Aggregated view of trend anomalies grouped by field and value."""
    entries: List[Dict] = field(default_factory=list)

    def to_dict(self) -> List[Dict]:
        return list(self.entries)

    def to_text(self) -> str:
        if not self.entries:
            return "No trend anomalies detected.\n"
        lines = ["Trend Anomalies", "=" * 40]
        for e in sorted(self.entries, key=lambda x: -x["spike_factor"]):
            lines.append(
                f"  field={e['field']!r} value={e['value']!r} "
                f"spike={e['spike_factor']:.2f}x "
                f"(prev={e['previous_rate']:.3f}/s curr={e['current_rate']:.3f}/s)"
            )
        lines.append("")
        return "\n".join(lines)


def build_trend_summary(anomalies: List[TrendAnomaly]) -> TrendSummary:
    """Convert a list of TrendAnomaly objects into a TrendSummary."""
    seen: Dict[tuple, Dict] = {}
    for a in anomalies:
        key = (a.field, a.value)
        if key not in seen or a.spike_factor > seen[key]["spike_factor"]:
            seen[key] = {
                "field": a.field,
                "value": a.value,
                "previous_rate": a.previous_rate,
                "current_rate": a.current_rate,
                "spike_factor": a.spike_factor,
            }
    return TrendSummary(entries=list(seen.values()))
