"""Text and dict reporting for cardinality anomalies."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from logdrift.cardinality import CardinalityAnomaly


@dataclass
class CardinalitySummary:
    field: str
    distinct_count: int
    max_distinct: int
    sample_values: List[str]
    timestamp: float

    def to_dict(self) -> Dict:
        return {
            "field": self.field,
            "distinct_count": self.distinct_count,
            "max_distinct": self.max_distinct,
            "sample_values": self.sample_values,
            "timestamp": self.timestamp,
        }

    def to_text(self) -> str:
        samples = ", ".join(self.sample_values[:5])
        return (
            f"[CARDINALITY] field={self.field!r} "
            f"distinct={self.distinct_count} (max={self.max_distinct}) "
            f"samples=[{samples}]"
        )


def build_summary(anomaly: CardinalityAnomaly) -> CardinalitySummary:
    return CardinalitySummary(
        field=anomaly.field,
        distinct_count=anomaly.distinct_count,
        max_distinct=anomaly.max_distinct,
        sample_values=anomaly.sample_values,
        timestamp=anomaly.timestamp,
    )


def build_summaries(anomalies: List[CardinalityAnomaly]) -> List[CardinalitySummary]:
    return [build_summary(a) for a in anomalies]


def summaries_to_text(summaries: List[CardinalitySummary]) -> str:
    if not summaries:
        return "No cardinality anomalies detected."
    lines = ["=== Cardinality Anomalies ==="]
    for s in sorted(summaries, key=lambda x: x.distinct_count, reverse=True):
        lines.append(s.to_text())
    return "\n".join(lines)
