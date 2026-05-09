"""Summarise topology (field co-occurrence) results for reporting."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from logdrift.topology import EdgeStats, Topology


@dataclass
class TopologyEdgeReport:
    field_a: str
    field_b: str
    a_value: str
    b_value: str
    count: int

    def to_dict(self) -> dict:
        return {
            "field_a": self.field_a,
            "field_b": self.field_b,
            "a_value": self.a_value,
            "b_value": self.b_value,
            "count": self.count,
        }


@dataclass
class TopologySummary:
    label: str
    edges: List[TopologyEdgeReport] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "edges": [e.to_dict() for e in self.edges],
        }

    def to_text(self) -> str:
        lines = [f"Topology [{self.label}]"]
        if not self.edges:
            lines.append("  (no edges above support threshold)")
        else:
            for e in sorted(self.edges, key=lambda x: -x.count):
                lines.append(
                    f"  {e.a_value!r} <-> {e.b_value!r}  count={e.count}"
                )
        return "\n".join(lines)


def build_topology_summary(
    topology: Topology,
    min_support: int | None = None,
) -> TopologySummary:
    """Build a :class:`TopologySummary` from a trained :class:`Topology`."""
    fa = topology.config.fields[0]
    fb = topology.config.fields[1]
    label = f"{fa}:{fb}"
    edges = topology.edges(min_support=min_support)
    reports = [
        TopologyEdgeReport(
            field_a=fa,
            field_b=fb,
            a_value=e.a_value,
            b_value=e.b_value,
            count=e.count,
        )
        for e in edges
    ]
    return TopologySummary(label=label, edges=reports)
