"""Helpers for building and querying Topology instances from config dicts."""
from __future__ import annotations

from typing import Dict, Iterable, List

from logdrift.topology import EdgeStats, Topology, TopologyConfig


def build_topology(config: dict) -> Topology:
    """Create a :class:`Topology` from a plain configuration dictionary.

    Expected keys:
        - ``fields`` (list[str], required)
        - ``min_support`` (int, optional, default 5)
    """
    cfg = TopologyConfig(
        fields=config["fields"],
        min_support=config.get("min_support", 5),
    )
    return Topology(cfg)


def build_topologies(configs: Iterable[dict]) -> List[Topology]:
    """Build multiple :class:`Topology` instances from an iterable of dicts."""
    return [build_topology(c) for c in configs]


def observe_all(topologies: Iterable[Topology], record: dict) -> None:
    """Push *record* through every topology in *topologies*."""
    for topo in topologies:
        topo.observe(record)


def rare_edges_for_record(
    topologies: Iterable[Topology], record: dict
) -> Dict[str, List[EdgeStats]]:
    """Return a mapping of field-pair label -> rare edges for *record*.

    The label is ``"<field_a>:<field_b>"``.
    """
    result: Dict[str, List[EdgeStats]] = {}
    for topo in topologies:
        rare = topo.rare_edges(record)
        if rare:
            label = f"{topo.config.fields[0]}:{topo.config.fields[1]}"
            result[label] = rare
    return result
