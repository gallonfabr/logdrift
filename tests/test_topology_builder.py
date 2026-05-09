"""Tests for logdrift.topology_builder."""
import pytest

from logdrift.topology import Topology, TopologyError
from logdrift.topology_builder import (
    build_topologies,
    build_topology,
    observe_all,
    rare_edges_for_record,
)


class TestBuildTopology:
    def test_returns_topology_instance(self):
        t = build_topology({"fields": ["env", "service"]})
        assert isinstance(t, Topology)

    def test_default_min_support(self):
        t = build_topology({"fields": ["env", "service"]})
        assert t.config.min_support == 5

    def test_custom_min_support(self):
        t = build_topology({"fields": ["env", "service"], "min_support": 2})
        assert t.config.min_support == 2

    def test_invalid_config_propagates(self):
        with pytest.raises(TopologyError):
            build_topology({"fields": ["only_one"]})


class TestBuildTopologies:
    def test_builds_multiple(self):
        configs = [
            {"fields": ["env", "service"]},
            {"fields": ["host", "level"]},
        ]
        topos = build_topologies(configs)
        assert len(topos) == 2
        assert all(isinstance(t, Topology) for t in topos)

    def test_empty_list(self):
        assert build_topologies([]) == []


class TestObserveAll:
    def test_pushes_to_all_topologies(self):
        t1 = build_topology({"fields": ["env", "service"], "min_support": 1})
        t2 = build_topology({"fields": ["host", "level"], "min_support": 1})
        record = {"env": "prod", "service": "api", "host": "h1", "level": "INFO"}
        observe_all([t1, t2], record)
        assert len(t1.edges()) == 1
        assert len(t2.edges()) == 1


class TestRareEdgesForRecord:
    def test_returns_rare_edges_with_label(self):
        t = build_topology({"fields": ["env", "service"], "min_support": 10})
        t.observe({"env": "prod", "service": "api"})
        record = {"env": "prod", "service": "api"}
        result = rare_edges_for_record([t], record)
        assert "env:service" in result
        assert len(result["env:service"]) == 1

    def test_no_rare_edges_excluded(self):
        t = build_topology({"fields": ["env", "service"], "min_support": 1})
        for _ in range(3):
            t.observe({"env": "prod", "service": "api"})
        record = {"env": "prod", "service": "api"}
        result = rare_edges_for_record([t], record)
        assert result == {}

    def test_empty_topologies(self):
        assert rare_edges_for_record([], {"env": "prod"}) == {}
