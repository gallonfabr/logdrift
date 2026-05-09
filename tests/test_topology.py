"""Tests for logdrift.topology."""
import pytest

from logdrift.topology import EdgeStats, Topology, TopologyConfig, TopologyError


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestTopologyConfig:
    def test_valid_config_created(self):
        cfg = TopologyConfig(fields=["env", "service"])
        assert cfg.fields == ["env", "service"]
        assert cfg.min_support == 5

    def test_single_field_raises(self):
        with pytest.raises(TopologyError, match="at least two"):
            TopologyConfig(fields=["env"])

    def test_empty_field_name_raises(self):
        with pytest.raises(TopologyError, match="non-empty"):
            TopologyConfig(fields=["env", ""])

    def test_min_support_zero_raises(self):
        with pytest.raises(TopologyError, match="min_support"):
            TopologyConfig(fields=["a", "b"], min_support=0)

    def test_custom_min_support(self):
        cfg = TopologyConfig(fields=["a", "b"], min_support=10)
        assert cfg.min_support == 10


# ---------------------------------------------------------------------------
# Topology observe / edges
# ---------------------------------------------------------------------------

def _topo(min_support: int = 3) -> Topology:
    return Topology(TopologyConfig(fields=["env", "service"], min_support=min_support))


class TestTopology:
    def test_observe_increments_edge(self):
        t = _topo(min_support=1)
        t.observe({"env": "prod", "service": "api"})
        edges = t.edges()
        assert len(edges) == 1
        assert edges[0].count == 1

    def test_missing_field_skipped(self):
        t = _topo()
        t.observe({"env": "prod"})  # no 'service'
        assert t.edges(min_support=1) == []

    def test_below_min_support_excluded(self):
        t = _topo(min_support=3)
        for _ in range(2):
            t.observe({"env": "prod", "service": "api"})
        assert t.edges() == []

    def test_meets_min_support_included(self):
        t = _topo(min_support=2)
        for _ in range(2):
            t.observe({"env": "prod", "service": "api"})
        edges = t.edges()
        assert len(edges) == 1
        assert edges[0].count == 2

    def test_multiple_edges_tracked(self):
        t = _topo(min_support=1)
        t.observe({"env": "prod", "service": "api"})
        t.observe({"env": "staging", "service": "worker"})
        assert len(t.edges()) == 2

    def test_rare_edges_returns_unseen_pair(self):
        t = _topo(min_support=5)
        t.observe({"env": "prod", "service": "api"})
        rare = t.rare_edges({"env": "prod", "service": "api"})
        assert len(rare) == 1
        assert rare[0].count == 1

    def test_rare_edges_empty_when_above_support(self):
        t = _topo(min_support=2)
        for _ in range(3):
            t.observe({"env": "prod", "service": "api"})
        assert t.rare_edges({"env": "prod", "service": "api"}) == []

    def test_rare_edges_empty_for_missing_fields(self):
        t = _topo()
        assert t.rare_edges({"env": "prod"}) == []
