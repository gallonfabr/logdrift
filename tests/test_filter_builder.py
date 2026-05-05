"""Tests for logdrift.filter_builder."""
import pytest

from logdrift.filter import FilterError
from logdrift.filter_builder import build_filter, _build_rule


class TestBuildRule:
    def test_eq_operator(self):
        rule = _build_rule({"field": "level", "op": "eq", "value": "ERROR"})
        assert rule.matches({"level": "ERROR"}) is True
        assert rule.matches({"level": "INFO"}) is False

    def test_neq_operator(self):
        rule = _build_rule({"field": "level", "op": "neq", "value": "DEBUG"})
        assert rule.matches({"level": "ERROR"}) is True

    def test_contains_operator(self):
        rule = _build_rule({"field": "msg", "op": "contains", "value": "timeout"})
        assert rule.matches({"msg": "connection timeout occurred"}) is True
        assert rule.matches({"msg": "all good"}) is False

    def test_regex_operator(self):
        rule = _build_rule({"field": "path", "op": "regex", "value": r"^/api/"})
        assert rule.matches({"path": "/api/v1/users"}) is True
        assert rule.matches({"path": "/health"}) is False

    def test_gt_operator(self):
        rule = _build_rule({"field": "latency", "op": "gt", "value": 500})
        assert rule.matches({"latency": 600}) is True
        assert rule.matches({"latency": 400}) is False

    def test_lt_operator(self):
        rule = _build_rule({"field": "latency", "op": "lt", "value": 100})
        assert rule.matches({"latency": 50}) is True

    def test_exists_operator(self):
        rule = _build_rule({"field": "trace_id", "op": "exists", "value": None})
        assert rule.matches({"trace_id": "abc"}) is True
        assert rule.matches({}) is False

    def test_missing_field_raises(self):
        with pytest.raises(FilterError, match="missing 'field'"):
            _build_rule({"op": "eq", "value": "x"})

    def test_unknown_op_raises(self):
        with pytest.raises(FilterError, match="unknown operator"):
            _build_rule({"field": "x", "op": "between", "value": 1})


class TestBuildFilter:
    def test_empty_specs_passes_all(self):
        f = build_filter([])
        assert f.apply({"level": "DEBUG"}) is True

    def test_multiple_specs_and_semantics(self):
        specs = [
            {"field": "level", "op": "eq", "value": "ERROR"},
            {"field": "service", "op": "eq", "value": "auth"},
        ]
        f = build_filter(specs)
        assert f.apply({"level": "ERROR", "service": "auth"}) is True
        assert f.apply({"level": "ERROR", "service": "web"}) is False

    def test_or_semantics(self):
        specs = [
            {"field": "level", "op": "eq", "value": "ERROR"},
            {"field": "level", "op": "eq", "value": "WARN"},
        ]
        f = build_filter(specs, require_all=False)
        assert f.apply({"level": "WARN"}) is True
        assert f.apply({"level": "INFO"}) is False

    def test_rules_count_matches_specs(self):
        specs = [
            {"field": "a", "op": "exists"},
            {"field": "b", "op": "exists"},
        ]
        f = build_filter(specs)
        assert len(f.rules) == 2
