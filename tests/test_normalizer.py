"""Tests for logdrift.normalizer and logdrift.normalizer_builder."""
from __future__ import annotations

import pytest

from logdrift.normalizer import Normalizer, NormalizerError, NormalizerRule
from logdrift.normalizer_builder import build_normalizer, build_rule


# ---------------------------------------------------------------------------
# NormalizerRule
# ---------------------------------------------------------------------------


class TestNormalizerRule:
    def test_applies_transform(self):
        rule = NormalizerRule(field="level", transform=str.upper)
        result = rule.apply({"level": "info", "msg": "ok"})
        assert result["level"] == "INFO"
        assert result["msg"] == "ok"

    def test_output_field_does_not_overwrite_source(self):
        rule = NormalizerRule(field="level", transform=str.upper, output_field="level_norm")
        result = rule.apply({"level": "warn"})
        assert result["level"] == "warn"
        assert result["level_norm"] == "WARN"

    def test_missing_field_leaves_record_unchanged(self):
        rule = NormalizerRule(field="missing", transform=str.upper)
        record = {"level": "info"}
        assert rule.apply(record) == record

    def test_empty_field_raises(self):
        with pytest.raises(NormalizerError, match="field must not be empty"):
            NormalizerRule(field="", transform=str.upper)

    def test_non_callable_transform_raises(self):
        with pytest.raises(NormalizerError, match="transform must be callable"):
            NormalizerRule(field="level", transform="upper")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


class TestNormalizer:
    def test_normalize_applies_rules_in_order(self):
        n = Normalizer()
        n.add_rule(NormalizerRule(field="msg", transform=str.strip))
        n.add_rule(NormalizerRule(field="msg", transform=str.lower))
        result = n.normalize({"msg": "  HELLO  "})
        assert result["msg"] == "hello"

    def test_normalize_all_processes_every_record(self):
        n = Normalizer()
        n.add_rule(NormalizerRule(field="level", transform=str.upper))
        records = [{"level": "info"}, {"level": "warn"}]
        results = n.normalize_all(records)
        assert [r["level"] for r in results] == ["INFO", "WARN"]

    def test_add_rule_rejects_non_rule(self):
        n = Normalizer()
        with pytest.raises(NormalizerError, match="NormalizerRule instance"):
            n.add_rule("not-a-rule")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# normalizer_builder
# ---------------------------------------------------------------------------


class TestBuildRule:
    def test_builtin_lower(self):
        rule = build_rule({"field": "level", "transform": "lower"})
        assert rule.apply({"level": "INFO"})["level"] == "info"

    def test_builtin_int(self):
        rule = build_rule({"field": "code", "transform": "int"})
        assert rule.apply({"code": "42"})["code"] == 42

    def test_callable_transform_accepted(self):
        rule = build_rule({"field": "x", "transform": lambda v: v * 2})
        assert rule.apply({"x": 5})["x"] == 10

    def test_unknown_builtin_raises(self):
        with pytest.raises(NormalizerError, match="unknown built-in transform"):
            build_rule({"field": "x", "transform": "nonexistent"})

    def test_output_field_forwarded(self):
        rule = build_rule({"field": "level", "transform": "upper", "output_field": "level_up"})
        result = rule.apply({"level": "debug"})
        assert result["level_up"] == "DEBUG"
        assert result["level"] == "debug"


class TestBuildNormalizer:
    def test_builds_normalizer_with_multiple_rules(self):
        configs = [
            {"field": "level", "transform": "lower"},
            {"field": "msg", "transform": "strip"},
        ]
        n = build_normalizer(configs)
        result = n.normalize({"level": "WARN", "msg": "  hello  "})
        assert result["level"] == "warn"
        assert result["msg"] == "hello"

    def test_empty_config_list_returns_identity_normalizer(self):
        n = build_normalizer([])
        record = {"level": "INFO"}
        assert n.normalize(record) == record
