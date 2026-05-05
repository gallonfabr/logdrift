"""Tests for logdrift.enricher."""
import pytest
from logdrift.enricher import Enricher, EnricherRule, regex_extract, static_field


# ---------------------------------------------------------------------------
# EnricherRule
# ---------------------------------------------------------------------------

class TestEnricherRule:
    def test_adds_derived_field(self):
        rule = EnricherRule(target_field="upper", fn=lambda r: r.get("msg", "").upper())
        record = {"msg": "hello"}
        rule.apply(record)
        assert record["upper"] == "HELLO"

    def test_does_not_overwrite_by_default(self):
        rule = EnricherRule(target_field="env", fn=lambda _: "prod")
        record = {"env": "staging"}
        rule.apply(record)
        assert record["env"] == "staging"

    def test_overwrites_when_flag_set(self):
        rule = EnricherRule(target_field="env", fn=lambda _: "prod", overwrite=True)
        record = {"env": "staging"}
        rule.apply(record)
        assert record["env"] == "prod"

    def test_none_result_skips_field(self):
        rule = EnricherRule(target_field="missing", fn=lambda _: None)
        record = {}
        rule.apply(record)
        assert "missing" not in record


# ---------------------------------------------------------------------------
# Enricher
# ---------------------------------------------------------------------------

class TestEnricher:
    def test_enrich_applies_all_rules(self):
        enricher = Enricher()
        enricher.add_rule(static_field("source", "logdrift"))
        enricher.add_rule(EnricherRule("level_upper", lambda r: r.get("level", "").upper()))
        record = {"level": "warn"}
        result = enricher.enrich(record)
        assert result["source"] == "logdrift"
        assert result["level_upper"] == "WARN"

    def test_enrich_all_returns_list(self):
        enricher = Enricher()
        enricher.add_rule(static_field("x", 1))
        records = [{"a": 1}, {"b": 2}]
        results = enricher.enrich_all(records)
        assert all(r["x"] == 1 for r in results)
        assert len(results) == 2

    def test_add_rule_returns_self(self):
        enricher = Enricher()
        result = enricher.add_rule(static_field("k", "v"))
        assert result is enricher


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------

def test_regex_extract_matches_group():
    rule = regex_extract("path", r"/users/(\d+)", "user_id")
    record = {"path": "/users/42/profile"}
    rule.apply(record)
    assert record["user_id"] == "42"


def test_regex_extract_missing_source_field():
    rule = regex_extract("path", r"/(\w+)", "first_segment")
    record = {}
    rule.apply(record)
    assert "first_segment" not in record


def test_regex_extract_no_match_skips_field():
    rule = regex_extract("msg", r"ERROR:(\w+)", "error_code")
    record = {"msg": "all good"}
    rule.apply(record)
    assert "error_code" not in record


def test_static_field_sets_value():
    rule = static_field("env", "test")
    record = {}
    rule.apply(record)
    assert record["env"] == "test"
