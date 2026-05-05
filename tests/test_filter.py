"""Tests for logdrift.filter."""
import pytest

from logdrift.filter import Filter, FilterError, FilterRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rule(field_name: str, predicate, description: str = "") -> FilterRule:
    return FilterRule(field_name=field_name, predicate=predicate, description=description)


# ---------------------------------------------------------------------------
# FilterRule
# ---------------------------------------------------------------------------

class TestFilterRule:
    def test_matches_when_predicate_true(self):
        rule = _rule("level", lambda v: v == "ERROR")
        assert rule.matches({"level": "ERROR"}) is True

    def test_no_match_when_predicate_false(self):
        rule = _rule("level", lambda v: v == "ERROR")
        assert rule.matches({"level": "INFO"}) is False

    def test_missing_field_passes_none_to_predicate(self):
        rule = _rule("missing", lambda v: v is None)
        assert rule.matches({}) is True


# ---------------------------------------------------------------------------
# Filter – AND semantics
# ---------------------------------------------------------------------------

class TestFilterAnd:
    def test_no_rules_passes_everything(self):
        f = Filter()
        assert f.apply({"level": "DEBUG"}) is True

    def test_single_rule_pass(self):
        f = Filter()
        f.add_rule(_rule("level", lambda v: v == "ERROR"))
        assert f.apply({"level": "ERROR"}) is True

    def test_single_rule_fail(self):
        f = Filter()
        f.add_rule(_rule("level", lambda v: v == "ERROR"))
        assert f.apply({"level": "INFO"}) is False

    def test_all_rules_must_pass(self):
        f = Filter()
        f.add_rule(_rule("level", lambda v: v == "ERROR"))
        f.add_rule(_rule("service", lambda v: v == "auth"))
        assert f.apply({"level": "ERROR", "service": "auth"}) is True
        assert f.apply({"level": "ERROR", "service": "web"}) is False

    def test_non_callable_predicate_raises(self):
        with pytest.raises(FilterError):
            f = Filter()
            f.add_rule(FilterRule(field_name="x", predicate="not_callable"))  # type: ignore


# ---------------------------------------------------------------------------
# Filter – OR semantics
# ---------------------------------------------------------------------------

class TestFilterOr:
    def test_any_rule_sufficient(self):
        f = Filter(require_all=False)
        f.add_rule(_rule("level", lambda v: v == "ERROR"))
        f.add_rule(_rule("level", lambda v: v == "WARN"))
        assert f.apply({"level": "WARN"}) is True
        assert f.apply({"level": "DEBUG"}) is False


# ---------------------------------------------------------------------------
# filter_records helper
# ---------------------------------------------------------------------------

def test_filter_records_returns_matching_subset():
    f = Filter()
    f.add_rule(_rule("level", lambda v: v == "ERROR"))
    records = [{"level": "INFO"}, {"level": "ERROR"}, {"level": "ERROR"}]
    result = f.filter_records(records)
    assert len(result) == 2
    assert all(r["level"] == "ERROR" for r in result)


def test_rules_property_is_copy():
    f = Filter()
    f.add_rule(_rule("x", lambda v: True))
    copy = f.rules
    copy.clear()
    assert len(f.rules) == 1
