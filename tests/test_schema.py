"""Unit tests for logdrift.schema."""

import pytest

from logdrift.schema import FieldSpec, Schema


@pytest.fixture()
def basic_schema() -> Schema:
    s = Schema()
    s.add("level", required=True, expected_type=str)
    s.add("msg", required=True)
    s.add("request_id", required=False)
    return s


class TestSchema:
    def test_valid_record(self, basic_schema):
        assert basic_schema.validate({"level": "info", "msg": "ok"}) == []

    def test_missing_required_field(self, basic_schema):
        violations = basic_schema.validate({"level": "info"})
        assert any("msg" in v for v in violations)

    def test_wrong_type(self, basic_schema):
        violations = basic_schema.validate({"level": 42, "msg": "hi"})
        assert any("level" in v and "str" in v for v in violations)

    def test_optional_field_absent_is_ok(self, basic_schema):
        assert basic_schema.validate({"level": "debug", "msg": "x"}) == []

    def test_multiple_violations(self, basic_schema):
        violations = basic_schema.validate({})
        assert len(violations) >= 2

    def test_field_spec_defaults(self):
        spec = FieldSpec(name="foo")
        assert spec.required is True
        assert spec.expected_type is None
