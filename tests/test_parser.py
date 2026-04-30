"""Unit tests for logdrift.parser."""

import pytest

from logdrift.parser import ParseError, parse_json, parse_kv, parse_line


class TestParseJson:
    def test_simple_object(self):
        result = parse_json('{"level": "info", "msg": "started"}')
        assert result == {"level": "info", "msg": "started"}

    def test_strips_whitespace(self):
        assert parse_json('  {"k": 1}  ') == {"k": 1}

    def test_invalid_json_raises(self):
        with pytest.raises(ParseError):
            parse_json("not json")

    def test_non_object_raises(self):
        with pytest.raises(ParseError, match="Expected a JSON object"):
            parse_json('["a", "b"]')


class TestParseKv:
    def test_simple_pairs(self):
        result = parse_kv('level=info msg=started')
        assert result == {"level": "info", "msg": "started"}

    def test_quoted_values(self):
        result = parse_kv('msg="hello world" level=warn')
        assert result["msg"] == "hello world"

    def test_empty_line_raises(self):
        with pytest.raises(ParseError):
            parse_kv("   ")


class TestParseLine:
    def test_dispatches_json(self):
        assert parse_line('{"a": 1}') == {"a": 1}

    def test_dispatches_kv(self):
        assert parse_line('x=1 y=2') == {"x": "1", "y": "2"}
