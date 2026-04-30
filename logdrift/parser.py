"""Structured log parser for logdrift.

Supports JSON logs and common key=value formats.
"""

from __future__ import annotations

import json
import re
from typing import Any

_KV_PATTERN = re.compile(r'(\w+)=("[^"]*"|\S+)')


class ParseError(ValueError):
    """Raised when a log line cannot be parsed."""


def parse_json(line: str) -> dict[str, Any]:
    """Parse a JSON-encoded log line."""
    try:
        result = json.loads(line.strip())
    except json.JSONDecodeError as exc:
        raise ParseError(f"Invalid JSON: {exc}") from exc
    if not isinstance(result, dict):
        raise ParseError("Expected a JSON object at the top level")
    return result


def parse_kv(line: str) -> dict[str, Any]:
    """Parse a key=value encoded log line."""
    result: dict[str, Any] = {}
    for key, value in _KV_PATTERN.findall(line):
        result[key] = value.strip('"')
    if not result:
        raise ParseError(f"No key=value pairs found in: {line!r}")
    return result


def parse_line(line: str) -> dict[str, Any]:
    """Auto-detect format and parse a single log line."""
    stripped = line.strip()
    if stripped.startswith("{"):
        return parse_json(stripped)
    return parse_kv(stripped)
