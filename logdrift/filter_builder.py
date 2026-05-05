"""Convenience factory for building Filter objects from plain config dicts."""
from __future__ import annotations

import re
from typing import Any, Dict, List

from logdrift.filter import Filter, FilterError, FilterRule

_OPERATORS: Dict[str, Any] = {
    "eq": lambda expected: (lambda v: v == expected),
    "neq": lambda expected: (lambda v: v != expected),
    "contains": lambda substr: (lambda v: isinstance(v, str) and substr in v),
    "regex": lambda pattern: (lambda v: isinstance(v, str) and bool(re.search(pattern, v))),
    "gt": lambda threshold: (lambda v: v is not None and v > threshold),
    "lt": lambda threshold: (lambda v: v is not None and v < threshold),
    "exists": lambda _: (lambda v: v is not None),
}


def _build_rule(spec: Dict[str, Any]) -> FilterRule:
    """Build a FilterRule from a specification dict.

    Expected keys:
      - ``field``  (str): name of the log record field
      - ``op``     (str): operator name, one of the keys in ``_OPERATORS``
      - ``value``  (Any, optional): operand passed to the operator factory
      - ``description`` (str, optional)
    """
    field_name = spec.get("field")
    if not field_name:
        raise FilterError("filter spec missing 'field'")

    op_name = spec.get("op")
    if op_name not in _OPERATORS:
        raise FilterError(
            f"unknown operator '{op_name}'; valid: {sorted(_OPERATORS)}"
        )

    factory = _OPERATORS[op_name]
    predicate = factory(spec.get("value"))
    return FilterRule(
        field_name=field_name,
        predicate=predicate,
        description=spec.get("description", ""),
    )


def build_filter(specs: List[Dict[str, Any]], require_all: bool = True) -> Filter:
    """Build a :class:`~logdrift.filter.Filter` from a list of spec dicts.

    Parameters
    ----------
    specs:
        List of rule specification dicts (see :func:`_build_rule`).
    require_all:
        ``True`` for AND semantics, ``False`` for OR semantics.
    """
    f = Filter(require_all=require_all)
    for spec in specs:
        f.add_rule(_build_rule(spec))
    return f
