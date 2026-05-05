"""Field-based record filtering for logdrift pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class FilterError(Exception):
    """Raised when a filter rule is misconfigured."""


@dataclass
class FilterRule:
    """A single predicate applied to one field of a log record."""

    field_name: str
    predicate: Callable[[Any], bool]
    description: str = ""

    def matches(self, record: Dict[str, Any]) -> bool:
        """Return True when the record satisfies this rule."""
        value = record.get(self.field_name)
        return self.predicate(value)


@dataclass
class Filter:
    """Applies a collection of FilterRules to log records.

    A record passes when *all* rules match (AND semantics by default).
    Set ``require_all=False`` for OR semantics.
    """

    require_all: bool = True
    _rules: List[FilterRule] = field(default_factory=list, init=False, repr=False)

    def add_rule(self, rule: FilterRule) -> None:
        """Register a new rule."""
        if not callable(rule.predicate):
            raise FilterError("predicate must be callable")
        self._rules.append(rule)

    def apply(self, record: Dict[str, Any]) -> bool:
        """Return True if the record passes the filter."""
        if not self._rules:
            return True
        results = (r.matches(record) for r in self._rules)
        return all(results) if self.require_all else any(results)

    def filter_records(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Return only the records that pass the filter."""
        return [rec for rec in records if self.apply(rec)]

    @property
    def rules(self) -> List[FilterRule]:
        """Read-only view of registered rules."""
        return list(self._rules)
