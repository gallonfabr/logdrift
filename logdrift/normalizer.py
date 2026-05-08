"""Field value normalizer — applies transformations to record fields before detection."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class NormalizerError(Exception):
    """Raised when a normalizer rule is misconfigured."""


@dataclass
class NormalizerRule:
    """A single transformation applied to one field of a record."""

    field: str
    transform: Callable[[Any], Any]
    output_field: Optional[str] = None  # if None, overwrites the source field

    def __post_init__(self) -> None:
        if not self.field:
            raise NormalizerError("field must not be empty")
        if not callable(self.transform):
            raise NormalizerError("transform must be callable")

    def apply(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of *record* with the transformation applied."""
        if self.field not in record:
            return record
        result = dict(record)
        transformed = self.transform(record[self.field])
        target = self.output_field if self.output_field else self.field
        result[target] = transformed
        return result


@dataclass
class Normalizer:
    """Applies an ordered list of :class:`NormalizerRule` objects to records."""

    _rules: List[NormalizerRule] = field(default_factory=list, init=False)

    def add_rule(self, rule: NormalizerRule) -> None:
        """Append *rule* to the normalizer pipeline."""
        if not isinstance(rule, NormalizerRule):
            raise NormalizerError("rule must be a NormalizerRule instance")
        self._rules.append(rule)

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all rules in order and return the transformed record."""
        for rule in self._rules:
            record = rule.apply(record)
        return record

    def normalize_all(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Normalize every record in *records*."""
        return [self.normalize(r) for r in records]

    def __repr__(self) -> str:  # pragma: no cover
        return f"Normalizer(rules={len(self._rules)})"
