"""Field enrichment: attach derived fields to log records before detection."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


EnrichFn = Callable[[Dict[str, Any]], Optional[Any]]


@dataclass
class EnricherRule:
    """A single enrichment rule: derive *target_field* from an existing record."""

    target_field: str
    fn: EnrichFn
    overwrite: bool = False

    def apply(self, record: Dict[str, Any]) -> Dict[str, Any]:
        if self.target_field in record and not self.overwrite:
            return record
        value = self.fn(record)
        if value is not None:
            record[self.target_field] = value
        return record


@dataclass
class Enricher:
    """Applies an ordered list of :class:`EnricherRule` objects to each record."""

    rules: List[EnricherRule] = field(default_factory=list)

    def add_rule(self, rule: EnricherRule) -> "Enricher":
        self.rules.append(rule)
        return self

    def enrich(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return *record* (mutated in-place) with all rules applied."""
        for rule in self.rules:
            rule.apply(record)
        return record

    def enrich_all(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.enrich(r) for r in records]


# ---------------------------------------------------------------------------
# Convenience factory functions
# ---------------------------------------------------------------------------

def regex_extract(source_field: str, pattern: str, target_field: str,
                  group: int = 1, overwrite: bool = False) -> EnricherRule:
    """Return a rule that extracts a regex group from *source_field*."""
    _re = re.compile(pattern)

    def _fn(record: Dict[str, Any]) -> Optional[str]:
        value = record.get(source_field)
        if not isinstance(value, str):
            return None
        m = _re.search(value)
        return m.group(group) if m else None

    return EnricherRule(target_field=target_field, fn=_fn, overwrite=overwrite)


def static_field(target_field: str, value: Any, overwrite: bool = False) -> EnricherRule:
    """Return a rule that sets *target_field* to a constant *value*."""
    return EnricherRule(target_field=target_field, fn=lambda _: value, overwrite=overwrite)
