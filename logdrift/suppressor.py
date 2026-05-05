"""Suppressor: skip anomaly events whose fields/values match a known-safe list."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Set

from logdrift.detector import AnomalyEvent


class SuppressorError(Exception):
    """Raised when the suppressor is misconfigured."""


@dataclass
class Suppressor:
    """Filter out AnomalyEvents whose (field, value) pair is suppressed.

    Example::

        s = Suppressor()
        s.add("level", "DEBUG")
        s.add("level", "INFO")
        kept = [e for e in events if not s.is_suppressed(e)]
    """

    _rules: Dict[str, Set[str]] = field(default_factory=dict, init=False, repr=False)

    def add(self, field_name: str, value: str) -> None:
        """Register *value* for *field_name* as a suppressed (safe) combination."""
        if not field_name:
            raise SuppressorError("field_name must be a non-empty string")
        self._rules.setdefault(field_name, set()).add(str(value))

    def remove(self, field_name: str, value: str) -> None:
        """Remove a previously registered suppression rule (no-op if absent)."""
        if field_name in self._rules:
            self._rules[field_name].discard(str(value))

    def is_suppressed(self, event: AnomalyEvent) -> bool:
        """Return True if *event* matches any suppression rule."""
        allowed = self._rules.get(event.field_name)
        if allowed is None:
            return False
        return str(event.value) in allowed

    def filter(self, events: Iterable[AnomalyEvent]) -> Iterable[AnomalyEvent]:
        """Yield only events that are *not* suppressed."""
        for event in events:
            if not self.is_suppressed(event):
                yield event

    def rule_count(self) -> int:
        """Return total number of (field, value) suppression pairs registered."""
        return sum(len(v) for v in self._rules.values())

    def __repr__(self) -> str:  # pragma: no cover
        return f"Suppressor(rules={dict(self._rules)})"
