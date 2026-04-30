"""Expected-field schema validation for parsed log records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FieldSpec:
    """Specification for a single expected field."""

    name: str
    required: bool = True
    expected_type: type | None = None


@dataclass
class Schema:
    """Collection of :class:`FieldSpec` objects describing expected log fields."""

    fields: list[FieldSpec] = field(default_factory=list)

    def add(self, name: str, *, required: bool = True, expected_type: type | None = None) -> None:
        self.fields.append(FieldSpec(name=name, required=required, expected_type=expected_type))

    def validate(self, record: dict[str, Any]) -> list[str]:
        """Return a list of violation messages (empty if the record is valid)."""
        violations: list[str] = []
        for spec in self.fields:
            if spec.name not in record:
                if spec.required:
                    violations.append(f"Missing required field '{spec.name}'")
                continue
            if spec.expected_type is not None:
                value = record[spec.name]
                if not isinstance(value, spec.expected_type):
                    violations.append(
                        f"Field '{spec.name}' expected {spec.expected_type.__name__}, "
                        f"got {type(value).__name__}"
                    )
        return violations
