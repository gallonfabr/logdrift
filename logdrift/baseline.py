"""Baseline persistence: save and load detector frequency baselines."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict


class BaselineError(Exception):
    """Raised when a baseline operation fails."""


@dataclass
class Baseline:
    """Stores per-field value frequency counts that can be persisted."""

    # {field_name: {value: count}}
    counts: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def record(self, field_name: str, value: str, count: int = 1) -> None:
        """Increment the frequency count for *value* under *field_name*."""
        bucket = self.counts.setdefault(field_name, {})
        bucket[value] = bucket.get(value, 0) + count

    def get_count(self, field_name: str, value: str) -> int:
        """Return the stored count for *value* under *field_name* (0 if absent)."""
        return self.counts.get(field_name, {}).get(value, 0)

    def total(self, field_name: str) -> int:
        """Return the total number of observations recorded for *field_name*."""
        return sum(self.counts.get(field_name, {}).values())

    def to_dict(self) -> dict:
        return {"counts": self.counts}

    @classmethod
    def from_dict(cls, data: dict) -> "Baseline":
        return cls(counts=data.get("counts", {}))


def save(baseline: Baseline, path: str) -> None:
    """Serialise *baseline* to a JSON file at *path*."""
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(baseline.to_dict(), fh, indent=2)
        os.replace(tmp, path)
    except OSError as exc:
        raise BaselineError(f"Cannot write baseline to {path!r}: {exc}") from exc


def load(path: str) -> Baseline:
    """Deserialise a *Baseline* from the JSON file at *path*."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise BaselineError(f"Baseline file not found: {path!r}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise BaselineError(f"Cannot read baseline from {path!r}: {exc}") from exc
    return Baseline.from_dict(data)
