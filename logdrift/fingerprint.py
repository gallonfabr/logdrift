"""Log record fingerprinting for grouping structurally similar events."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


class FingerprintError(Exception):
    """Raised when fingerprint configuration or input is invalid."""


@dataclass
class FingerprintConfig:
    """Configuration for the Fingerprinter."""

    key_fields: List[str]
    hash_length: int = 8

    def __post_init__(self) -> None:
        if not self.key_fields:
            raise FingerprintError("key_fields must not be empty")
        if self.hash_length < 4 or self.hash_length > 64:
            raise FingerprintError("hash_length must be between 4 and 64")


@dataclass
class FingerprintResult:
    """Result of fingerprinting a single record."""

    fingerprint: str
    record: Dict

    def __repr__(self) -> str:  # pragma: no cover
        return f"FingerprintResult(fingerprint={self.fingerprint!r})"


class Fingerprinter:
    """Computes a short hash fingerprint from selected fields of a log record."""

    def __init__(self, config: FingerprintConfig) -> None:
        self._config = config
        self._counts: Dict[str, int] = {}

    def compute(self, record: Dict) -> FingerprintResult:
        """Return a FingerprintResult for *record*."""
        extracted = {
            k: record.get(k) for k in self._config.key_fields
        }
        raw = json.dumps(extracted, sort_keys=True, default=str)
        digest = hashlib.sha256(raw.encode()).hexdigest()[: self._config.hash_length]
        self._counts[digest] = self._counts.get(digest, 0) + 1
        return FingerprintResult(fingerprint=digest, record=record)

    def process(self, records: Iterable[Dict]) -> List[FingerprintResult]:
        """Fingerprint an iterable of records and return results."""
        return [self.compute(r) for r in records]

    def count(self, fingerprint: str) -> int:
        """Return how many times *fingerprint* has been seen."""
        return self._counts.get(fingerprint, 0)

    def top(self, n: int = 5) -> List[tuple]:
        """Return the *n* most common fingerprints as (fingerprint, count) pairs."""
        if n <= 0:
            raise FingerprintError("n must be a positive integer")
        sorted_items = sorted(self._counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:n]
