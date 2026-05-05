"""Rate-based log sampler: keeps every N-th record or those matching a filter."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, Optional


class SamplerError(Exception):
    """Raised when sampler is misconfigured."""


@dataclass
class SamplerConfig:
    """Configuration for the log sampler."""
    rate: int = 1  # keep every N-th record (1 = keep all)
    always_include: Optional[Callable[[Dict], bool]] = None  # predicate for forced inclusion

    def __post_init__(self) -> None:
        if self.rate < 1:
            raise SamplerError(f"rate must be >= 1, got {self.rate}")


class Sampler:
    """Stateful sampler that sub-samples a stream of log records."""

    def __init__(self, config: SamplerConfig) -> None:
        self._config = config
        self._seen: int = 0
        self._kept: int = 0

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def should_keep(self, record: Dict) -> bool:
        """Return True if *record* should be passed downstream."""
        self._seen += 1
        forced = (
            self._config.always_include is not None
            and self._config.always_include(record)
        )
        if forced or (self._seen % self._config.rate == 0):
            self._kept += 1
            return True
        return False

    def filter(self, records: Iterator[Dict]) -> Iterator[Dict]:
        """Yield only the records that pass the sampling criteria."""
        for record in records:
            if self.should_keep(record):
                yield record

    @property
    def stats(self) -> Dict[str, int]:
        """Return a snapshot of seen / kept counters."""
        return {"seen": self._seen, "kept": self._kept}

    def reset(self) -> None:
        """Reset internal counters (useful between pipeline runs)."""
        self._seen = 0
        self._kept = 0
