"""Helpers for building PatternCounters from plain config dicts and
running them over iterables of records."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from logdrift.pattern_counter import PatternCounter, PatternCounterConfig, PatternHit


def build_counter(config: dict) -> PatternCounter:
    """Build a :class:`PatternCounter` from a plain dict.

    Expected keys:
      - ``fields`` (list[str], required)
      - ``window_seconds`` (float, optional, default 60)
      - ``min_count`` (int, optional, default 2)
    """
    cfg = PatternCounterConfig(
        fields=config["fields"],
        window_seconds=float(config.get("window_seconds", 60.0)),
        min_count=int(config.get("min_count", 2)),
    )
    return PatternCounter(cfg)


def build_counters(configs: List[dict]) -> List[PatternCounter]:
    """Build multiple counters from a list of config dicts."""
    return [build_counter(c) for c in configs]


def observe_all(
    counters: List[PatternCounter],
    record: dict,
    now: Optional[float] = None,
) -> List[PatternHit]:
    """Feed *record* into every counter and return all resulting hits."""
    return [c.observe(record, now=now) for c in counters]


def rare_hits_for_record(
    counters: List[PatternCounter],
    record: dict,
    now: Optional[float] = None,
) -> List[PatternHit]:
    """Return only the :class:`PatternHit` objects where ``is_rare`` is True."""
    return [h for h in observe_all(counters, record, now=now) if h.is_rare]
