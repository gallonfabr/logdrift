"""Build a Baseline from an Aggregator's current WindowStats."""
from __future__ import annotations

from typing import Optional

from logdrift.aggregator import Aggregator
from logdrift.baseline import Baseline, save


def build_from_aggregator(aggregator: Aggregator) -> Baseline:
    """Convert the current window statistics in *aggregator* into a Baseline.

    For every (field, value) pair tracked by the aggregator the total count
    across all windows is summed and recorded in the returned Baseline.
    """
    baseline = Baseline()
    for field_name in aggregator.fields():
        stats_list = aggregator.get_stats(field_name)  # list[WindowStats]
        combined: dict[str, int] = {}
        for ws in stats_list:
            for value, cnt in ws.counts.items():
                combined[value] = combined.get(value, 0) + cnt
        for value, total in combined.items():
            baseline.record(field_name, value, count=total)
    return baseline


def build_and_save(
    aggregator: Aggregator,
    path: str,
    *,
    merge_existing: bool = False,
) -> Baseline:
    """Build a Baseline from *aggregator* and persist it to *path*.

    Parameters
    ----------
    aggregator:
        Source of observed frequency data.
    path:
        Destination JSON file path.
    merge_existing:
        When *True* and a baseline already exists at *path*, merge the new
        counts into it before saving.  Defaults to *False* (overwrite).
    """
    new_baseline = build_from_aggregator(aggregator)

    if merge_existing:
        try:
            from logdrift.baseline import load as _load
            existing = _load(path)
            for field_name, values in new_baseline.counts.items():
                for value, cnt in values.items():
                    existing.record(field_name, value, count=cnt)
            new_baseline = existing
        except Exception:
            # If loading fails (file absent, corrupt, etc.) just use the new one.
            pass

    save(new_baseline, path)
    return new_baseline
