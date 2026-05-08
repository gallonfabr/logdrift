"""Helpers to build Watchdog instances from plain config dicts and to
integrate them into a record-processing loop."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from logdrift.watchdog import Watchdog, WatchdogAlert, WatchdogConfig


def build_watchdog(cfg: Dict) -> Watchdog:
    """Construct a Watchdog from a plain dictionary.

    Recognised keys mirror WatchdogConfig fields:
        window_seconds, min_rate, max_rate, name
    """
    return Watchdog(
        WatchdogConfig(
            window_seconds=float(cfg.get("window_seconds", 60.0)),
            min_rate=float(cfg.get("min_rate", 0.0)),
            max_rate=float(cfg.get("max_rate", 0.0)),
            name=str(cfg.get("name", "default")),
        )
    )


def build_watchdogs(cfgs: Iterable[Dict]) -> List[Watchdog]:
    """Build multiple Watchdog instances from an iterable of config dicts."""
    return [build_watchdog(c) for c in cfgs]


def observe_record(watchdogs: List[Watchdog], ts: Optional[float] = None) -> None:
    """Notify every watchdog that one record has been ingested."""
    for wd in watchdogs:
        wd.record(ts)


def alerts_for_tick(
    watchdogs: List[Watchdog], now: Optional[float] = None
) -> List[WatchdogAlert]:
    """Return all currently-firing alerts across every watchdog."""
    results: List[WatchdogAlert] = []
    for wd in watchdogs:
        alert = wd.check(now)
        if alert is not None:
            results.append(alert)
    return results
