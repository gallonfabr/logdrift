"""Watchdog: monitors log ingestion rate and fires alerts when the rate
drops below (or exceeds) configurable thresholds."""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional


class WatchdogError(Exception):
    """Raised for invalid watchdog configuration."""


@dataclass
class WatchdogConfig:
    window_seconds: float = 60.0
    min_rate: float = 0.0   # records/sec; 0 means no lower bound
    max_rate: float = 0.0   # records/sec; 0 means no upper bound
    name: str = "default"

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise WatchdogError("window_seconds must be positive")
        if self.min_rate < 0:
            raise WatchdogError("min_rate must be >= 0")
        if self.max_rate < 0:
            raise WatchdogError("max_rate must be >= 0")
        if self.max_rate and self.min_rate > self.max_rate:
            raise WatchdogError("min_rate must not exceed max_rate")


@dataclass
class WatchdogAlert:
    name: str
    current_rate: float
    threshold: float
    kind: str          # "below_min" | "above_max"
    timestamp: float = field(default_factory=time.time)

    def __repr__(self) -> str:
        return (
            f"WatchdogAlert(name={self.name!r}, kind={self.kind!r}, "
            f"current_rate={self.current_rate:.3f}, threshold={self.threshold:.3f})"
        )


class Watchdog:
    """Sliding-window ingestion-rate monitor."""

    def __init__(self, config: WatchdogConfig) -> None:
        self._cfg = config
        self._timestamps: Deque[float] = deque()

    def record(self, ts: Optional[float] = None) -> None:
        """Record one ingested log line at time *ts* (default: now)."""
        now = ts if ts is not None else time.time()
        self._timestamps.append(now)
        self._purge(now)

    def current_rate(self, now: Optional[float] = None) -> float:
        """Return records/sec inside the current window."""
        t = now if now is not None else time.time()
        self._purge(t)
        if self._cfg.window_seconds == 0:
            return 0.0
        return len(self._timestamps) / self._cfg.window_seconds

    def check(self, now: Optional[float] = None) -> Optional[WatchdogAlert]:
        """Return a WatchdogAlert if the rate violates a threshold, else None."""
        t = now if now is not None else time.time()
        rate = self.current_rate(t)
        cfg = self._cfg
        if cfg.min_rate and rate < cfg.min_rate:
            return WatchdogAlert(cfg.name, rate, cfg.min_rate, "below_min", t)
        if cfg.max_rate and rate > cfg.max_rate:
            return WatchdogAlert(cfg.name, rate, cfg.max_rate, "above_max", t)
        return None

    def _purge(self, now: float) -> None:
        cutoff = now - self._cfg.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
