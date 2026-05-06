"""Context window: capture a rolling buffer of recent log records around an anomaly."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List


class ContextWindowError(ValueError):
    """Raised when ContextWindowConfig is invalid."""


@dataclass
class ContextWindowConfig:
    size: int = 5  # number of records to keep before and after the trigger

    def __post_init__(self) -> None:
        if self.size < 1:
            raise ContextWindowError(f"size must be >= 1, got {self.size}")
        if self.size > 500:
            raise ContextWindowError(f"size must be <= 500, got {self.size}")


@dataclass
class ContextSnapshot:
    """A snapshot of records surrounding a trigger record."""

    trigger: Dict[str, Any]
    before: List[Dict[str, Any]]
    after: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trigger": self.trigger,
            "before": self.before,
            "after": self.after,
        }


class ContextWindow:
    """Rolling buffer that captures context around a trigger record."""

    def __init__(self, config: ContextWindowConfig | None = None) -> None:
        self._config = config or ContextWindowConfig()
        self._buffer: Deque[Dict[str, Any]] = deque(maxlen=self._config.size)
        self._pending: List[_PendingCapture] = []

    def observe(self, record: Dict[str, Any]) -> None:
        """Feed a record into the window; resolves any pending after-buffers."""
        for pc in self._pending:
            if not pc.done:
                pc.add_after(record)
        self._pending = [pc for pc in self._pending if not pc.done]
        self._buffer.append(record)

    def capture(self, trigger: Dict[str, Any]) -> _PendingCapture:
        """Start capturing context for *trigger*. Call observe() for subsequent records."""
        pc = _PendingCapture(
            trigger=trigger,
            before=list(self._buffer),
            size=self._config.size,
        )
        self._pending.append(pc)
        return pc

    @property
    def buffer(self) -> List[Dict[str, Any]]:
        return list(self._buffer)


class _PendingCapture:
    """Internal helper that accumulates 'after' records."""

    def __init__(self, trigger: Dict[str, Any], before: List[Dict[str, Any]], size: int) -> None:
        self._trigger = trigger
        self._before = before
        self._after: List[Dict[str, Any]] = []
        self._size = size

    def add_after(self, record: Dict[str, Any]) -> None:
        if not self.done:
            self._after.append(record)

    @property
    def done(self) -> bool:
        return len(self._after) >= self._size

    def snapshot(self) -> ContextSnapshot:
        return ContextSnapshot(trigger=self._trigger, before=self._before, after=list(self._after))
