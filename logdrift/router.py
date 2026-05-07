"""Route AnomalyEvents to different alert handlers based on field predicates."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from logdrift.alerts import AlertConfig, dispatch
from logdrift.detector import AnomalyEvent


class RouterError(Exception):
    """Raised when router configuration is invalid."""


@dataclass
class Route:
    """A single routing rule: if *predicate* matches the event, send it via *config*."""

    config: AlertConfig
    predicate: Callable[[AnomalyEvent], bool] = field(default=lambda _: True)
    label: str = ""

    def matches(self, event: AnomalyEvent) -> bool:
        try:
            return bool(self.predicate(event))
        except Exception:
            return False


class Router:
    """Evaluate an ordered list of Routes and dispatch matching events.

    By default every matching route fires (fan-out).  Set *first_match=True* to
    stop after the first matching route (chain behaviour).
    """

    def __init__(self, first_match: bool = False) -> None:
        if not isinstance(first_match, bool):
            raise RouterError("first_match must be a bool")
        self._first_match = first_match
        self._routes: List[Route] = []

    # ------------------------------------------------------------------
    def add_route(self, route: Route) -> None:
        if not isinstance(route, Route):
            raise RouterError("route must be a Route instance")
        self._routes.append(route)

    # ------------------------------------------------------------------
    def route(self, event: AnomalyEvent) -> List[str]:
        """Dispatch *event* to all matching routes.

        Returns the labels (or repr) of routes that fired.
        """
        fired: List[str] = []
        for r in self._routes:
            if r.matches(event):
                dispatch(event, r.config)
                fired.append(r.label or repr(r.config))
                if self._first_match:
                    break
        return fired

    # ------------------------------------------------------------------
    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Router(routes={len(self._routes)}, first_match={self._first_match})"
        )
