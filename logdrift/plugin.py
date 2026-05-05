"""Simple plugin registry for enricher rules and alert handlers.

Third-party packages can register :class:`~logdrift.enricher.EnricherRule`
objects or callable alert handlers under a named *group* so that the
pipeline can discover them without hard-coded imports.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


_registry: Dict[str, List[Any]] = {}


class PluginError(Exception):
    """Raised when plugin registration or lookup fails."""


def register(group: str, obj: Any) -> None:
    """Register *obj* under *group*.

    Parameters
    ----------
    group:
        Logical category, e.g. ``"enricher"`` or ``"alert_handler"``.
    obj:
        The object to register (rule, callable, …).
    """
    if not group:
        raise PluginError("group must be a non-empty string")
    _registry.setdefault(group, []).append(obj)


def get(group: str) -> List[Any]:
    """Return all objects registered under *group* (empty list if none)."""
    return list(_registry.get(group, []))


def clear(group: Optional[str] = None) -> None:
    """Remove registered plugins.

    If *group* is given, only that group is cleared; otherwise the entire
    registry is reset (useful in tests).
    """
    if group is None:
        _registry.clear()
    else:
        _registry.pop(group, None)


def plugin(group: str) -> Callable[[Any], Any]:
    """Decorator that registers the decorated object under *group*.

    Example
    -------
    >>> @plugin("enricher")
    ... def my_rule(record):
    ...     record["extra"] = True
    """
    def decorator(obj: Any) -> Any:
        register(group, obj)
        return obj
    return decorator


def list_groups() -> List[str]:
    """Return the names of all non-empty groups currently in the registry."""
    return [g for g, items in _registry.items() if items]
