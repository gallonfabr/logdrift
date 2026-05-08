"""Convenience helpers for building :class:`Normalizer` instances from config dicts."""
from __future__ import annotations

from typing import Any, Dict, List

from logdrift.normalizer import Normalizer, NormalizerError, NormalizerRule

# Built-in transform shortcuts available in config dicts.
_BUILTIN_TRANSFORMS: Dict[str, Any] = {
    "lower": str.lower,
    "upper": str.upper,
    "strip": str.strip,
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
}


def _resolve_transform(spec: Any) -> Any:
    """Return a callable from *spec*, which may be a string key or a callable."""
    if callable(spec):
        return spec
    if isinstance(spec, str):
        if spec not in _BUILTIN_TRANSFORMS:
            raise NormalizerError(
                f"unknown built-in transform '{spec}'; "
                f"available: {sorted(_BUILTIN_TRANSFORMS)}"
            )
        return _BUILTIN_TRANSFORMS[spec]
    raise NormalizerError("transform must be a callable or a built-in name string")


def build_rule(config: Dict[str, Any]) -> NormalizerRule:
    """Build a :class:`NormalizerRule` from a plain config dict.

    Required keys: ``field``, ``transform``.
    Optional key:  ``output_field``.
    """
    field = config.get("field", "")
    transform = _resolve_transform(config.get("transform"))
    output_field = config.get("output_field", None)
    return NormalizerRule(field=field, transform=transform, output_field=output_field)


def build_normalizer(configs: List[Dict[str, Any]]) -> Normalizer:
    """Build a :class:`Normalizer` from a list of rule config dicts."""
    normalizer = Normalizer()
    for cfg in configs:
        normalizer.add_rule(build_rule(cfg))
    return normalizer
