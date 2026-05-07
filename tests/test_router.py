"""Tests for logdrift.router."""
from __future__ import annotations

import io
from unittest.mock import patch

import pytest

from logdrift.alerts import AlertConfig
from logdrift.detector import AnomalyEvent
from logdrift.router import Route, Router, RouterError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _event(field: str = "status", value: str = "500", score: float = 0.9) -> AnomalyEvent:
    return AnomalyEvent(field=field, value=value, score=score, count=1, total=100)


def _config(stream: io.StringIO | None = None) -> AlertConfig:
    buf = stream or io.StringIO()
    return AlertConfig(min_score=0.0, destination="stdout", _stream=buf)


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

class TestRoute:
    def test_default_predicate_always_matches(self):
        r = Route(config=_config())
        assert r.matches(_event()) is True

    def test_custom_predicate_filters(self):
        r = Route(config=_config(), predicate=lambda e: e.value == "404")
        assert r.matches(_event(value="404")) is True
        assert r.matches(_event(value="500")) is False

    def test_predicate_exception_returns_false(self):
        r = Route(config=_config(), predicate=lambda e: 1 / 0)  # noqa: SIM901
        assert r.matches(_event()) is False


# ---------------------------------------------------------------------------
# Router construction
# ---------------------------------------------------------------------------

class TestRouterConfig:
    def test_default_first_match_is_false(self):
        rt = Router()
        assert rt._first_match is False

    def test_invalid_first_match_raises(self):
        with pytest.raises(RouterError):
            Router(first_match="yes")  # type: ignore[arg-type]

    def test_add_invalid_route_raises(self):
        rt = Router()
        with pytest.raises(RouterError):
            rt.add_route("not-a-route")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Router.route — fan-out (default)
# ---------------------------------------------------------------------------

class TestRouterFanOut:
    def test_no_routes_returns_empty(self):
        rt = Router()
        assert rt.route(_event()) == []

    def test_matching_route_fires(self):
        buf = io.StringIO()
        cfg = _config(buf)
        rt = Router()
        rt.add_route(Route(config=cfg, label="all"))
        fired = rt.route(_event())
        assert fired == ["all"]
        assert buf.getvalue() != ""

    def test_non_matching_route_does_not_fire(self):
        buf = io.StringIO()
        cfg = _config(buf)
        rt = Router()
        rt.add_route(Route(config=cfg, predicate=lambda e: e.value == "404", label="404-only"))
        fired = rt.route(_event(value="500"))
        assert fired == []
        assert buf.getvalue() == ""

    def test_multiple_matching_routes_all_fire(self):
        bufs = [io.StringIO(), io.StringIO()]
        rt = Router()
        for i, b in enumerate(bufs):
            rt.add_route(Route(config=_config(b), label=f"r{i}"))
        fired = rt.route(_event())
        assert fired == ["r0", "r1"]
        for b in bufs:
            assert b.getvalue() != ""


# ---------------------------------------------------------------------------
# Router.route — first_match
# ---------------------------------------------------------------------------

class TestRouterFirstMatch:
    def test_stops_after_first_match(self):
        bufs = [io.StringIO(), io.StringIO()]
        rt = Router(first_match=True)
        for i, b in enumerate(bufs):
            rt.add_route(Route(config=_config(b), label=f"r{i}"))
        fired = rt.route(_event())
        assert fired == ["r0"]
        assert bufs[0].getvalue() != ""
        assert bufs[1].getvalue() == ""
