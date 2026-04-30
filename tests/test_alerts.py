"""Tests for logdrift.alerts."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import patch
import io

import pytest

from logdrift.alerts import AlertConfig, build_handler, dispatch
from logdrift.detector import AnomalyEvent


@pytest.fixture()
def event() -> AnomalyEvent:
    return AnomalyEvent(field="status", value="500", score=0.95)


@pytest.fixture()
def low_score_event() -> AnomalyEvent:
    return AnomalyEvent(field="status", value="200", score=0.1)


class TestBuildHandler:
    def test_stderr_handler_writes_to_stderr(self, event, capsys):
        handler = build_handler(AlertConfig(channel="stderr"))
        handler(event)
        captured = capsys.readouterr()
        assert "ANOMALY" in captured.err
        assert "status" in captured.err
        assert "500" in captured.err

    def test_stdout_handler_writes_to_stdout(self, event, capsys):
        handler = build_handler(AlertConfig(channel="stdout"))
        handler(event)
        captured = capsys.readouterr()
        assert "ANOMALY" in captured.out
        assert "status" in captured.out

    def test_json_file_handler_appends_record(self, event):
        with tempfile.NamedTemporaryFile(
            mode="r", suffix=".jsonl", delete=False
        ) as tmp:
            path = tmp.name
        try:
            handler = build_handler(AlertConfig(channel="json_file", path=path))
            handler(event)
            handler(event)
            lines = open(path).readlines()
            assert len(lines) == 2
            record = json.loads(lines[0])
            assert record["field"] == "status"
            assert record["score"] == pytest.approx(0.95)
        finally:
            os.unlink(path)

    def test_json_file_requires_path(self):
        with pytest.raises(ValueError, match="path"):
            build_handler(AlertConfig(channel="json_file"))

    def test_unknown_channel_raises(self):
        with pytest.raises(ValueError, match="Unknown alert channel"):
            build_handler(AlertConfig(channel="slack"))

    def test_min_severity_filters_low_score(self, low_score_event, capsys):
        handler = build_handler(AlertConfig(channel="stdout", min_severity=0.5))
        handler(low_score_event)
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_min_severity_passes_high_score(self, event, capsys):
        handler = build_handler(AlertConfig(channel="stdout", min_severity=0.5))
        handler(event)
        captured = capsys.readouterr()
        assert "ANOMALY" in captured.out


class TestDispatch:
    def test_dispatch_calls_all_handlers(self, event):
        calls: list = []
        dispatch(event, [lambda e: calls.append(("a", e)), lambda e: calls.append(("b", e))])
        assert len(calls) == 2
        assert calls[0][0] == "a"
        assert calls[1][0] == "b"

    def test_dispatch_empty_handlers(self, event):
        dispatch(event, [])  # should not raise
