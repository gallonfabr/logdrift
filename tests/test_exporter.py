"""Tests for logdrift.exporter."""

from __future__ import annotations

import json
import pathlib

import pytest

from logdrift.aggregator import Aggregator
from logdrift.detector import AnomalyEvent
from logdrift.exporter import ExportError, to_csv_file, to_csv_string, to_json_file
from logdrift.report import Report


def _make_report() -> Report:
    agg = Aggregator(window_seconds=60)
    evt = AnomalyEvent(field="level", value="ERROR", score=0.9, count=3)
    agg.add(evt)
    evt2 = AnomalyEvent(field="level", value="INFO", score=0.1, count=50)
    agg.add(evt2)
    return Report.build(agg)


class TestToJsonFile:
    def test_creates_file(self, tmp_path: pathlib.Path) -> None:
        dest = tmp_path / "report.json"
        result = to_json_file(_make_report(), dest)
        assert result == dest
        assert dest.exists()

    def test_valid_json_content(self, tmp_path: pathlib.Path) -> None:
        dest = tmp_path / "report.json"
        to_json_file(_make_report(), dest)
        data = json.loads(dest.read_text())
        assert "fields" in data
        assert "window_seconds" in data

    def test_bad_path_raises_export_error(self) -> None:
        bad = pathlib.Path("/no_such_dir/report.json")
        with pytest.raises(ExportError):
            to_json_file(_make_report(), bad)


class TestToCsvString:
    def test_header_present(self) -> None:
        csv_text = to_csv_string(_make_report())
        first_line = csv_text.splitlines()[0]
        assert first_line == "field,value,count,anomaly_score"

    def test_rows_contain_field_data(self) -> None:
        csv_text = to_csv_string(_make_report())
        assert "level" in csv_text
        assert "ERROR" in csv_text
        assert "INFO" in csv_text

    def test_empty_report_only_header(self) -> None:
        agg = Aggregator(window_seconds=30)
        report = Report.build(agg)
        lines = to_csv_string(report).splitlines()
        assert len(lines) == 1


class TestToCsvFile:
    def test_creates_file(self, tmp_path: pathlib.Path) -> None:
        dest = tmp_path / "out.csv"
        result = to_csv_file(_make_report(), dest)
        assert result == dest
        assert dest.exists()

    def test_bad_path_raises_export_error(self) -> None:
        with pytest.raises(ExportError):
            to_csv_file(_make_report(), "/no_such_dir/out.csv")
