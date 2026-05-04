"""Export anomaly reports to various output formats (JSON file, CSV)."""

from __future__ import annotations

import csv
import io
import json
import pathlib
from typing import Union

from logdrift.report import Report


class ExportError(Exception):
    """Raised when an export operation fails."""


def to_json_file(report: Report, path: Union[str, pathlib.Path]) -> pathlib.Path:
    """Write *report* as a JSON file and return the resolved path."""
    dest = pathlib.Path(path)
    try:
        dest.write_text(report.to_json(), encoding="utf-8")
    except OSError as exc:
        raise ExportError(f"Cannot write JSON to {dest}: {exc}") from exc
    return dest


def to_csv_string(report: Report) -> str:
    """Serialise *report* to a CSV string.

    Columns: field, value, count, anomaly_score
    One row per (field, value) pair across all windows.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["field", "value", "count", "anomaly_score"])

    data = report.to_dict()
    for field_report in data.get("fields", []):
        field_name = field_report["field"]
        for entry in field_report.get("values", []):
            writer.writerow(
                [
                    field_name,
                    entry["value"],
                    entry["count"],
                    entry["anomaly_score"],
                ]
            )
    return buf.getvalue()


def to_csv_file(report: Report, path: Union[str, pathlib.Path]) -> pathlib.Path:
    """Write *report* as a CSV file and return the resolved path."""
    dest = pathlib.Path(path)
    try:
        dest.write_text(to_csv_string(report), encoding="utf-8")
    except OSError as exc:
        raise ExportError(f"Cannot write CSV to {dest}: {exc}") from exc
    return dest
