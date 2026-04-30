"""Log file reader that yields parsed records."""

from __future__ import annotations

import sys
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Any

from logdrift.parser import ParseError, parse_line


def iter_lines(source: str | Path | None) -> Generator[str, None, None]:
    """Yield raw lines from a file path or stdin (when source is None)."""
    if source is None:
        yield from sys.stdin
    else:
        with Path(source).open(encoding="utf-8") as fh:
            yield from fh


def read_records(
    source: str | Path | None,
    *,
    skip_errors: bool = True,
) -> Generator[dict[str, Any], None, None]:
    """Yield parsed log records from *source*.

    Args:
        source: Path to a log file, or ``None`` to read from stdin.
        skip_errors: When *True*, malformed lines are silently skipped;
            when *False*, a :class:`~logdrift.parser.ParseError` is raised.
    """
    for lineno, raw in enumerate(iter_lines(source), start=1):
        if not raw.strip():
            continue
        try:
            record = parse_line(raw)
            record.setdefault("_lineno", lineno)
            yield record
        except ParseError:
            if not skip_errors:
                raise
