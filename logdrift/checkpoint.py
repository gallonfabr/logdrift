"""Checkpoint: persist and restore pipeline progress by tracking the last
processed log position (byte offset or line number)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


class CheckpointError(Exception):
    """Raised when a checkpoint operation fails."""


@dataclass
class Checkpoint:
    """Tracks the last successfully processed position in a log source."""

    path: Path
    source: str
    offset: int = field(default=0)
    line_number: int = field(default=0)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self) -> None:
        """Write checkpoint state to *path* as JSON."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    {
                        "source": self.source,
                        "offset": self.offset,
                        "line_number": self.line_number,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            os.replace(tmp, self.path)
        except OSError as exc:
            raise CheckpointError(f"Failed to save checkpoint: {exc}") from exc

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def advance(self, *, bytes_read: int = 0, lines_read: int = 1) -> None:
        """Advance the stored position by *bytes_read* and *lines_read*."""
        self.offset += bytes_read
        self.line_number += lines_read

    def reset(self) -> None:
        """Reset position back to the beginning of the source."""
        self.offset = 0
        self.line_number = 0

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Checkpoint(source={self.source!r}, "
            f"offset={self.offset}, line_number={self.line_number})"
        )


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def load_checkpoint(path: Path, source: str) -> Checkpoint:
    """Load a :class:`Checkpoint` from *path*, or return a fresh one.

    If the file does not exist, a zero-offset checkpoint is returned so
    callers can treat first-run and resume identically.
    """
    if not path.exists():
        return Checkpoint(path=path, source=source)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"Failed to load checkpoint: {exc}") from exc
    return Checkpoint(
        path=path,
        source=data.get("source", source),
        offset=int(data.get("offset", 0)),
        line_number=int(data.get("line_number", 0)),
    )


def delete_checkpoint(path: Path) -> None:
    """Remove a checkpoint file if it exists."""
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise CheckpointError(f"Failed to delete checkpoint: {exc}") from exc
