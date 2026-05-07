"""Tests for logdrift.checkpoint."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from logdrift.checkpoint import (
    Checkpoint,
    CheckpointError,
    delete_checkpoint,
    load_checkpoint,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ckpt(tmp_path: Path, source: str = "app.log") -> Checkpoint:
    return Checkpoint(path=tmp_path / "ckpt.json", source=source)


# ---------------------------------------------------------------------------
# Checkpoint.advance / reset
# ---------------------------------------------------------------------------


class TestCheckpoint:
    def test_initial_state(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        assert ck.offset == 0
        assert ck.line_number == 0

    def test_advance_increments_both(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        ck.advance(bytes_read=128, lines_read=3)
        assert ck.offset == 128
        assert ck.line_number == 3

    def test_advance_default_lines_read_is_one(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        ck.advance(bytes_read=64)
        assert ck.line_number == 1

    def test_advance_accumulates(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        ck.advance(bytes_read=10, lines_read=1)
        ck.advance(bytes_read=20, lines_read=2)
        assert ck.offset == 30
        assert ck.line_number == 3

    def test_reset_clears_position(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        ck.advance(bytes_read=500, lines_read=10)
        ck.reset()
        assert ck.offset == 0
        assert ck.line_number == 0


# ---------------------------------------------------------------------------
# Checkpoint.save / load_checkpoint
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_save_creates_file(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        ck.advance(bytes_read=256, lines_read=4)
        ck.save()
        assert ck.path.exists()

    def test_roundtrip(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path, source="server.log")
        ck.advance(bytes_read=1024, lines_read=8)
        ck.save()

        loaded = load_checkpoint(ck.path, source="server.log")
        assert loaded.offset == 1024
        assert loaded.line_number == 8
        assert loaded.source == "server.log"

    def test_load_missing_returns_fresh(self, tmp_path: Path) -> None:
        path = tmp_path / "nonexistent.json"
        ck = load_checkpoint(path, source="app.log")
        assert ck.offset == 0
        assert ck.line_number == 0

    def test_load_corrupt_file_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.json"
        path.write_text("not json", encoding="utf-8")
        with pytest.raises(CheckpointError, match="Failed to load"):
            load_checkpoint(path, source="app.log")

    def test_save_nested_dirs_created(self, tmp_path: Path) -> None:
        path = tmp_path / "a" / "b" / "ckpt.json"
        ck = Checkpoint(path=path, source="x")
        ck.save()
        assert path.exists()


# ---------------------------------------------------------------------------
# delete_checkpoint
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_removes_file(self, tmp_path: Path) -> None:
        ck = _ckpt(tmp_path)
        ck.save()
        assert ck.path.exists()
        delete_checkpoint(ck.path)
        assert not ck.path.exists()

    def test_delete_missing_is_silent(self, tmp_path: Path) -> None:
        path = tmp_path / "ghost.json"
        delete_checkpoint(path)  # should not raise
