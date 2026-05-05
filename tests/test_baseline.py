"""Tests for logdrift.baseline."""
import json
import os
import pytest

from logdrift.baseline import Baseline, BaselineError, load, save


# ---------------------------------------------------------------------------
# Baseline unit tests
# ---------------------------------------------------------------------------

class TestBaseline:
    def test_record_and_get_count(self):
        b = Baseline()
        b.record("level", "ERROR")
        b.record("level", "ERROR")
        b.record("level", "INFO")
        assert b.get_count("level", "ERROR") == 2
        assert b.get_count("level", "INFO") == 1

    def test_get_count_missing_field(self):
        b = Baseline()
        assert b.get_count("nonexistent", "val") == 0

    def test_get_count_missing_value(self):
        b = Baseline()
        b.record("level", "INFO")
        assert b.get_count("level", "DEBUG") == 0

    def test_total(self):
        b = Baseline()
        b.record("status", "200", count=10)
        b.record("status", "500", count=3)
        assert b.total("status") == 13

    def test_total_missing_field(self):
        b = Baseline()
        assert b.total("missing") == 0

    def test_record_with_explicit_count(self):
        b = Baseline()
        b.record("env", "prod", count=5)
        assert b.get_count("env", "prod") == 5

    def test_round_trip_dict(self):
        b = Baseline()
        b.record("level", "WARN", count=7)
        b2 = Baseline.from_dict(b.to_dict())
        assert b2.get_count("level", "WARN") == 7


# ---------------------------------------------------------------------------
# Persistence tests
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_save_creates_file(self, tmp_path):
        b = Baseline()
        b.record("level", "INFO", count=4)
        dest = str(tmp_path / "baseline.json")
        save(b, dest)
        assert os.path.isfile(dest)

    def test_save_valid_json(self, tmp_path):
        b = Baseline()
        b.record("level", "ERROR", count=2)
        dest = str(tmp_path / "baseline.json")
        save(b, dest)
        with open(dest) as fh:
            data = json.load(fh)
        assert data["counts"]["level"]["ERROR"] == 2

    def test_load_round_trip(self, tmp_path):
        b = Baseline()
        b.record("host", "web-01", count=9)
        dest = str(tmp_path / "bl.json")
        save(b, dest)
        b2 = load(dest)
        assert b2.get_count("host", "web-01") == 9

    def test_load_missing_file_raises(self, tmp_path):
        with pytest.raises(BaselineError, match="not found"):
            load(str(tmp_path / "no_such_file.json"))

    def test_save_bad_path_raises(self):
        b = Baseline()
        with pytest.raises(BaselineError, match="Cannot write"):
            save(b, "/no/such/directory/baseline.json")
