"""Tests for logdrift.fingerprint."""
import pytest

from logdrift.fingerprint import (
    Fingerprinter,
    FingerprintConfig,
    FingerprintError,
    FingerprintResult,
)


# ---------------------------------------------------------------------------
# FingerprintConfig
# ---------------------------------------------------------------------------

class TestFingerprintConfig:
    def test_valid_config_created(self):
        cfg = FingerprintConfig(key_fields=["level", "service"])
        assert cfg.key_fields == ["level", "service"]
        assert cfg.hash_length == 8

    def test_empty_key_fields_raises(self):
        with pytest.raises(FingerprintError, match="key_fields"):
            FingerprintConfig(key_fields=[])

    def test_hash_length_too_short_raises(self):
        with pytest.raises(FingerprintError, match="hash_length"):
            FingerprintConfig(key_fields=["level"], hash_length=2)

    def test_hash_length_too_long_raises(self):
        with pytest.raises(FingerprintError, match="hash_length"):
            FingerprintConfig(key_fields=["level"], hash_length=65)

    def test_custom_hash_length_accepted(self):
        cfg = FingerprintConfig(key_fields=["level"], hash_length=16)
        assert cfg.hash_length == 16


# ---------------------------------------------------------------------------
# Fingerprinter
# ---------------------------------------------------------------------------

@pytest.fixture()
def fingerprinter():
    cfg = FingerprintConfig(key_fields=["level", "service"], hash_length=8)
    return Fingerprinter(cfg)


class TestFingerprinter:
    def test_returns_fingerprint_result(self, fingerprinter):
        result = fingerprinter.compute({"level": "error", "service": "api"})
        assert isinstance(result, FingerprintResult)
        assert len(result.fingerprint) == 8

    def test_same_fields_same_fingerprint(self, fingerprinter):
        r1 = fingerprinter.compute({"level": "error", "service": "api", "msg": "boom"})
        r2 = fingerprinter.compute({"level": "error", "service": "api", "msg": "other"})
        assert r1.fingerprint == r2.fingerprint

    def test_different_fields_different_fingerprint(self, fingerprinter):
        r1 = fingerprinter.compute({"level": "error", "service": "api"})
        r2 = fingerprinter.compute({"level": "info", "service": "api"})
        assert r1.fingerprint != r2.fingerprint

    def test_missing_field_treated_as_none(self, fingerprinter):
        r1 = fingerprinter.compute({"level": "error"})
        r2 = fingerprinter.compute({"level": "error", "service": None})
        assert r1.fingerprint == r2.fingerprint

    def test_count_increments(self, fingerprinter):
        r = fingerprinter.compute({"level": "error", "service": "api"})
        fingerprinter.compute({"level": "error", "service": "api"})
        assert fingerprinter.count(r.fingerprint) == 2

    def test_count_unknown_fingerprint_is_zero(self, fingerprinter):
        assert fingerprinter.count("deadbeef") == 0

    def test_process_returns_list(self, fingerprinter):
        records = [
            {"level": "info", "service": "web"},
            {"level": "error", "service": "db"},
        ]
        results = fingerprinter.process(records)
        assert len(results) == 2
        assert all(isinstance(r, FingerprintResult) for r in results)

    def test_top_returns_most_common(self, fingerprinter):
        for _ in range(3):
            fingerprinter.compute({"level": "error", "service": "api"})
        fingerprinter.compute({"level": "info", "service": "api"})
        top = fingerprinter.top(n=1)
        assert len(top) == 1
        assert top[0][1] == 3

    def test_top_invalid_n_raises(self, fingerprinter):
        with pytest.raises(FingerprintError, match="positive"):
            fingerprinter.top(n=0)
