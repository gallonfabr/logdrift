"""Anomaly detection for structured log records."""

from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional


class AnomalyEvent:
    """Represents a detected anomaly in a log record."""

    def __init__(self, record: Dict[str, Any], reason: str, field: Optional[str] = None):
        self.record = record
        self.reason = reason
        self.field = field

    def __repr__(self) -> str:
        return f"AnomalyEvent(field={self.field!r}, reason={self.reason!r})"


class Detector:
    """Detects anomalies by tracking field value distributions."""

    def __init__(self, min_samples: int = 10, alert_threshold: float = 0.01):
        """
        Args:
            min_samples: Minimum records seen before anomaly detection activates.
            alert_threshold: Minimum frequency ratio below which a value is anomalous.
        """
        self.min_samples = min_samples
        self.alert_threshold = alert_threshold
        self._counts: Dict[str, Dict[Any, int]] = defaultdict(lambda: defaultdict(int))
        self._total: int = 0

    def feed(self, record: Dict[str, Any]) -> List[AnomalyEvent]:
        """Feed a record into the detector and return any anomalies found."""
        anomalies: List[AnomalyEvent] = []

        if self._total >= self.min_samples:
            anomalies = self._check(record)

        self._update(record)
        self._total += 1
        return anomalies

    def _update(self, record: Dict[str, Any]) -> None:
        for key, value in record.items():
            self._counts[key][value] += 1

    def _check(self, record: Dict[str, Any]) -> List[AnomalyEvent]:
        anomalies = []
        for key, value in record.items():
            field_counts = self._counts.get(key)
            if field_counts is None:
                anomalies.append(AnomalyEvent(record, f"unseen field '{key}'", field=key))
                continue
            total_for_field = sum(field_counts.values())
            if total_for_field == 0:
                continue
            freq = field_counts.get(value, 0) / total_for_field
            if freq < self.alert_threshold:
                anomalies.append(
                    AnomalyEvent(
                        record,
                        f"rare value {value!r} for field '{key}' (freq={freq:.4f})",
                        field=key,
                    )
                )
        return anomalies

    def scan(self, records: Iterator[Dict[str, Any]]) -> Iterator[AnomalyEvent]:
        """Scan an iterable of records, yielding anomaly events."""
        for record in records:
            yield from self.feed(record)
