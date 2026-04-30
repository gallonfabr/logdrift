# logdrift

Lightweight log anomaly detector that alerts on unexpected patterns in structured logs.

---

## Installation

```bash
pip install logdrift
```

## Usage

```python
from logdrift import LogDriftDetector

detector = LogDriftDetector(threshold=0.85)

# Train on baseline logs
detector.fit("logs/baseline.json")

# Monitor new logs and alert on anomalies
alerts = detector.detect("logs/current.json")

for alert in alerts:
    print(f"[ALERT] {alert.timestamp} — {alert.message} (score: {alert.score:.2f})")
```

You can also run it from the command line:

```bash
logdrift --baseline logs/baseline.json --input logs/current.json --threshold 0.85
```

### Supported Log Formats

- JSON (structured)
- Key-value pairs
- Common log format (CLF)

## Configuration

| Option | Default | Description |
|---|---|---|
| `threshold` | `0.80` | Anomaly sensitivity (0.0–1.0) |
| `window` | `100` | Baseline rolling window size |
| `alert_on` | `all` | Alert level: `all`, `critical`, `warning` |

## License

MIT © logdrift contributors