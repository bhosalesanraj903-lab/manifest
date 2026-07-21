"""R2: push run metrics to a Prometheus pushgateway.

Formats the normalize run summary in Prometheus exposition format and PUTs it
to PUSHGATEWAY_URL (default http://localhost:9091; docker network uses
http://pushgateway:9091). Best-effort by design: metrics failure logs to
stderr and never fails the pipeline run.
"""

import os
import sys

import requests

DEFAULT_URL = "http://localhost:9091"


def exposition(summary: dict) -> str:
    """Run-summary dict -> Prometheus exposition text."""
    q = summary["quarantined"]
    lines = [
        "# TYPE manifest_events_read gauge",
        f"manifest_events_read {summary['events_read']}",
        "# TYPE manifest_events_processed gauge",
        f"manifest_events_processed {summary['events_processed']}",
        "# TYPE manifest_duplicates gauge",
        f"manifest_duplicates {summary['duplicates']}",
        "# TYPE manifest_quarantined gauge",
    ]
    for reason in ("unmapped_ref", "unknown_status", "unparseable_time"):
        lines.append(f'manifest_quarantined{{reason="{reason}"}} {q[reason]}')
    lines += [
        "# TYPE manifest_exceptions gauge",
        f"manifest_exceptions {summary['exceptions']}",
        "# TYPE manifest_run_seconds gauge",
        f"manifest_run_seconds {summary['runtime_s']}",
    ]
    return "\n".join(lines) + "\n"


def push_run_summary(summary: dict, job: str = "carrier_normalize") -> bool:
    url = os.environ.get("PUSHGATEWAY_URL", DEFAULT_URL)
    try:
        resp = requests.put(f"{url}/metrics/job/{job}", data=exposition(summary), timeout=5)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"metrics push to {url} failed ({e!r}); continuing", file=sys.stderr)
        return False
