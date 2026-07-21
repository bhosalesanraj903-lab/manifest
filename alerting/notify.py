"""R1: Slack exception alerter.

Runs after normalize. Diffs the current silver exception_queue against
previously-alerted state (data/silver/_alert_state.json). Sends ONE Slack
webhook message per NEW exception_id; known exceptions stay silent unless
age_hours crosses into a higher severity band (0-48h -> 48-96h -> 96h+),
which re-alerts once per band.

Config: SLACK_WEBHOOK_URL env var. Unset -> log-only mode (dev): messages go
to stdout, state is still recorded so behavior matches prod.

Failure mode: webhook 4xx/5xx/network error -> retry 3x with backoff -> on
final failure append the alert to data/silver/_alert_failures.ndjson and
continue WITHOUT recording state, so the next run retries it. Never raises.

Usage:
    python -m alerting.notify [--asof ISO-UTC]
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "data" / "silver"

BANDS_H = [48, 96]  # band 0: <48h, band 1: 48-96h, band 2: >=96h
RETRIES = 3
BACKOFF_S = 2.0  # base; grows 2x per attempt (patched short in tests)


def band(age_hours: float) -> int:
    return sum(age_hours >= b for b in BANDS_H)


def format_message(exc: dict, escalated: bool) -> str:
    prefix = ":rotating_light: ESCALATION" if escalated else ":package: New exception"
    return (f"{prefix} [{exc['exception_type']}] shipment {exc['shipment_id']} "
            f"({exc['carrier']}) — {exc['detail']} — age {exc['age_hours']}h")


def send_slack(text: str, webhook_url: str) -> bool:
    """POST to the webhook with retries. Returns True on success."""
    delay = BACKOFF_S
    for attempt in range(RETRIES):
        try:
            resp = requests.post(webhook_url, json={"text": text}, timeout=10)
            if resp.status_code < 400:
                return True
            print(f"webhook HTTP {resp.status_code} (attempt {attempt + 1}/{RETRIES})", file=sys.stderr)
        except requests.RequestException as e:
            print(f"webhook error {e!r} (attempt {attempt + 1}/{RETRIES})", file=sys.stderr)
        if attempt < RETRIES - 1:
            time.sleep(delay)
            delay *= 2
    return False


def run(silver: Path = SILVER, webhook_url: str | None = None,
        now: datetime | None = None) -> dict:
    webhook_url = webhook_url if webhook_url is not None else os.environ.get("SLACK_WEBHOOK_URL", "")
    now = now or datetime.now(timezone.utc)
    state_path = silver / "_alert_state.json"
    failures_path = silver / "_alert_failures.ndjson"

    queue_path = silver / "exception_queue.csv"
    with queue_path.open() as f:
        queue = list(csv.DictReader(f))
    state = json.loads(state_path.read_text()) if state_path.exists() else {}

    summary = {"exceptions": len(queue), "alerted_new": 0, "alerted_escalated": 0,
               "suppressed": 0, "failed": 0, "mode": "slack" if webhook_url else "log-only"}

    for exc in queue:
        eid = exc["exception_id"]
        b = band(float(exc["age_hours"]))
        known = state.get(eid)
        if known is not None and b <= known["band"]:
            summary["suppressed"] += 1
            continue

        escalated = known is not None
        text = format_message(exc, escalated)

        if webhook_url:
            ok = send_slack(text, webhook_url)
        else:
            print(f"[log-only] {text}")
            ok = True

        if ok:
            state[eid] = {"type": exc["exception_type"], "band": b,
                          "alerted_at": now.strftime("%Y-%m-%dT%H:%M:%SZ")}
            summary["alerted_escalated" if escalated else "alerted_new"] += 1
        else:
            summary["failed"] += 1
            with failures_path.open("a") as f:
                f.write(json.dumps({"failed_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "exception": exc, "message": text}) + "\n")

    state_path.write_text(json.dumps(state, indent=2))
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asof", default=None)
    args = ap.parse_args()
    now = (datetime.fromisoformat(args.asof).replace(tzinfo=timezone.utc)
           if args.asof else datetime.now(timezone.utc))
    print(json.dumps(run(now=now)))


if __name__ == "__main__":
    main()
