import csv
import json
from datetime import datetime, timezone

import pytest

from alerting import notify

NOW = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)

QUEUE = [
    {"exception_id": "e-new", "shipment_id": "MAEU123456789", "carrier": "MAEU",
     "exception_type": "LATE_DEPARTURE", "detected_at": "2026-07-21T00:00:00Z",
     "age_hours": "10.0", "detail": "departed 30h after plan"},
    {"exception_id": "e-old", "shipment_id": "MSCU1234567", "carrier": "MSCU",
     "exception_type": "CUSTOMS_DWELL", "detected_at": "2026-07-21T00:00:00Z",
     "age_hours": "50.0", "detail": "in customs 98h with no release"},
]


@pytest.fixture
def silver(tmp_path):
    with (tmp_path / "exception_queue.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(QUEUE[0]))
        w.writeheader()
        w.writerows(QUEUE)
    return tmp_path


def write_queue(silver, rows):
    with (silver / "exception_queue.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(QUEUE[0]))
        w.writeheader()
        w.writerows(rows)


def test_new_exception_alerts_once_then_silent(silver):
    s1 = notify.run(silver=silver, webhook_url="", now=NOW)
    assert s1["alerted_new"] == 2 and s1["suppressed"] == 0
    s2 = notify.run(silver=silver, webhook_url="", now=NOW)
    assert s2["alerted_new"] == 0 and s2["suppressed"] == 2


def test_escalation_band_realerts_once(silver):
    notify.run(silver=silver, webhook_url="", now=NOW)
    # e-new ages from 10h (band 0) into 60h (band 1) -> one escalation alert
    aged = [dict(QUEUE[0], age_hours="60.0"), QUEUE[1]]
    write_queue(silver, aged)
    s = notify.run(silver=silver, webhook_url="", now=NOW)
    assert s["alerted_escalated"] == 1 and s["suppressed"] == 1
    # same band again -> silent
    s = notify.run(silver=silver, webhook_url="", now=NOW)
    assert s["alerted_escalated"] == 0 and s["suppressed"] == 2


def test_webhook_failure_writes_failure_record_and_retries_next_run(silver, monkeypatch):
    calls = []

    class Resp:
        status_code = 500

    monkeypatch.setattr(notify, "BACKOFF_S", 0.0)
    monkeypatch.setattr(notify.requests, "post",
                        lambda url, json, timeout: calls.append(url) or Resp())

    s = notify.run(silver=silver, webhook_url="https://hooks.slack.example/x", now=NOW)
    assert s["failed"] == 2
    assert len(calls) == 2 * notify.RETRIES  # 3 attempts per exception

    failures = (silver / "_alert_failures.ndjson").read_text().splitlines()
    assert len(failures) == 2
    assert json.loads(failures[0])["exception"]["exception_id"] == "e-new"

    # state NOT recorded on failure -> next (healthy) run alerts them
    s2 = notify.run(silver=silver, webhook_url="", now=NOW)
    assert s2["alerted_new"] == 2


def test_webhook_success_posts_message(silver, monkeypatch):
    sent = []

    class Resp:
        status_code = 200

    monkeypatch.setattr(notify.requests, "post",
                        lambda url, json, timeout: sent.append(json) or Resp())
    s = notify.run(silver=silver, webhook_url="https://hooks.slack.example/x", now=NOW)
    assert s["alerted_new"] == 2 and s["failed"] == 0
    assert "LATE_DEPARTURE" in sent[0]["text"]


def test_band_boundaries():
    assert notify.band(0) == 0
    assert notify.band(47.9) == 0
    assert notify.band(48) == 1
    assert notify.band(95.9) == 1
    assert notify.band(96) == 2
