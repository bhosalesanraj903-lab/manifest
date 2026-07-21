"""Regression tests for the R11 game-day fixes."""

import csv
import json
import os
import time
from datetime import datetime, timezone

from alerting import notify
from pipelines import ingest

NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def test_feed_silence_measured_from_newest_bronze(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest, "BRONZE", tmp_path / "bronze")
    assert ingest.hours_since_last_landed() is None  # no bronze at all

    part = tmp_path / "bronze" / "2026-07-21"
    part.mkdir(parents=True)
    f = part / "events_abc.ndjson"
    f.write_text("{}\n")
    five_h_ago = time.time() - 5 * 3600
    os.utime(f, (five_h_ago, five_h_ago))
    assert 4.9 < ingest.hours_since_last_landed() < 5.1


def test_quarantine_spike_alerts_once_per_day(tmp_path):
    with (tmp_path / "exception_queue.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["exception_id", "shipment_id", "carrier",
                                          "exception_type", "detected_at", "age_hours",
                                          "detail", "probable_cause"])
        w.writeheader()
    (tmp_path / "_run_summary.json").write_text(json.dumps(
        {"events_read": 1000, "quarantined": {"total": 111}}))

    s1 = notify.run(silver=tmp_path, webhook_url="", now=NOW)
    assert s1["quarantine_pct"] == 11.1
    assert s1.get("ops_alerts") == 1
    s2 = notify.run(silver=tmp_path, webhook_url="", now=NOW)
    assert s2.get("ops_alerts") is None  # suppressed same day

    next_day = datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc)
    s3 = notify.run(silver=tmp_path, webhook_url="", now=next_day)
    assert s3.get("ops_alerts") == 1  # new day, still breaching -> re-alert


def test_no_spike_no_ops_alert(tmp_path):
    with (tmp_path / "exception_queue.csv").open("w", newline="") as f:
        csv.DictWriter(f, fieldnames=["exception_id", "shipment_id", "carrier",
                                      "exception_type", "detected_at", "age_hours",
                                      "detail", "probable_cause"]).writeheader()
    (tmp_path / "_run_summary.json").write_text(json.dumps(
        {"events_read": 1000, "quarantined": {"total": 10}}))
    s = notify.run(silver=tmp_path, webhook_url="", now=NOW)
    assert s["quarantine_pct"] == 1.0
    assert s.get("ops_alerts") is None
