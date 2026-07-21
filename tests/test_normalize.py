import csv
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines import normalize

FIXTURE = Path(__file__).parent / "fixtures" / "carrier_events.ndjson"
ASOF = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def lake(tmp_path, monkeypatch):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    part = bronze / "2026-07-21"
    part.mkdir(parents=True)
    shutil.copy(FIXTURE, part / "events_abc.ndjson")
    monkeypatch.setattr(normalize, "BRONZE", bronze)
    monkeypatch.setattr(normalize, "SILVER", silver)
    return silver


def read_csv(path):
    with path.open() as f:
        return list(csv.DictReader(f))


def test_normalize_counts_and_outputs(lake):
    summary = normalize.run(ASOF)

    assert summary["events_read"] == 13
    assert summary["duplicates"] == 1
    assert summary["quarantined"] == {
        "total": 3, "unmapped_ref": 1, "unknown_status": 1, "unparseable_time": 1,
    }
    assert summary["events_processed"] == 9

    events = read_csv(lake / "shipment_events.csv")
    assert len(events) == 9
    # timestamps normalized to ISO UTC regardless of carrier encoding
    mscu = next(e for e in events if e["carrier"] == "MSCU")
    assert mscu["actual_ts"] == "2026-07-20T14:00:00Z"
    hlcu = next(e for e in events if e["carrier"] == "HLCU")
    assert hlcu["actual_ts"] == "2026-07-18T12:00:00Z"


def test_exception_rules_fire(lake):
    normalize.run(ASOF)
    excs = read_csv(lake / "exception_queue.csv")
    by_type = {e["exception_type"]: e for e in excs}

    assert set(by_type) == {"LATE_DEPARTURE", "CUSTOMS_DWELL", "MISSED_MILESTONE"}
    assert by_type["LATE_DEPARTURE"]["shipment_id"] == "MSCU1234567"
    assert by_type["CUSTOMS_DWELL"]["shipment_id"] == "HLCU123A456789"
    assert by_type["MISSED_MILESTONE"]["shipment_id"] == "CMDUAB1234567"
    # delivered on-time shipment raises nothing
    assert all(e["shipment_id"] != "MAEU123456789" for e in excs)
    # age_hours: customs dwell of 60h is 12h past the 48h threshold
    assert float(by_type["CUSTOMS_DWELL"]["age_hours"]) == pytest.approx(12.0, abs=0.1)


def test_exception_ids_stable_across_runs(lake):
    normalize.run(ASOF)
    first = {e["exception_id"] for e in read_csv(lake / "exception_queue.csv")}
    normalize.run(datetime(2026, 7, 21, 6, 0, tzinfo=timezone.utc))
    second = {e["exception_id"] for e in read_csv(lake / "exception_queue.csv")}
    assert first == second  # R1's alerter diffs on these IDs


def test_rerun_is_idempotent(lake):
    s1 = normalize.run(ASOF)
    s2 = normalize.run(ASOF)
    assert s1["events_processed"] == s2["events_processed"]
    assert read_csv(lake / "shipment_events.csv") == read_csv(lake / "shipment_events.csv")


def test_quarantined_rows_do_not_crash_run(lake):
    summary = normalize.run(ASOF)
    assert summary["quarantined"]["total"] == 3
    events = read_csv(lake / "shipment_events.csv")
    assert all(not e["shipment_id"].startswith("XXXX") for e in events)
