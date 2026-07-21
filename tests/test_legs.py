import csv
from datetime import datetime, timezone

from pipelines import normalize

ASOF = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)


def row(sid, etype, actual, planned=None, mmsi=""):
    return {"event_id": f"{sid}-{etype}", "shipment_id": sid, "carrier": "MAEU",
            "event_type": etype, "planned_ts": planned or actual, "actual_ts": actual,
            "origin": "CNSHA", "dest": "USLAX", "vessel_mmsi": mmsi}


def test_build_legs_one_per_vessel_shipment():
    rows = [
        row("S1", "loaded_on_vessel", "2026-07-18T00:00:00Z", mmsi="353136000"),
        row("S1", "vessel_departed", "2026-07-19T00:00:00Z", mmsi="353136000"),
        row("S2", "booking_confirmed", "2026-07-19T00:00:00Z"),  # no vessel yet
    ]
    legs = normalize.build_legs(rows)
    assert legs == [{"shipment_id": "S1", "leg_seq": 1, "mode": "ocean",
                     "vessel_mmsi": "353136000", "origin": "CNSHA", "dest": "USLAX"}]


def test_vessel_stalled_fires_mid_voyage_only():
    dwell = {"353136000": 100.0}  # 100h at anchor, threshold 72h
    mid_voyage = [row("S1", "vessel_departed", "2026-07-19T00:00:00Z", mmsi="353136000")]
    excs = normalize.detect_exceptions(mid_voyage, ASOF, dwell)
    stalled = [e for e in excs if e["exception_type"] == "VESSEL_STALLED"]
    assert len(stalled) == 1
    assert stalled[0]["age_hours"] == 28.0  # 100 - 72

    # same vessel but shipment already arrived -> not stalled
    arrived = mid_voyage + [row("S1", "vessel_arrived", "2026-07-20T00:00:00Z", mmsi="353136000")]
    excs = normalize.detect_exceptions(arrived, ASOF, dwell)
    assert not [e for e in excs if e["exception_type"] == "VESSEL_STALLED"]

    # vessel under threshold -> not stalled
    excs = normalize.detect_exceptions(mid_voyage, ASOF, {"353136000": 10.0})
    assert not [e for e in excs if e["exception_type"] == "VESSEL_STALLED"]


def test_legs_written_by_run(tmp_path, monkeypatch):
    import shutil
    from pathlib import Path
    bronze = tmp_path / "bronze" / "2026-07-21"
    bronze.mkdir(parents=True)
    fixture = Path(__file__).parent / "fixtures" / "carrier_events.ndjson"
    shutil.copy(fixture, bronze / "events_abc.ndjson")
    monkeypatch.setattr(normalize, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(normalize, "SILVER", tmp_path / "silver")
    monkeypatch.setattr(normalize, "QUARANTINE", tmp_path / "quarantine")
    normalize.run(ASOF)
    assert (tmp_path / "silver" / "shipment_legs.csv").exists()
    with (tmp_path / "silver" / "shipment_legs.csv").open() as f:
        legs = list(csv.DictReader(f))
    assert legs == []  # fixture events carry no vessel_mmsi
