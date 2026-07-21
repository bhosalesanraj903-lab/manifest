import csv
import json
from datetime import datetime, timezone

import pytest

from generator.generate import gen_events
from india.pincode import normalize_pincode
from pipelines import normalize

ASOF = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("raw,expected", [
    ("Plot 4, MIDC Area, Mumbai 400001", "400001"),
    ("Plot 4, MIDC Area, Mumbai 400 001", "400001"),
    ("Flat 2, Pune PIN-411001", "411001"),
    ("Surat - 395-003", "395003"),
    ("Gandhidham 37O2O1", "370201"),          # confusable O -> 0
    ("Plot 4, MIDC Area, Mumbai", None),        # missing PIN
    ("street 12345", None),                      # too short
    ("zone 900100 nope", None),                  # invalid first digit
    ("", None),
])
def test_pincode_normalizer(raw, expected):
    assert normalize_pincode(raw) == expected


def test_pin_is_last_group_when_multiple():
    assert normalize_pincode("Gate 400001 Compound, Ahmedabad 380001") == "380001"


@pytest.fixture
def india_lake(tmp_path, monkeypatch):
    part = tmp_path / "bronze" / "2026-07-21"
    part.mkdir(parents=True)
    with (part / "events.ndjson").open("w") as f:
        for e in gen_events(400, seed=42, asof=ASOF):
            f.write(json.dumps(e) + "\n")
    monkeypatch.setattr(normalize, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(normalize, "SILVER", tmp_path / "silver")
    monkeypatch.setattr(normalize, "QUARANTINE", tmp_path / "quarantine")
    return tmp_path / "silver"


def test_last_mile_table_built_with_normalized_pins(india_lake):
    normalize.run(ASOF)
    with (india_lake / "last_mile.csv").open() as f:
        rows = list(csv.DictReader(f))
    assert rows, "seed 42 should produce India-bound out-for-delivery shipments"
    with_pin = [r for r in rows if r["pincode"]]
    assert with_pin, "expected normalized pincodes"
    assert all(len(r["pincode"]) == 6 and r["pincode"].isdigit() for r in with_pin)
    assert all(r["eway_bill_no"].startswith("EWB") for r in rows)


def test_eway_expiry_exception_fires(india_lake):
    normalize.run(ASOF)
    with (india_lake / "exception_queue.csv").open() as f:
        excs = [e for e in csv.DictReader(f) if e["exception_type"] == "EWAY_BILL_EXPIRED"]
    assert excs, "seed 42 should include an expired e-way bill on an undelivered shipment"
    assert all(float(e["age_hours"]) > 0 for e in excs)
    assert all("EWB" in e["detail"] for e in excs)
