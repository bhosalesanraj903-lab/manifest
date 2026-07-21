from datetime import datetime, timezone

from generator.generate import RAW_STATUS, gen_events

ASOF = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)


def test_deterministic_on_seed():
    a = gen_events(100, seed=42, asof=ASOF)
    b = gen_events(100, seed=42, asof=ASOF)
    assert a == b


def test_different_seeds_differ():
    assert gen_events(100, seed=42, asof=ASOF) != gen_events(100, seed=43, asof=ASOF)


def test_plants_all_quarantine_reasons_and_dupes():
    events = gen_events(400, seed=42, asof=ASOF)
    known_statuses = set(RAW_STATUS.values())

    bad_refs = [e for e in events if e["ref"].startswith("XXXX")]
    bad_statuses = [e for e in events if e["status"] not in known_statuses]
    bad_times = [e for e in events if e["timestamp"] == "not-a-time"]
    ids = [e["event_id"] for e in events]
    dupes = len(ids) - len(set(ids))

    assert bad_refs, "expected planted unmapped refs"
    assert bad_statuses, "expected planted unknown statuses"
    assert bad_times, "expected planted unparseable times"
    assert dupes > 0, "expected planted duplicate events"


def test_events_belong_to_requested_shipment_count():
    events = gen_events(50, seed=7, asof=ASOF)
    shipments = {e["ref"] for e in events if not e["ref"].startswith("XXXX")}
    assert 0 < len(shipments) <= 50
