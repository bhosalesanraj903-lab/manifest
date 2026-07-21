"""Bronze -> silver normalizer + exception detection.

Full rebuild each run: reads every bronze partition, so re-runs are idempotent
by construction (fine at this scale; R12 revisits with Spark).

Per event: parse the carrier-specific timestamp encoding, validate the shipment
ref against the carrier's pattern, map the raw status string to the canonical
event_type, dedupe on event_id. Rows that fail validation are QUARANTINED —
currently counted in the run summary by reason (unmapped_ref | unknown_status |
unparseable_time); R3 will write them to data/quarantine/.

Exception rules (silver exception_queue):
  MISSED_MILESTONE — gap since the shipment's last event exceeds the max normal
                     gap for its last milestone (a dark shipment), age_hours =
                     hours past that threshold.
  LATE_DEPARTURE   — vessel_departed actual > 24h after planned, age_hours =
                     hours since the late departure happened.
  CUSTOMS_DWELL    — vessel_arrived but no customs_release after 48h,
                     age_hours = hours past the 48h threshold.

exception_id is stable (hash of shipment|type) so downstream alerting (R1) can
diff runs and alert each exception once.

Usage:
    python -m pipelines.normalize [--asof ISO-UTC]
"""

import argparse
import csv
import json
import re
import time
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze" / "carrier"
SILVER = ROOT / "data" / "silver"

CARRIERS_CFG = yaml.safe_load((ROOT / "config" / "carriers.yml").read_text())
CARRIERS = CARRIERS_CFG["carriers"]

STATUS_MAP = {
    "BOOKING CONFIRMED": "booking_confirmed",
    "GATE-IN FULL": "gate_in",
    "LOAD ON VESSEL": "loaded_on_vessel",
    "VESSEL DEPARTURE": "vessel_departed",
    "VESSEL ARRIVAL": "vessel_arrived",
    "CUSTOMS RELEASE": "customs_release",
    "OUT FOR DELIVERY": "out_for_delivery",
    "DELIVERED": "delivered",
}

MILESTONE_ORDER = list(STATUS_MAP.values())

# Max normal hours of silence after each milestone before MISSED_MILESTONE.
# The departed->arrived ocean leg is legitimately quiet for ~2 weeks.
MAX_GAP_H = {
    "booking_confirmed": 60,
    "gate_in": 36,
    "loaded_on_vessel": 24,
    "vessel_departed": 16 * 24,
    "vessel_arrived": 72,
    "customs_release": 48,
    "out_for_delivery": 36,
    "delivered": None,  # terminal
}

LATE_DEPARTURE_H = 24
CUSTOMS_DWELL_H = 48

EVENT_FIELDS = ["event_id", "shipment_id", "carrier", "event_type",
                "planned_ts", "actual_ts", "origin", "dest"]
EXC_FIELDS = ["exception_id", "shipment_id", "carrier", "exception_type",
              "detected_at", "age_hours", "detail"]


def parse_ts(raw: str, fmt: str) -> datetime:
    if fmt == "iso8601":
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if fmt == "epoch_ms":
        return datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
    if fmt == "eu_slash":
        return datetime.strptime(raw, "%d/%m/%Y %H:%M").replace(tzinfo=timezone.utc)
    raise ValueError(f"unknown ts_format {fmt}")


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_event(raw: dict) -> tuple[dict | None, str | None]:
    """Return (silver_row, None) or (None, quarantine_reason)."""
    carrier = raw.get("carrier")
    cfg = CARRIERS.get(carrier)
    if cfg is None or not re.match(cfg["ref_pattern"], raw.get("ref", "")):
        return None, "unmapped_ref"

    event_type = STATUS_MAP.get(raw.get("status", ""))
    if event_type is None:
        return None, "unknown_status"

    try:
        actual = parse_ts(raw["timestamp"], cfg["ts_format"])
        planned = parse_ts(raw["planned"], cfg["ts_format"])
    except (ValueError, KeyError, OSError):
        return None, "unparseable_time"

    return {
        "event_id": raw["event_id"],
        "shipment_id": raw["ref"],
        "carrier": carrier,
        "event_type": event_type,
        "planned_ts": iso(planned),
        "actual_ts": iso(actual),
        "origin": raw.get("origin", ""),
        "dest": raw.get("dest", ""),
    }, None


def detect_exceptions(rows: list[dict], asof: datetime) -> list[dict]:
    by_shipment: dict[str, list[dict]] = {}
    for r in rows:
        by_shipment.setdefault(r["shipment_id"], []).append(r)

    exceptions = []

    def add(sid: str, carrier: str, etype: str, age_h: float, detail: str) -> None:
        exceptions.append({
            "exception_id": sha1(f"{sid}|{etype}".encode()).hexdigest()[:16],
            "shipment_id": sid,
            "carrier": carrier,
            "exception_type": etype,
            "detected_at": iso(asof),
            "age_hours": round(age_h, 1),
            "detail": detail,
        })

    for sid, evs in by_shipment.items():
        evs.sort(key=lambda r: MILESTONE_ORDER.index(r["event_type"]))
        by_type = {r["event_type"]: r for r in evs}
        last = evs[-1]
        carrier = last["carrier"]

        # MISSED_MILESTONE: shipment has gone quiet longer than normal.
        max_gap = MAX_GAP_H[last["event_type"]]
        if max_gap is not None:
            last_ts = datetime.strptime(last["actual_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            silence_h = (asof - last_ts).total_seconds() / 3600
            if silence_h > max_gap:
                add(sid, carrier, "MISSED_MILESTONE", silence_h - max_gap,
                    f"no event for {silence_h:.0f}h after {last['event_type']} (max normal {max_gap}h)")

        # LATE_DEPARTURE: departed well after plan.
        dep = by_type.get("vessel_departed")
        if dep:
            planned = datetime.strptime(dep["planned_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            actual = datetime.strptime(dep["actual_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            late_h = (actual - planned).total_seconds() / 3600
            if late_h > LATE_DEPARTURE_H:
                add(sid, carrier, "LATE_DEPARTURE", (asof - actual).total_seconds() / 3600,
                    f"departed {late_h:.0f}h after plan")

        # CUSTOMS_DWELL: arrived but no customs release.
        arr = by_type.get("vessel_arrived")
        if arr and "customs_release" not in by_type:
            arr_ts = datetime.strptime(arr["actual_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            dwell_h = (asof - arr_ts).total_seconds() / 3600
            if dwell_h > CUSTOMS_DWELL_H:
                add(sid, carrier, "CUSTOMS_DWELL", dwell_h - CUSTOMS_DWELL_H,
                    f"in customs {dwell_h:.0f}h with no release")

    exceptions.sort(key=lambda e: e["exception_id"])
    return exceptions


def run(asof: datetime) -> dict:
    t0 = time.monotonic()
    summary = {
        "asof": iso(asof), "events_read": 0, "events_processed": 0, "duplicates": 0,
        "quarantined": {"total": 0, "unmapped_ref": 0, "unknown_status": 0, "unparseable_time": 0},
        "exceptions": 0, "runtime_s": 0.0,
    }

    rows: list[dict] = []
    seen: set[str] = set()
    for path in sorted(BRONZE.glob("*/*.ndjson")):
        for line in path.read_text().splitlines():
            summary["events_read"] += 1
            row, reason = normalize_event(json.loads(line))
            if reason:
                summary["quarantined"][reason] += 1
                summary["quarantined"]["total"] += 1
                continue
            if row["event_id"] in seen:
                summary["duplicates"] += 1
                continue
            seen.add(row["event_id"])
            rows.append(row)

    rows.sort(key=lambda r: (r["shipment_id"], r["actual_ts"], r["event_id"]))
    exceptions = detect_exceptions(rows, asof)

    SILVER.mkdir(parents=True, exist_ok=True)
    with (SILVER / "shipment_events.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
        w.writeheader()
        w.writerows(rows)
    with (SILVER / "exception_queue.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=EXC_FIELDS)
        w.writeheader()
        w.writerows(exceptions)

    summary["events_processed"] = len(rows)
    summary["exceptions"] = len(exceptions)
    summary["runtime_s"] = round(time.monotonic() - t0, 2)
    (SILVER / "_run_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asof", default=None, help="ISO UTC instant to evaluate exceptions at (default: now)")
    args = ap.parse_args()
    asof = (datetime.fromisoformat(args.asof).replace(tzinfo=timezone.utc)
            if args.asof else datetime.now(timezone.utc))
    print(json.dumps(run(asof)))


if __name__ == "__main__":
    main()
