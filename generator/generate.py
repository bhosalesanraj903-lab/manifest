"""Synthetic carrier tracking-event generator.

Emits raw carrier feed events (ndjson) into the landing zone, mimicking what a
carrier EDI/API integration would deliver: per-carrier reference formats,
per-carrier timestamp encodings, and a small rate of real-world dirt —
duplicates, unmapped references, unknown status codes, unparseable timestamps.

Deterministic for a given (--seed, --asof): no wall-clock reads in the data path.

Usage:
    python -m generator.generate --count 500 --seed 42
"""

import argparse
import hashlib
import json
import random
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing" / "carrier"

CARRIERS_CFG = yaml.safe_load((ROOT / "config" / "carriers.yml").read_text())
CARRIERS = list(CARRIERS_CFG["carriers"])
PORTS = ["USLAX", "USLGB", "SGSIN", "INNSA", "INMUN", "CNSHA", "NLRTM", "DEHAM"]

# Canonical milestone -> raw status string as carriers actually send them.
RAW_STATUS = {
    "booking_confirmed": "BOOKING CONFIRMED",
    "gate_in": "GATE-IN FULL",
    "loaded_on_vessel": "LOAD ON VESSEL",
    "vessel_departed": "VESSEL DEPARTURE",
    "vessel_arrived": "VESSEL ARRIVAL",
    "customs_release": "CUSTOMS RELEASE",
    "out_for_delivery": "OUT FOR DELIVERY",
    "delivered": "DELIVERED",
}
MILESTONES = list(RAW_STATUS)

# Hours after booking each milestone is planned, with jitter applied per shipment.
PLAN_OFFSETS_H = {
    "booking_confirmed": 0,
    "gate_in": 48,
    "loaded_on_vessel": 72,
    "vessel_departed": 84,
    "vessel_arrived": 84 + 14 * 24,
    "customs_release": 84 + 14 * 24 + 36,
    "out_for_delivery": 84 + 14 * 24 + 60,
    "delivered": 84 + 14 * 24 + 72,
}

DUP_RATE = 0.02
BAD_REF_RATE = 0.005
BAD_STATUS_RATE = 0.005
BAD_TIME_RATE = 0.005

# Shipment behavior profiles: weight, departure delay hours, customs dwell extra
# hours, and whether the shipment silently stalls (stops emitting events).
PROFILES = [
    ("on_time", 0.70, 0, 0, False),
    ("late_departure", 0.12, 30, 0, False),
    ("customs_stuck", 0.10, 0, 60, False),
    ("gone_dark", 0.08, 0, 0, True),
]


def make_ref(rng: random.Random, carrier: str) -> str:
    if carrier == "MAEU":
        return "MAEU" + "".join(rng.choices(string.digits, k=9))
    if carrier == "MSCU":
        return "MSCU" + "".join(rng.choices(string.digits, k=7))
    if carrier == "CMDU":
        return "CMDU" + "".join(rng.choices(string.ascii_uppercase, k=2)) + "".join(rng.choices(string.digits, k=7))
    if carrier == "HLCU":
        return ("HLCU" + "".join(rng.choices(string.digits, k=3))
                + rng.choice(string.ascii_uppercase) + "".join(rng.choices(string.digits, k=6)))
    if carrier == "ONEY":
        return "ONEY" + "".join(rng.choices(string.ascii_uppercase + string.digits, k=10))
    raise ValueError(carrier)


def encode_ts(dt: datetime, fmt: str) -> str:
    if fmt == "iso8601":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if fmt == "epoch_ms":
        return str(int(dt.timestamp() * 1000))
    if fmt == "eu_slash":
        return dt.strftime("%d/%m/%Y %H:%M")
    raise ValueError(fmt)


def event_id(ref: str, milestone: str) -> str:
    return hashlib.sha1(f"{ref}|{milestone}".encode()).hexdigest()[:16]


def gen_events(count: int, seed: int, asof: datetime) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    weights = [p[1] for p in PROFILES]

    for _ in range(count):
        carrier = rng.choice(CARRIERS)
        ts_fmt = CARRIERS_CFG["carriers"][carrier]["ts_format"]
        ref = make_ref(rng, carrier)
        origin, dest = rng.sample(PORTS, 2)
        profile, _, dep_delay, customs_extra, gone_dark = rng.choices(PROFILES, weights=weights)[0]

        booked = asof - timedelta(hours=rng.uniform(24, 26 * 24))
        jitter = rng.uniform(0.9, 1.1)
        dark_after = rng.randint(1, 4) if gone_dark else len(MILESTONES)

        for i, ms in enumerate(MILESTONES):
            planned = booked + timedelta(hours=PLAN_OFFSETS_H[ms] * jitter)
            delay = timedelta(hours=rng.uniform(-2, 4))
            if ms in ("vessel_departed", "vessel_arrived"):
                delay += timedelta(hours=dep_delay)
            if ms in ("customs_release", "out_for_delivery", "delivered"):
                delay += timedelta(hours=dep_delay + customs_extra)
            actual = planned + delay

            # Event exists only if it has actually happened and the shipment
            # hasn't gone dark. Overdue planned milestones with no event are
            # what the normalizer turns into exceptions.
            if actual > asof or i >= dark_after:
                continue

            record = {
                "event_id": event_id(ref, ms),
                "carrier": carrier,
                "ref": ref,
                "status": RAW_STATUS[ms],
                "timestamp": encode_ts(actual, ts_fmt),
                "planned": encode_ts(planned, ts_fmt),
                "origin": origin,
                "dest": dest,
            }

            # Plant dirt at low rates (quarantine reasons per R3).
            r = rng.random()
            if r < BAD_REF_RATE:
                record["ref"] = "XXXX" + record["ref"][4:]
            elif r < BAD_REF_RATE + BAD_STATUS_RATE:
                record["status"] = "STATUS_" + str(rng.randint(100, 999))
            elif r < BAD_REF_RATE + BAD_STATUS_RATE + BAD_TIME_RATE:
                record["timestamp"] = "not-a-time"

            events.append(record)
            if rng.random() < DUP_RATE:
                events.append(dict(record))

    rng.shuffle(events)
    return events


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--count", type=int, default=200, help="number of shipments")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--asof", default=None, help="ISO UTC instant the feed is generated at (default: now)")
    ap.add_argument("--out", default=None, help="output ndjson path (default: data/landing/carrier/events.ndjson)")
    args = ap.parse_args()

    asof = (datetime.fromisoformat(args.asof).replace(tzinfo=timezone.utc)
            if args.asof else datetime.now(timezone.utc))
    events = gen_events(args.count, args.seed, asof)

    out = Path(args.out) if args.out else LANDING / "events.ndjson"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(json.dumps({"shipments": args.count, "events_written": len(events),
                      "seed": args.seed, "asof": asof.isoformat(), "out": str(out)}))


if __name__ == "__main__":
    main()
