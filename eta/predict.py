"""R10: ETA v1 for in-flight shipments.

    predicted_delivery = planned_delivery (reconstructed)
                       + lane/carrier median offset  (eta_baseline mart, R9)
                       + live adjustment             (observed late departure,
                                                      VESSEL_STALLED, congestion)

planned_delivery is reconstructed as the shipment's first planned timestamp +
the lane's median planned span (booking -> delivered) learned from completed
shipments, since the synthetic feed (like many carrier feeds) only carries
planned times on already-emitted events.

Output: data/gold/shipment_eta.csv with the prediction AND every input used
(explainability columns — offset source, span source, adjustment reason).

Evaluation (--evaluate): completed shipments are split by hash into train
(learn spans/offsets) and test (events after vessel_departed hidden, predict
delivery, compare to the actual). MAE is reported and asserted in tests to be
under 24h on seed 42.

Usage:
    python -m eta.predict [--evaluate]
"""

import argparse
import csv
import json
import statistics
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "data" / "silver"
GOLD = ROOT / "data" / "gold"

MILESTONE_ORDER = ["booking_confirmed", "gate_in", "loaded_on_vessel",
                   "vessel_departed", "vessel_arrived", "customs_release",
                   "out_for_delivery", "delivered"]

STALL_ADJ_H = 24.0
CONGESTION_ADJ_H = 12.0

ETA_FIELDS = ["shipment_id", "carrier", "lane", "last_event_type",
              "predicted_delivery_ts", "planned_anchor_ts", "planned_span_h",
              "span_source", "median_offset_h", "offset_source",
              "live_adjustment_h", "adjustment_reason"]


def _ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def load_events(silver: Path) -> dict[str, list[dict]]:
    with (silver / "shipment_events.csv").open() as f:
        rows = list(csv.DictReader(f))
    by: dict[str, list[dict]] = {}
    for r in rows:
        by.setdefault(r["shipment_id"], []).append(r)
    for evs in by.values():
        evs.sort(key=lambda r: MILESTONE_ORDER.index(r["event_type"]))
    return by


def load_exceptions(silver: Path) -> dict[str, list[dict]]:
    path = silver / "exception_queue.csv"
    by: dict[str, list[dict]] = {}
    if path.exists():
        with path.open() as f:
            for r in csv.DictReader(f):
                by.setdefault(r["shipment_id"], []).append(r)
    return by


def learn(completed: dict[str, list[dict]]) -> dict:
    """Median planned span (booking->delivered) and delivery offset per lane/carrier."""
    spans: dict[tuple, list[float]] = {}
    offsets: dict[tuple, list[float]] = {}
    ratios: list[float] = []
    for evs in completed.values():
        first, last = evs[0], evs[-1]
        key = (last["carrier"], f"{last['origin']}-{last['dest']}")
        span_h = (_ts(last["planned_ts"]) - _ts(first["planned_ts"])).total_seconds() / 3600
        off_h = (_ts(last["actual_ts"]) - _ts(last["planned_ts"])).total_seconds() / 3600
        # Jitter-invariant schedule ratio: (first->delivered)/(first->departed)
        # planned gaps. A shipment's own visible planned gap times this ratio
        # recovers ITS planned delivery, immune to per-shipment schedule scale.
        dep_ev = next((e for e in evs if e["event_type"] == "vessel_departed"), None)
        if dep_ev:
            dep_gap = (_ts(dep_ev["planned_ts"]) - _ts(first["planned_ts"])).total_seconds() / 3600
            if dep_gap > 0 and span_h > 0:
                ratios.append(span_h / dep_gap)
        # Learn the offset RESIDUAL after the observed departure delay: the
        # live adjustment re-adds that delay at predict time, so leaving it in
        # here would double-count it.
        by_type = {e["event_type"]: e for e in evs}
        dep = by_type.get("vessel_departed")
        if dep:
            dep_delay = (_ts(dep["actual_ts"]) - _ts(dep["planned_ts"])).total_seconds() / 3600
            if dep_delay > 1:
                off_h -= dep_delay
        spans.setdefault(key, []).append(span_h)
        offsets.setdefault(key, []).append(off_h)

    def medians(d):
        return {k: statistics.median(v) for k, v in d.items()}

    all_spans = [s for v in spans.values() for s in v]
    all_offsets = [o for v in offsets.values() for o in v]
    return {
        "span_by_lane": medians(spans),
        "offset_by_lane": medians(offsets),
        "span_global": statistics.median(all_spans) if all_spans else 0.0,
        "offset_global": statistics.median(all_offsets) if all_offsets else 0.0,
        "departed_ratio": statistics.median(ratios) if ratios else None,
    }


def load_baseline_offsets(gold: Path) -> dict[tuple, float]:
    """Delivery offsets from the dbt eta_baseline mart when it's been exported."""
    path = gold / "eta_baseline.csv"
    out: dict[tuple, float] = {}
    if path.exists():
        with path.open() as f:
            for r in csv.DictReader(f):
                if r["event_type"] == "delivered":
                    out[(r["carrier"], r["lane"])] = float(r["median_offset_h"])
    return out


def predict_one(evs: list[dict], excs: list[dict], model: dict,
                mart_offsets: dict[tuple, float]) -> dict:
    first, last = evs[0], evs[-1]
    carrier, lane = last["carrier"], f"{last['origin']}-{last['dest']}"
    key = (carrier, lane)

    by_type_planned = {e["event_type"]: e for e in evs}
    dep_vis = by_type_planned.get("vessel_departed")
    span_h = None
    if dep_vis and model["departed_ratio"]:
        dep_gap = (_ts(dep_vis["planned_ts"]) - _ts(first["planned_ts"])).total_seconds() / 3600
        if dep_gap > 0:
            span_h = dep_gap * model["departed_ratio"]
            span_source = f"planned_ratio(x{model['departed_ratio']:.2f})"
    if span_h is None:
        span_h = model["span_by_lane"].get(key)
        span_source = f"lane_median({lane})" if span_h is not None else "global_median"
        if span_h is None:
            span_h = model["span_global"]

    if key in mart_offsets:
        offset_h, offset_source = mart_offsets[key], "eta_baseline_mart"
    elif key in model["offset_by_lane"]:
        offset_h, offset_source = model["offset_by_lane"][key], f"lane_median({lane})"
    else:
        offset_h, offset_source = model["offset_global"], "global_median"

    adj_h, reasons = 0.0, []
    by_type = {e["event_type"]: e for e in evs}
    dep = by_type.get("vessel_departed")
    if dep:
        dep_delay = (_ts(dep["actual_ts"]) - _ts(dep["planned_ts"])).total_seconds() / 3600
        if dep_delay > 1:
            adj_h += dep_delay
            reasons.append(f"observed_departure_delay(+{dep_delay:.1f}h)")
    for e in excs:
        if e["exception_type"] == "VESSEL_STALLED":
            adj_h += STALL_ADJ_H
            reasons.append(f"vessel_stalled(+{STALL_ADJ_H}h)")
        elif e.get("probable_cause") == "congestion":
            adj_h += CONGESTION_ADJ_H
            reasons.append(f"congestion(+{CONGESTION_ADJ_H}h)")

    anchor = _ts(first["planned_ts"])
    predicted = anchor + timedelta(hours=span_h + offset_h + adj_h)
    return {
        "shipment_id": last["shipment_id"], "carrier": carrier, "lane": lane,
        "last_event_type": last["event_type"],
        "predicted_delivery_ts": _iso(predicted),
        "planned_anchor_ts": first["planned_ts"],
        "planned_span_h": round(span_h, 1), "span_source": span_source,
        "median_offset_h": round(offset_h, 1), "offset_source": offset_source,
        "live_adjustment_h": round(adj_h, 1),
        "adjustment_reason": ";".join(reasons) or "none",
    }


def run(silver: Path = SILVER, gold: Path = GOLD) -> dict:
    events = load_events(silver)
    exceptions = load_exceptions(silver)
    completed = {s: e for s, e in events.items() if e[-1]["event_type"] == "delivered"}
    inflight = {s: e for s, e in events.items() if s not in completed}

    model = learn(completed)
    mart_offsets = load_baseline_offsets(gold)

    rows = [predict_one(evs, exceptions.get(sid, []), model, mart_offsets)
            for sid, evs in sorted(inflight.items())]
    gold.mkdir(parents=True, exist_ok=True)
    with (gold / "shipment_eta.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ETA_FIELDS)
        w.writeheader()
        w.writerows(rows)
    return {"inflight": len(rows), "completed_trained_on": len(completed),
            "mart_offsets_used": len(mart_offsets)}


def evaluate(silver: Path = SILVER, cutoff: str = "vessel_departed") -> dict:
    """Hold-out eval: hide events after `cutoff` on test-half completed
    shipments, predict delivery, MAE vs actual."""
    events = load_events(silver)
    exceptions = load_exceptions(silver)
    completed = {s: e for s, e in events.items() if e[-1]["event_type"] == "delivered"}

    train = {s: e for s, e in completed.items() if int(sha1(s.encode()).hexdigest(), 16) % 2 == 0}
    test = {s: e for s, e in completed.items() if s not in train}
    model = learn(train)

    cut_idx = MILESTONE_ORDER.index(cutoff)
    errors = []
    for sid, evs in test.items():
        visible = [e for e in evs if MILESTONE_ORDER.index(e["event_type"]) <= cut_idx]
        if not visible:
            continue
        pred = predict_one(visible, exceptions.get(sid, []), model, {})
        actual = _ts(evs[-1]["actual_ts"])
        err_h = abs((_ts(pred["predicted_delivery_ts"]) - actual).total_seconds() / 3600)
        errors.append(err_h)

    mae = round(statistics.mean(errors), 2) if errors else None
    return {"test_shipments": len(errors), "train_shipments": len(train), "mae_hours": mae}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--evaluate", action="store_true")
    args = ap.parse_args()
    print(json.dumps(evaluate() if args.evaluate else run()))


if __name__ == "__main__":
    main()
