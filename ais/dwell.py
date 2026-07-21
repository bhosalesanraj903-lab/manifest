"""R5: hourly AIS dwell/congestion job.

Reads a day's bronze AIS positions. A vessel is "at anchor" in a box for a
given hour if it reported sog < 0.5 kn inside that box during that hour.
Anchor-hours accumulate into two silver tables (upserted by date, so re-runs
are idempotent):

  port_congestion.csv  port(box), date, vessels_at_anchor, avg_dwell_h
  vessel_dwell.csv     mmsi, date, box, anchor_hours   (feeds VESSEL_STALLED, R6)

ShipStaticData messages upsert dim_vessel.csv (mmsi, imo, name, type,
observed_at; last-write-wins on observed_at).

Usage:
    python -m ais.dwell [--date YYYY-MM-DD]
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BRONZE_AIS = ROOT / "data" / "bronze" / "ais"
SILVER = ROOT / "data" / "silver"

PORTS_CFG = yaml.safe_load((ROOT / "config" / "ports.yml").read_text())
SOG_ANCHOR_KN = 0.5

CONGESTION_FIELDS = ["port", "date", "vessels_at_anchor", "avg_dwell_h"]
DWELL_FIELDS = ["mmsi", "date", "box", "anchor_hours"]
VESSEL_FIELDS = ["mmsi", "imo", "name", "type", "observed_at"]


def in_box(lat: float, lon: float, box: list[list[float]]) -> bool:
    (lat0, lon0), (lat1, lon1) = box
    return lat0 <= lat <= lat1 and lon0 <= lon <= lon1


def box_for(lat: float, lon: float) -> str | None:
    for name, cfg in PORTS_CFG["ais_boxes"].items():
        if in_box(lat, lon, cfg["box"]):
            return name
    return None


def upsert_csv(path: Path, fields: list[str], rows: list[dict],
               drop_where: dict | None = None, key: list[str] | None = None) -> None:
    """Rewrite a CSV keeping existing rows except those matching drop_where /
    duplicated keys, then append rows. Idempotent building block."""
    existing: list[dict] = []
    if path.exists():
        with path.open() as f:
            existing = list(csv.DictReader(f))
    if drop_where:
        existing = [r for r in existing
                    if not all(r.get(k) == v for k, v in drop_where.items())]
    if key:
        new_keys = {tuple(r[k] for k in key) for r in rows}
        existing = [r for r in existing if tuple(r.get(k) for k in key) not in new_keys]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(existing + rows)


def run(date: str) -> dict:
    src = BRONZE_AIS / date / "positions.ndjson"
    summary = {"date": date, "messages": 0, "position_reports": 0,
               "static_reports": 0, "boxes": {}}
    if not src.exists():
        print(f"no AIS bronze for {date} ({src})", file=sys.stderr)
        return summary

    anchor_hours: set[tuple[str, str, str]] = set()  # (mmsi, box, hour)
    statics: dict[str, dict] = {}

    for line in src.read_text().splitlines():
        summary["messages"] += 1
        msg = json.loads(line)
        # aisstream payloads use "MetaData"; their docs say "Metadata" — accept both.
        meta = msg.get("MetaData") or msg.get("Metadata") or {}
        received = msg.get("received_at", "")
        if msg.get("MessageType") == "PositionReport":
            summary["position_reports"] += 1
            body = msg.get("Message", {}).get("PositionReport", {})
            mmsi = str(meta.get("MMSI") or body.get("UserID") or "")
            lat = body.get("Latitude", meta.get("latitude") or meta.get("Latitude"))
            lon = body.get("Longitude", meta.get("longitude") or meta.get("Longitude"))
            sog = body.get("Sog")
            if lat is None or lon is None or sog is None or sog >= SOG_ANCHOR_KN:
                continue
            box = box_for(lat, lon)
            if box:
                anchor_hours.add((mmsi, box, received[:13]))  # YYYY-MM-DDTHH
        elif msg.get("MessageType") == "ShipStaticData":
            summary["static_reports"] += 1
            body = msg.get("Message", {}).get("ShipStaticData", {})
            mmsi = str(meta.get("MMSI") or body.get("UserID") or "")
            prev = statics.get(mmsi)
            if prev is None or received > prev["observed_at"]:
                statics[mmsi] = {
                    "mmsi": mmsi,
                    "imo": str(body.get("ImoNumber", "")),
                    "name": (body.get("Name") or "").strip(),
                    "type": str(body.get("Type", "")),
                    "observed_at": received,
                }

    per_vessel_box: dict[tuple[str, str], int] = {}
    for mmsi, box, _hour in anchor_hours:
        per_vessel_box[(mmsi, box)] = per_vessel_box.get((mmsi, box), 0) + 1

    dwell_rows = [{"mmsi": m, "date": date, "box": b, "anchor_hours": h}
                  for (m, b), h in sorted(per_vessel_box.items())]
    congestion_rows = []
    for box in PORTS_CFG["ais_boxes"]:
        vessels = [h for (m, b), h in per_vessel_box.items() if b == box]
        if vessels:
            congestion_rows.append({
                "port": box, "date": date, "vessels_at_anchor": len(vessels),
                "avg_dwell_h": round(sum(vessels) / len(vessels), 1),
            })
        summary["boxes"][box] = len(vessels)

    SILVER.mkdir(parents=True, exist_ok=True)
    upsert_csv(SILVER / "vessel_dwell.csv", DWELL_FIELDS, dwell_rows,
               drop_where={"date": date})
    upsert_csv(SILVER / "port_congestion.csv", CONGESTION_FIELDS, congestion_rows,
               drop_where={"date": date})
    if statics:
        upsert_csv(SILVER / "dim_vessel.csv", VESSEL_FIELDS,
                   sorted(statics.values(), key=lambda r: r["mmsi"]), key=["mmsi"])
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary = run(date)
    print(json.dumps(summary))
    if os.environ.get("PUSHGATEWAY_URL"):
        import time

        import requests
        url = os.environ["PUSHGATEWAY_URL"]
        body = ("# TYPE manifest_ais_messages gauge\n"
                f"manifest_ais_messages {summary['messages']}\n"
                "# TYPE manifest_ais_last_run_timestamp gauge\n"
                f"manifest_ais_last_run_timestamp {int(time.time())}\n")
        try:
            requests.put(f"{url}/metrics/job/ais_dwell", data=body, timeout=5).raise_for_status()
        except requests.RequestException as e:
            print(f"metrics push failed ({e!r}); continuing", file=sys.stderr)


if __name__ == "__main__":
    main()
