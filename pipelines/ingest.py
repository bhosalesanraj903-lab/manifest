"""Landing -> bronze ingest.

Takes raw carrier feed files from data/landing/carrier/, verifies each line is
valid JSON (a corrupt line fails the whole file into data/landing/rejected/ —
never silently dropped), and lands good files into the dated bronze partition
data/bronze/carrier/YYYY-MM-DD/ with a content hash in the filename so re-runs
are idempotent.

Usage:
    python -m pipelines.ingest [--date YYYY-MM-DD]
"""

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing" / "carrier"
REJECTED = ROOT / "data" / "landing" / "rejected"
BRONZE = ROOT / "data" / "bronze" / "carrier"


def ingest(date: str) -> dict:
    part = BRONZE / date
    part.mkdir(parents=True, exist_ok=True)
    summary = {"date": date, "files_landed": 0, "files_rejected": 0, "events": 0}

    for src in sorted(LANDING.glob("*.ndjson")):
        lines = src.read_text().splitlines()
        try:
            for ln in lines:
                json.loads(ln)
        except json.JSONDecodeError:
            REJECTED.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), REJECTED / src.name)
            summary["files_rejected"] += 1
            continue

        digest = hashlib.sha1(src.read_bytes()).hexdigest()[:12]
        dest = part / f"events_{digest}.ndjson"
        if not dest.exists():
            shutil.move(str(src), dest)
        else:
            src.unlink()  # identical content already landed
        summary["files_landed"] += 1
        summary["events"] += len(lines)

    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=None, help="bronze partition date (default: today UTC)")
    args = ap.parse_args()
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary = ingest(date)
    print(json.dumps(summary))
    if summary["files_rejected"]:
        sys.exit(1)  # rejects are visible in the exit code, not silent


if __name__ == "__main__":
    main()
