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


def hours_since_last_landed() -> float | None:
    """Age in hours of the newest bronze carrier file (None if no bronze)."""
    files = list(BRONZE.glob("*/*.ndjson"))
    if not files:
        return None
    newest = max(f.stat().st_mtime for f in files)
    return (datetime.now(timezone.utc).timestamp() - newest) / 3600


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
    ap.add_argument("--max-silence-hours", type=float, default=3.0,
                    help="fail if nothing landed AND the newest bronze file is older "
                         "than this (game day R11a: a silent feed must not stay green)")
    args = ap.parse_args()
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary = ingest(date)

    silence = hours_since_last_landed()
    summary["feed_silence_hours"] = round(silence, 2) if silence is not None else None
    print(json.dumps(summary))

    if summary["files_rejected"]:
        sys.exit(1)  # rejects are visible in the exit code, not silent
    if summary["files_landed"] == 0 and (silence is None or silence > args.max_silence_hours):
        print(f"FEED SILENT: nothing landed and newest bronze is "
              f"{summary['feed_silence_hours']}h old (max {args.max_silence_hours}h)",
              file=sys.stderr)
        sys.exit(2)  # DAG task fails -> Slack failure callback fires


if __name__ == "__main__":
    main()
