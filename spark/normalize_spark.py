"""R12: Spark implementation of the bronze -> silver normalizer.

Correctness strategy: reuses the SAME pure `normalize_event` function as the
Python normalizer inside mapPartitions, so per-row semantics are identical by
construction; only dedupe/sort move to Spark. Output is written sorted with
the same columns, making `diff` against the Python normalizer's
shipment_events.csv a valid equality check (asserted on seed 42 in the
benchmark).

Usage:
    python -m spark.normalize_spark --bronze <dir> --out <csv> [--report <json>]
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

# Workers must run the same interpreter as the driver (PYTHON_VERSION_MISMATCH
# otherwise when the system python differs from the venv).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

from pipelines.normalize import EVENT_FIELDS, normalize_event


def parse_partition(lines):
    for line in lines:
        raw = json.loads(line)
        row, reason = normalize_event(raw)
        if reason:
            yield {"__quarantine__": reason}
        else:
            yield row


def run(bronze: Path, out: Path, report_path: Path | None) -> dict:
    spark = (SparkSession.builder.appName("manifest-normalize")
             .master("local[*]").config("spark.ui.enabled", "false").getOrCreate())
    try:
        rdd = spark.sparkContext.textFile(f"{bronze}/*/*.ndjson")
        parsed = rdd.mapPartitions(parse_partition).cache()

        quarantined = (parsed.filter(lambda r: "__quarantine__" in r)
                       .map(lambda r: (r["__quarantine__"], 1)).countByKey())
        good = parsed.filter(lambda r: "__quarantine__" not in r)
        df = spark.createDataFrame(good.map(lambda r: [r[f] for f in EVENT_FIELDS]),
                                   schema=EVENT_FIELDS)
        total_good = df.count()
        deduped = df.dropDuplicates(["event_id"])
        rows = deduped.orderBy("shipment_id", "actual_ts", "event_id").collect()

        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(EVENT_FIELDS)
            w.writerows([list(r) for r in rows])

        summary = {
            "events_read": total_good + sum(quarantined.values()),
            "events_processed": len(rows),
            "duplicates": total_good - len(rows),
            "quarantined": {"total": sum(quarantined.values()), **dict(quarantined)},
            "engine": "spark-local",
        }
        if report_path:
            report_path.write_text(json.dumps(summary, indent=2))
        return summary
    finally:
        spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bronze", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--report", default=None)
    args = ap.parse_args()
    summary = run(Path(args.bronze), Path(args.out),
                  Path(args.report) if args.report else None)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
