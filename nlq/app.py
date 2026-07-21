"""R16: NL-query capstone.

Answers natural-language questions ONLY from the dbt marts via the semantic
layer in nlq/metrics.yml. Every answer carries attribution: which mart, which
column, which rows, and the governed metric definition. Questions that don't
resolve to a defined metric are refused — by design, this app cannot
hallucinate: it either computes from a governed definition or says so.

Usage:
    python -m nlq.app "which carrier has the best on-time rate?"
    python -m nlq.app --serve   # tiny REPL
"""

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold"
CFG = yaml.safe_load((ROOT / "nlq" / "metrics.yml").read_text())

SUPERLATIVE_BEST = ("best", "highest", "top", "most")
SUPERLATIVE_WORST = ("worst", "lowest", "bottom", "least")
# For these metrics a LOWER value is better.
LOWER_IS_BETTER = {"mis_scan_rate", "avg_customs_dwell", "eta_offset"}


def load_mart(name: str) -> list[dict]:
    path = GOLD / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"mart {name} not exported — run `make dbt` first ({path})")
    with path.open() as f:
        return list(csv.DictReader(f))


def match_metric(q: str) -> str | None:
    ql = q.lower()
    best, best_len = None, 0
    for name, m in CFG["metrics"].items():
        for phrase in [name.replace("_", " ")] + m.get("synonyms", []):
            if phrase in ql and len(phrase) > best_len:
                best, best_len = name, len(phrase)
    return best


def match_dimension(q: str) -> dict:
    ql = q.lower()
    found = {}
    for dim, spec in CFG["dimensions"].items():
        hits = [(phrase, code) for phrase, code in spec["values"].items() if phrase in ql]
        if hits:
            phrase, code = max(hits, key=lambda h: len(h[0]))
            found[dim] = code
    return found


def aggregate(rows: list[dict], metric: dict) -> float | None:
    vals = [(float(r[metric["column"]]), r) for r in rows if r[metric["column"]] not in ("", None)]
    if not vals:
        return None
    if metric["agg"] == "sum":
        return sum(v for v, _ in vals)
    if metric["agg"] == "avg":
        return round(statistics.mean(v for v, _ in vals), 2)
    if metric["agg"] == "median_of_medians":
        return round(statistics.median(v for v, _ in vals), 2)
    if metric["agg"] == "weighted_avg":
        wcol = metric["weight_column"]
        total_w = sum(float(r[wcol]) for _, r in vals)
        if total_w == 0:
            return None
        return round(sum(v * float(r[wcol]) for v, r in vals) / total_w, 2)
    raise ValueError(metric["agg"])


def answer(question: str) -> dict:
    metric_name = match_metric(question)
    if metric_name is None:
        return {"answer": None,
                "refusal": "I can only answer questions about governed metrics: "
                           + ", ".join(CFG["metrics"]) + ". This question doesn't "
                           "match any of them.",
                "attribution": None}

    metric = CFG["metrics"][metric_name]
    dims = match_dimension(question)
    rows = load_mart(metric["mart"])

    if "carrier" in dims:
        rows = [r for r in rows if r.get("carrier") == dims["carrier"]]
    if "port" in dims:
        rows = [r for r in rows if dims["port"] in r.get("lane", "")]

    ql = question.lower()
    superlative = (any(w in ql for w in SUPERLATIVE_BEST) and "best"
                   or any(w in ql for w in SUPERLATIVE_WORST) and "worst" or None)

    if superlative and "carrier" in {c for r in rows for c in (["carrier"] if "carrier" in r else [])}:
        by_carrier = {}
        for r in rows:
            by_carrier.setdefault(r["carrier"], []).append(r)
        scored = {c: aggregate(rs, metric) for c, rs in by_carrier.items()}
        scored = {c: v for c, v in scored.items() if v is not None}
        if scored:
            lower_better = metric_name in LOWER_IS_BETTER
            pick_min = (superlative == "best") == lower_better
            winner = min(scored, key=scored.get) if pick_min else max(scored, key=scored.get)
            return {
                "answer": f"{winner}: {scored[winner]}{metric['unit']}",
                "value": scored[winner], "entity": winner,
                "all_values": dict(sorted(scored.items())),
                "attribution": _attr(metric_name, metric, len(rows), dims),
            }

    value = aggregate(rows, metric)
    if value is None:
        return {"answer": None,
                "refusal": f"No rows in {metric['mart']} match the filters {dims}.",
                "attribution": _attr(metric_name, metric, 0, dims)}
    return {"answer": f"{value}{metric['unit']}", "value": value,
            "attribution": _attr(metric_name, metric, len(rows), dims)}


def _attr(name: str, metric: dict, n_rows: int, dims: dict) -> dict:
    return {"metric": name, "mart": f"dbt:{metric['mart']}", "column": metric["column"],
            "aggregation": metric["agg"], "rows_used": n_rows, "filters": dims,
            "definition": metric["definition"]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("question", nargs="*")
    ap.add_argument("--serve", action="store_true")
    args = ap.parse_args()

    if args.serve:
        print("manifest nlq — ask about: " + ", ".join(CFG["metrics"]) + " (ctrl-d to exit)")
        for line in sys.stdin:
            if line.strip():
                print(json.dumps(answer(line.strip()), indent=2))
        return
    if not args.question:
        ap.error("provide a question or --serve")
    print(json.dumps(answer(" ".join(args.question)), indent=2))


if __name__ == "__main__":
    main()
