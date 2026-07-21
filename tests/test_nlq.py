import json
from pathlib import Path

import pytest

from nlq import app

EVAL = Path(__file__).parents[1] / "nlq" / "eval.jsonl"
GOLD = Path(__file__).parents[1] / "data" / "gold"

needs_marts = pytest.mark.skipif(
    not (GOLD / "carrier_scorecard.csv").exists(),
    reason="gold marts not exported (run `make dbt`)")


def load_eval():
    return [json.loads(l) for l in EVAL.read_text().splitlines()]


@needs_marts
def test_eval_set_full_pass():
    """Every eval question must behave as specified: right metric answered
    with attribution, or a refusal for out-of-scope questions."""
    failures = []
    for case in load_eval():
        r = app.answer(case["question"])
        exp = case["expect"]
        if exp["kind"] == "refusal":
            if r.get("refusal") is None:
                failures.append((case["question"], "expected refusal, got answer"))
            continue
        if r.get("answer") is None:
            failures.append((case["question"], f"expected answer, got {r.get('refusal')}"))
            continue
        if r["attribution"]["metric"] != exp["metric"]:
            failures.append((case["question"],
                             f"metric {r['attribution']['metric']} != {exp['metric']}"))
        for dim, code in exp.get("filters", {}).items():
            if r["attribution"]["filters"].get(dim) != code:
                failures.append((case["question"], f"filter {dim} missed"))
        if exp["kind"] == "superlative" and "entity" not in r:
            failures.append((case["question"], "expected a superlative winner entity"))
    assert not failures, failures


@needs_marts
def test_every_answer_has_attribution():
    r = app.answer("which carrier has the best on-time rate?")
    a = r["attribution"]
    assert a["mart"].startswith("dbt:")
    assert a["rows_used"] > 0
    assert a["definition"]


def test_out_of_scope_is_refused_without_marts():
    r = app.answer("what is the meaning of life?")
    assert r["answer"] is None and "governed metrics" in r["refusal"]
