import csv
import json
from datetime import datetime, timezone

import pytest

from eta import predict
from generator.generate import gen_events
from pipelines import normalize

ASOF = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)
MAE_THRESHOLD_H = 24.0  # stated threshold, seed 42 (R10)


@pytest.fixture(scope="module")
def seed42_silver(tmp_path_factory):
    """Full seed-42 dataset through the real normalizer."""
    tmp = tmp_path_factory.mktemp("lake")
    part = tmp / "bronze" / "2026-07-21"
    part.mkdir(parents=True)
    with (part / "events.ndjson").open("w") as f:
        for e in gen_events(300, seed=42, asof=ASOF):
            f.write(json.dumps(e) + "\n")
    orig = (normalize.BRONZE, normalize.SILVER, normalize.QUARANTINE)
    normalize.BRONZE = tmp / "bronze"
    normalize.SILVER = tmp / "silver"
    normalize.QUARANTINE = tmp / "quarantine"
    try:
        normalize.run(ASOF)
    finally:
        normalize.BRONZE, normalize.SILVER, normalize.QUARANTINE = orig
    return tmp / "silver"


def test_mae_under_threshold_on_seed_42(seed42_silver):
    result = predict.evaluate(silver=seed42_silver)
    assert result["test_shipments"] > 20, "eval set too small to be meaningful"
    assert result["mae_hours"] is not None
    assert result["mae_hours"] < MAE_THRESHOLD_H, (
        f"MAE {result['mae_hours']}h breaches the {MAE_THRESHOLD_H}h threshold")


def test_predictions_have_explainability_columns(seed42_silver, tmp_path):
    summary = predict.run(silver=seed42_silver, gold=tmp_path)
    assert summary["inflight"] > 0
    with (tmp_path / "shipment_eta.csv").open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == summary["inflight"]
    r = rows[0]
    for col in ("predicted_delivery_ts", "median_offset_h", "offset_source",
                "live_adjustment_h", "adjustment_reason", "span_source"):
        assert r[col] != ""
    # a shipment that departed late must carry the observed-delay adjustment
    late = [r for r in rows if "observed_departure_delay" in r["adjustment_reason"]]
    assert late, "expected at least one late-departure adjustment in seed 42"


def test_prediction_is_deterministic(seed42_silver, tmp_path):
    predict.run(silver=seed42_silver, gold=tmp_path / "a")
    predict.run(silver=seed42_silver, gold=tmp_path / "b")
    assert ((tmp_path / "a" / "shipment_eta.csv").read_text()
            == (tmp_path / "b" / "shipment_eta.csv").read_text())
