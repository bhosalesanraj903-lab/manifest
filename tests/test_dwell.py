import csv
import json

import pytest

from ais import dwell


def pos(mmsi, lat, lon, sog, received):
    return {"MessageType": "PositionReport",
            "MetaData": {"MMSI": mmsi, "latitude": lat, "longitude": lon},
            "Message": {"PositionReport": {"Latitude": lat, "Longitude": lon, "Sog": sog}},
            "received_at": received}


def static(mmsi, name, imo, received):
    return {"MessageType": "ShipStaticData",
            "MetaData": {"MMSI": mmsi},
            "Message": {"ShipStaticData": {"Name": name, "ImoNumber": imo, "Type": 71}},
            "received_at": received}


# LA anchorage point inside la_long_beach box; Singapore point inside its box.
LA = (33.70, -118.20)
SG = (1.20, 103.80)

MESSAGES = (
    # vessel 111: anchored in LA across 3 distinct hours -> 3 anchor-hours
    [pos(111, *LA, 0.1, f"2026-07-21T0{h}:15:00Z") for h in (1, 2, 3)]
    # vessel 111: extra message same hour -> still counts once
    + [pos(111, *LA, 0.2, "2026-07-21T03:45:00Z")]
    # vessel 222: moving fast through LA -> not anchored
    + [pos(222, *LA, 12.0, "2026-07-21T02:00:00Z")]
    # vessel 333: anchored in Singapore 1 hour
    + [pos(333, *SG, 0.0, "2026-07-21T05:10:00Z")]
    # vessel 444: slow but outside any box -> ignored
    + [pos(444, 0.0, 0.0, 0.1, "2026-07-21T05:10:00Z")]
    # static data: two reports for 111, later one wins
    + [static(111, "EVER GIVEN  ", 9811000, "2026-07-21T01:00:00Z"),
       static(111, "EVER GIVEN", 9811000, "2026-07-21T09:00:00Z")]
)


@pytest.fixture
def lake(tmp_path, monkeypatch):
    day = tmp_path / "bronze" / "2026-07-21"
    day.mkdir(parents=True)
    with (day / "positions.ndjson").open("w") as f:
        for m in MESSAGES:
            f.write(json.dumps(m) + "\n")
    monkeypatch.setattr(dwell, "BRONZE_AIS", tmp_path / "bronze")
    monkeypatch.setattr(dwell, "SILVER", tmp_path / "silver")
    return tmp_path / "silver"


def read_csv(p):
    with p.open() as f:
        return list(csv.DictReader(f))


def test_dwell_accumulates_anchor_hours(lake):
    dwell.run("2026-07-21")
    rows = {(r["mmsi"], r["box"]): r for r in read_csv(lake / "vessel_dwell.csv")}
    assert rows[("111", "la_long_beach")]["anchor_hours"] == "3"
    assert rows[("333", "singapore_strait")]["anchor_hours"] == "1"
    assert ("222", "la_long_beach") not in rows  # moving vessel
    assert not any(m == "444" for m, _ in rows)  # outside all boxes


def test_port_congestion_summary(lake):
    dwell.run("2026-07-21")
    cong = {r["port"]: r for r in read_csv(lake / "port_congestion.csv")}
    assert cong["la_long_beach"]["vessels_at_anchor"] == "1"
    assert cong["la_long_beach"]["avg_dwell_h"] == "3.0"
    assert cong["singapore_strait"]["vessels_at_anchor"] == "1"


def test_dim_vessel_last_write_wins(lake):
    dwell.run("2026-07-21")
    vessels = read_csv(lake / "dim_vessel.csv")
    v111 = next(v for v in vessels if v["mmsi"] == "111")
    assert v111["name"] == "EVER GIVEN"
    assert v111["observed_at"] == "2026-07-21T09:00:00Z"
    assert v111["imo"] == "9811000"


def test_rerun_is_idempotent(lake):
    dwell.run("2026-07-21")
    first = read_csv(lake / "vessel_dwell.csv")
    dwell.run("2026-07-21")
    assert read_csv(lake / "vessel_dwell.csv") == first


def test_missing_day_does_not_crash(lake):
    s = dwell.run("2026-01-01")
    assert s["messages"] == 0
