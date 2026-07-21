import json

import pytest

from pipelines import enrich, normalize


@pytest.fixture
def weather_bronze(tmp_path, monkeypatch):
    day = tmp_path / "weather" / "2026-07-21"
    day.mkdir(parents=True)
    (day / "nws.json").write_text(json.dumps({
        "results": {"USLAX": {"features": [{"properties": {"event": "High Wind Warning"}}]},
                    "USLGB": {"features": []}},
        "errors": {}}))
    (day / "open_meteo.json").write_text(json.dumps({
        "results": {code: {"daily": {"weather_code": [3], "wind_speed_10m_max": [wind],
                                     "precipitation_sum": [0.0]}}
                    for code, wind in [("USLAX", 72.5), ("USLGB", 20.0), ("SGSIN", 10.0),
                                       ("INNSA", 15.0), ("INMUN", 15.0), ("CNSHA", 30.0),
                                       ("NLRTM", 41.0), ("DEHAM", 35.0)]},
        "errors": {}}))
    (day / "gdelt.json").write_text(json.dumps({
        "articles": [{"title": "Dockworkers strike shuts Rotterdam terminals"},
                     {"title": "Unrelated news"}]}))
    monkeypatch.setattr(enrich, "BRONZE_WEATHER", tmp_path / "weather")
    return tmp_path


def test_build_conditions(weather_bronze):
    rows = {r["port"]: r for r in enrich.build_conditions("2026-07-21")}
    assert len(rows) == 8
    assert rows["USLAX"]["weather_alerts"] == 1          # NWS alert counted
    assert rows["USLAX"]["wind_max_kmh"] == 72.5
    assert rows["NLRTM"]["disruption_news"] == 1         # Rotterdam headline matched
    assert rows["SGSIN"]["weather_alerts"] == 0


def test_probable_cause_rules():
    conditions = {
        "USLAX": {"port": "USLAX", "date": "2026-07-21", "weather_alerts": "1",
                  "wind_max_kmh": "72.5", "precip_mm": "0", "disruption_news": "0"},
        "NLRTM": {"port": "NLRTM", "date": "2026-07-21", "weather_alerts": "0",
                  "wind_max_kmh": "41", "precip_mm": "0", "disruption_news": "1"},
        "SGSIN": {"port": "SGSIN", "date": "2026-07-21", "weather_alerts": "0",
                  "wind_max_kmh": "10", "precip_mm": "0", "disruption_news": "0"},
    }
    congestion = {"singapore_strait": {"port": "singapore_strait", "date": "2026-07-21",
                                       "vessels_at_anchor": "9", "avg_dwell_h": "20"}}

    assert normalize.probable_cause("USLAX", conditions, congestion) == "weather"
    assert normalize.probable_cause("NLRTM", conditions, congestion) == "disruption"
    assert normalize.probable_cause("SGSIN", conditions, congestion) == "congestion"
    assert normalize.probable_cause("CNSHA", conditions, congestion) == "unknown"
