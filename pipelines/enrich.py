"""R8: weather/disruption enrichment pollers (run as Airflow tasks, not services).

Sources (all free, no API key):
  nws        NWS active alerts for US ports (api.weather.gov)
  open_meteo daily weather for every port in config/ports.yml
  gdelt      GDELT doc API poll for port strike/closure news near our ports

Each poller lands raw JSON to data/bronze/weather/YYYY-MM-DD/<source>.json.
`conditions` then builds the daily silver table port_conditions.csv:
  port, date, weather_alerts, weather_code, wind_max_kmh, precip_mm, disruption_news

Failure mode: a poller error for one port is recorded in the summary and the
raw file; if a source fails for ALL its ports the exit code is nonzero so the
Airflow task fails loudly. The conditions builder is pure and offline.

Usage:
    python -m pipelines.enrich --source nws|open_meteo|gdelt|conditions|all [--date YYYY-MM-DD]
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
BRONZE_WEATHER = ROOT / "data" / "bronze" / "weather"
SILVER = ROOT / "data" / "silver"

PORTS = yaml.safe_load((ROOT / "config" / "ports.yml").read_text())["ports"]
UA = {"User-Agent": "manifest-pipeline (github.com/bhosalesanraj903-lab/manifest)"}

GDELT_QUERY = '("port strike" OR "port closure" OR "port congestion" OR "dockworkers strike")'

COND_FIELDS = ["port", "date", "weather_alerts", "weather_code",
               "wind_max_kmh", "precip_mm", "disruption_news"]


def _land(date: str, source: str, payload: dict) -> None:
    out = BRONZE_WEATHER / date / f"{source}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2))


def poll_nws(date: str) -> dict:
    results, errors = {}, {}
    for code, p in PORTS.items():
        if p["country"] != "US":
            continue
        try:
            r = requests.get(f"https://api.weather.gov/alerts/active?point={p['lat']},{p['lon']}",
                             headers=UA, timeout=15)
            r.raise_for_status()
            results[code] = r.json()
        except requests.RequestException as e:
            errors[code] = repr(e)
    _land(date, "nws", {"results": results, "errors": errors})
    return {"source": "nws", "ok": len(results), "errors": len(errors)}


def poll_open_meteo(date: str) -> dict:
    results, errors = {}, {}
    for code, p in PORTS.items():
        try:
            r = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={"latitude": p["lat"], "longitude": p["lon"],
                        "daily": "weather_code,wind_speed_10m_max,precipitation_sum",
                        "forecast_days": 1, "timezone": "UTC"},
                timeout=15)
            r.raise_for_status()
            results[code] = r.json()
        except requests.RequestException as e:
            errors[code] = repr(e)
    _land(date, "open_meteo", {"results": results, "errors": errors})
    return {"source": "open_meteo", "ok": len(results), "errors": len(errors)}


def poll_gdelt(date: str) -> dict:
    # GDELT allows 1 request per 5s and can be slow; retry with spacing.
    last_err = None
    for attempt in range(3):
        if attempt:
            time.sleep(6)
        try:
            r = requests.get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params={"query": GDELT_QUERY, "mode": "artlist", "format": "json",
                        "maxrecords": 75, "timespan": "24h"},
                headers=UA, timeout=45)
            r.raise_for_status()
            payload = r.json()
            _land(date, "gdelt", payload)
            return {"source": "gdelt", "ok": 1, "errors": 0,
                    "articles": len(payload.get("articles", []))}
        except (requests.RequestException, ValueError) as e:
            last_err = e
    _land(date, "gdelt", {"error": repr(last_err)})
    return {"source": "gdelt", "ok": 0, "errors": 1}


def build_conditions(date: str) -> list[dict]:
    """Pure transform: bronze weather files for `date` -> port_conditions rows."""
    day = BRONZE_WEATHER / date

    def load(name):
        p = day / f"{name}.json"
        return json.loads(p.read_text()) if p.exists() else {}

    nws = load("nws").get("results", {})
    meteo = load("open_meteo").get("results", {})
    gdelt = load("gdelt")
    articles = gdelt.get("articles", []) if isinstance(gdelt, dict) else []

    rows = []
    for code, p in PORTS.items():
        daily = meteo.get(code, {}).get("daily", {})
        name_l = p["name"].lower()
        news = sum(1 for a in articles if name_l in a.get("title", "").lower())
        rows.append({
            "port": code,
            "date": date,
            "weather_alerts": len(nws.get(code, {}).get("features", [])),
            "weather_code": (daily.get("weather_code") or [""])[0],
            "wind_max_kmh": (daily.get("wind_speed_10m_max") or [""])[0],
            "precip_mm": (daily.get("precipitation_sum") or [""])[0],
            "disruption_news": news,
        })
    return rows


def write_conditions(date: str) -> dict:
    from ais.dwell import upsert_csv
    rows = build_conditions(date)
    upsert_csv(SILVER / "port_conditions.csv", COND_FIELDS, rows, drop_where={"date": date})
    return {"source": "conditions", "rows": len(rows)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", required=True,
                    choices=["nws", "open_meteo", "gdelt", "conditions", "all"])
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    steps = {"nws": [poll_nws], "open_meteo": [poll_open_meteo], "gdelt": [poll_gdelt],
             "conditions": [write_conditions],
             "all": [poll_nws, poll_open_meteo, poll_gdelt, write_conditions]}[args.source]

    summaries = [s(date) for s in steps]
    print(json.dumps(summaries))
    for s in summaries:
        if s.get("errors") and not s.get("ok"):
            sys.exit(1)  # source completely down -> fail the task loudly


if __name__ == "__main__":
    main()
