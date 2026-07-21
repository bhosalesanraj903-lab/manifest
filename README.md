# Manifest

Supply-chain visibility platform: ingests carrier tracking events (synthetic) and
**live AIS vessel positions**, normalizes them into a bronze/silver/gold lakehouse
layout, detects shipment exceptions, and (Phase 1+) alerts to Slack with full
observability. Built one requirement at a time against `REQUIREMENTS.md`.

## Data flow

```
 SOURCES                 BRONZE (raw)            SILVER (clean)              CONSUMERS
 ─────────               ────────────            ──────────────              ─────────
 generator ──ndjson──▶ data/bronze/carrier/ ─┐
 (synthetic carrier                          ├─ normalize ──▶ shipment_events.csv
  events, seeded,                            │   │            exception_queue.csv ──▶ Slack alerter (R1)
  planted dupes/dirt)                        │   └─ bad rows ▶ quarantine counts (writes in R3)
                                             │
 aisstream.io ─websocket─▶ data/bronze/ais/ ─┴─ dwell/congestion (R5)

 ORCHESTRATION: Airflow (docker compose) — hourly carrier_normalize DAG: ingest → normalize
 OBSERVABILITY: run summary JSON now; Prometheus + Grafana in R2/R7
```

## Quickstart

```bash
make venv        # once: create .venv and install deps
make run         # generate → ingest → normalize; prints JSON run summary
make test        # pytest
make up          # Airflow at http://localhost:8080 (admin/admin); needs Docker
make ais         # live AIS consumer; needs AISSTREAM_API_KEY (free: aisstream.io)
```

## Layout

| Path | What |
|---|---|
| `generator/generate.py` | Seeded synthetic carrier feed → `data/landing/carrier/` |
| `pipelines/ingest.py` | Landing → dated bronze partition; rejects corrupt files loudly |
| `pipelines/normalize.py` | Bronze → silver; dedupe, ref/status/time validation, exception rules |
| `ais/consumer.py` | aisstream.io websocket → `data/bronze/ais/` (auto-reconnect) |
| `dags/carrier_normalize.py` | Hourly Airflow DAG: ingest → normalize |
| `config/carriers.yml` | Carrier ref regexes + timestamp encodings (normalizer is config-driven) |
| `config/ports.yml` | Ports + AIS anchorage bounding boxes |
| `data/` | Gitignored lake: `landing/ bronze/ silver/ quarantine/ gold/` |
| `docs/adr/` | Architecture decision records |
| `docs/runbooks/` | Operator docs — start at `triage.md` |

## Silver contracts (stable — breaking changes need an ADR)

**`shipment_events.csv`**: `event_id, shipment_id, carrier, event_type, planned_ts, actual_ts, origin, dest`
(timestamps ISO-8601 UTC; `event_type` from the canonical list in `config/carriers.yml`)

**`exception_queue.csv`**: `exception_id, shipment_id, carrier, exception_type, detected_at, age_hours, detail`
`exception_id` is a stable hash of `shipment|type` — downstream alerting diffs runs on it.

Exception rules: `LATE_DEPARTURE` (>24h after plan), `CUSTOMS_DWELL` (arrived,
no release after 48h), `MISSED_MILESTONE` (silent longer than normal for its
last milestone; the ocean leg is allowed ~16 days).

## Failure modes (by design, never silent)

- Corrupt landing file → moved to `data/landing/rejected/`, nonzero exit.
- Bad event rows → quarantine-counted by reason (`unmapped_ref | unknown_status |
  unparseable_time`) in the run summary; R3 writes them to `data/quarantine/`.
- Duplicate events → dropped, counted.
- AIS disconnect → reconnect with exponential backoff (max 60s).

## Status

- [x] Phase 0 — foundation (generator, ingest, normalize, AIS consumer, DAG, tests)
- [ ] R1 Slack exception alerter
- [ ] R2 DAG hardening + Prometheus metrics
- [ ] R3 quarantine writes
- [ ] R4 deploy to always-on box
- [ ] Phase 2+ — see `REQUIREMENTS.md`
