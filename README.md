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
- [x] R1 Slack alerter (escalation bands, retry -> failure ledger)
- [x] R2 DAG hardening + pushgateway metrics
- [x] R3 quarantine writes by reason
- [x] R4 deploy artifacts (`deploy/`, runbooks) — *live deploy needs Docker + a box*
- [x] R5 AIS depth: 3 port regions, dwell/congestion, dim_vessel
- [x] R6 ocean legs + VESSEL_STALLED
- [x] R7 SLOs (`docs/slo.md`) + Grafana dashboard provisioning
- [x] R8 NWS/Open-Meteo/GDELT enrichment + probable_cause (live-verified)
- [x] R9 dbt gold layer on duckdb (24/24 build incl. reconciliation test)
- [x] R10 ETA v1 — MAE 5.15h on seed 42 (`docs/` + explainability columns)
- [x] R11 game days A+B with postmortems (`docs/postmortems/`)
- [x] R12 Spark normalizer row-identical at 936k events (`docs/benchmarks/spark.md`)
- [x] R13 Snowflake artifacts (`snowflake/`, ADR-003) — *execution needs an account*
- [x] R14 AWS serverless (`infra/terraform/` validated, `lambdas/`, ADR-002) — *apply needs creds*
- [x] R15 India module: pincode normalizer, e-way expiry, AQ enrichment
- [x] R16 NL-query app over governed marts (`nlq/`, eval set)
