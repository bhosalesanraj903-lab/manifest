# Service Level Objectives (R7)

| SLO | Target | Measured by |
|---|---|---|
| Normalize freshness | silver rebuilt < 75 min ago | `time() - manifest_last_run_timestamp < 4500` |
| AIS staleness | dwell job ran < 10 min ago* | `time() - manifest_ais_last_run_timestamp < 600` |
| Alert latency | exception alerted < 5 min after detection | alert task runs in the same DAG run as normalize; measured as normalize->alert task gap in Airflow |

*The dwell DAG is hourly; the 10-min AIS staleness SLO refers to the raw
websocket feed. Until we export a consumer heartbeat metric, the proxy is the
consumer container being up (`docker ps`) plus bronze file growth (triage
step 4). A consumer heartbeat gauge is the first hardening item in Phase 4.

Attainment is charted on the "Manifest Ops" Grafana dashboard
(`infra/grafana/provisioning/dashboards/manifest-ops.json`, auto-provisioned).
Breaches page via the R2 Slack failure callback; sustained breaches get an
incident note in `docs/weekly/`.
