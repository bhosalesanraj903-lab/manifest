# Restore runbook (R4)

What can be lost, and how to get it back. Bronze is the source of truth;
everything downstream is re-derivable.

## Silver/gold corrupted or deleted

Full rebuild from bronze — this is by design (ADR-001):

```bash
docker compose -f deploy/docker-compose.prod.yml exec airflow \
  bash -c "cd /opt/airflow/manifest && python -m pipelines.normalize"
```

Alert state (`data/silver/_alert_state.json`) lost -> next alerter run
re-alerts currently-open exceptions once. Noisy for one run, never silent.

## Bronze lost (disk failure)

Bronze is the one thing to back up. Cheap rsync cron from the box:

```bash
rsync -a data/bronze/ backup-target:/backups/manifest/bronze/
```

Carrier bronze can be regenerated synthetically (`make run`), but AIS bronze
is real captured data — it cannot be re-fetched. Restore from the rsync copy.

## Airflow metadata volume lost

Nothing precious lives there (no XCom state the pipeline depends on).
`docker compose up -d` recreates it; re-unpause the DAG in the UI.

## Grafana/Prometheus volumes lost

Dashboards and datasources are provisioned from `infra/grafana/` in git —
recreated automatically. Prometheus history is lost; accepted (SLO attainment
restarts from zero, noted in the weekly ops note).

## Whole box dies

New box + "First deploy" in deploy.md + restore bronze from backup. Target
RTO: under 1 hour.
