# ADR-002: AWS serverless deployment of the batch legs (R14)

Date: 2026-07-21 · Status: accepted (apply blocked on AWS credentials)

## Context
The always-on-box deployment (R4) is a single machine. R14 lifts the *batch*
legs (carrier ingest -> normalize -> exception flag) to event-driven AWS
serverless so scale and availability stop being our problem. AIS websocket
consumption stays on the box (long-lived connections are a poor Lambda fit).

## Architecture

```
carrier file -> S3 (landing/) --S3 event--> EventBridge rule
    -> SQS (buffer, DLQ attached)
    -> Lambda validate   (schema check; bad file -> quarantine/ prefix + SNS)
    -> Lambda normalize  (same pipelines/normalize.py logic, packaged)
    -> Lambda flag       (exception rules -> exceptions/ prefix)
    -> Glue catalog over Iceberg tables on S3 (silver)
    -> Athena for SQL          -> SNS topic for alerts (email/Slack via sub)
```

## Decisions
1. **Terraform** in `infra/terraform/` (per REQUIREMENTS; module-free single
   stack at this size). State backend: local until a shared team exists.
2. **One Lambda codebase, three handlers** (`lambdas/handler.py`), reusing the
   exact `pipelines/normalize.py` functions — same single-implementation
   principle as the Airflow DAG. Python 3.12 runtime, zip packaging.
3. **SQS between EventBridge and Lambda** for burst buffering and a DLQ; every
   failure path lands somewhere queryable (DLQ or quarantine/ prefix) plus an
   SNS notification. Never silence, same contract as local.
4. **Iceberg via Glue catalog** for silver, giving Athena SQL and schema
   evolution; Snowflake (ADR-003) reads the same bucket via storage
   integration.
5. AIS consumer and Grafana stack remain on the box; Prometheus scrapes
   nothing from AWS (CloudWatch is the observability plane there).

## Consequences
- Two silver stores (local CSV + Iceberg) until we choose to cut over; the
  generator/normalizer make either derivable from bronze, so no divergence.
- Costs: pennies at this volume (Lambda + SQS + S3 + Athena scans on demand);
  optional EMR spot run for R12-scale jobs is out of scope until data justifies.
