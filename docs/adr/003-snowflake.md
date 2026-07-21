# ADR-003: Snowflake warehouse layer (R13)

Date: 2026-07-21 · Status: accepted (implementation blocked on account credentials)

## Context
The duckdb gold layer (ADR-001, R9) is single-machine. R13 moves the warehouse
to Snowflake for shared access, dynamic tables, Time Travel, and zero-copy
clones for CI, without changing the dbt models.

## Decisions
1. **Load silver via external S3 stage + COPY** (`snowflake/02_load.sql`).
   Snowpipe auto-ingest is scripted but commented until S3 events exist (R14
   provides the bucket). COPY on a schedule is sufficient at current volume.
2. **dbt retarget, not rewrite**: same models, new `snowflake` output in
   `dbt/profiles.yml` driven by env vars. duckdb stays the local/CI default.
   The only model-level change is the `read_csv` staging sources swap to
   Snowflake external tables (handled via `generate_schema_name`-free source
   overrides when the target is snowflake).
3. **Exception mart as a Dynamic Table** (`03_dynamic_exception_mart.sql`,
   TARGET_LAG 15 minutes) - continuously maintained, no orchestration.
4. **Time Travel demo + zero-copy clone for CI** are scripted
   (`04`/`05_*.sql`): CI clones prod db in seconds at zero storage cost, runs
   dbt against the clone, drops it.

## Consequences
- Needs: account identifier, user/role with SYSADMIN-ish grants, and a
  storage-integration-capable role. Free trial suffices for the demo.
- Cost control: X-SMALL warehouse, AUTO_SUSPEND=60s, dynamic table lag 15m.
