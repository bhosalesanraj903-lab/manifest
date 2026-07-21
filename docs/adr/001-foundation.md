# ADR-001: Foundation stack

Date: 2026-07-21 · Status: accepted

## Context
Greenfield build of the Manifest platform (see REQUIREMENTS.md). Need the
smallest stack that supports the Phase 1 operability work without painting us
into a corner for dbt (R9), Spark (R12), or Snowflake/AWS (R13/R14).

## Decisions

1. **Local files, medallion layout (`data/landing|bronze|silver`), CSV silver.**
   Bronze is immutable ndjson in dated partitions; silver is rebuilt in full on
   every run — idempotency by construction at this scale. duckdb/dbt arrive in
   R9 on top of the same files; Spark (R12) and Snowflake (R13) re-derive from
   bronze. No database to operate yet.

2. **Python stdlib + pyyaml + websockets only.** No pandas: the normalizer is
   row-at-a-time validation logic, and keeping it dependency-light makes the
   R12 Spark port a clean comparison.

3. **Airflow 3 (apache/airflow:3.0.2, standalone in docker compose).**
   Airflow 2 is legacy; but note Airflow 3 REMOVED task SLAs, so R2's "15-min
   SLA" will be implemented as a deadline/duration check + failure callback
   rather than the old `sla=` param. Flagged for the R2 design note.

4. **Config-driven carrier handling** (`config/carriers.yml`: ref regex +
   timestamp encoding per carrier). Adding a carrier is a config change, not a
   code change.

5. **Stable exception IDs** (`sha1(shipment|type)`), recomputed each run.
   The queue is a *current-state* table; alert-once semantics live in R1's
   alerter, which diffs IDs against persisted alert state.

6. **Exception detection is deterministic given `--asof`** — no wall-clock
   reads inside the logic. This is what makes the rules unit-testable.

## Consequences
- Full-rebuild normalize is O(all bronze); acceptable until R12, revisit there.
- MISSED_MILESTONE uses per-milestone max-silence thresholds (see
  `pipelines/normalize.py`); tuning them is data work in Phase 2 once real
  cadence data accumulates.
