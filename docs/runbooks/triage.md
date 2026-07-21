# Morning triage (Phase 0 draft — R4 formalizes this)

1. `cat data/silver/_run_summary.json` — check `events_read > 0`, quarantined
   counts look normal (< ~2% of read), `exceptions` count vs yesterday.
2. Airflow UI (localhost:8080) → `carrier_normalize` — last run green? If red,
   open the failed task log; both tasks print a JSON summary line.
3. Rejected files? `ls data/landing/rejected/` — investigate any file there;
   nothing else re-processes it automatically.
4. AIS: `ls -la data/bronze/ais/$(date -u +%F)/` — positions.ndjson should be
   growing if the consumer is running.
5. Log the note in `docs/weekly/` (standing ops workstream).
