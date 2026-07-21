# Game day A: carrier feed goes silent (R11)

Date: 2026-07-21 · Scripted failure injection, run locally

## Scenario
Carrier feed stops delivering files (simulated: empty landing zone, bronze
mtimes backdated 5h).

## What happened (before fix)
`ingest` exited 0 with `files_landed: 0`; `normalize` happily rebuilt from old
bronze; `alert` found nothing new. **Every task green, zero signal.** A dead
integration would have gone unnoticed until someone looked at a dashboard.

## Gap
No freshness check anywhere in the hourly path. "Success" was defined as "no
errors", not "data arrived".

## Fix
`pipelines/ingest.py --max-silence-hours` (default 3h): if nothing lands AND
the newest bronze file is older than the threshold, ingest exits 2 ->
Airflow task fails -> R2 Slack failure callback pages.

## Verification
Backdated bronze 5h, ran ingest: `FEED SILENT: nothing landed and newest
bronze is 5.01h old (max 3.0h)`, exit code 2. Fresh bronze (0.19h): exit 0.

## Lessons
- Green pipelines lie: absence of data is a failure mode that produces no
  errors. Freshness must be asserted, not assumed.
- The fix cost ~15 lines. The gap survived R1-R10 because every test fed the
  pipeline data; none tested the *absence* of data.
