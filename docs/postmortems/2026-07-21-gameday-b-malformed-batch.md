# Game day B: malformed batch (R11)

Date: 2026-07-21 · Scripted failure injection, run locally

## Scenario
Two flavors of garbage injected into the landing zone:
1. A file with a syntactically corrupt line (not JSON).
2. A 300-row batch of valid JSON with garbage refs (`BAD000000042`...).

## What happened
1. Corrupt file: ingest rejected the whole file to `data/landing/rejected/`,
   exit 1. Worked as designed (Phase 0 behavior). ✅
2. Garbage batch: all 300 rows quarantined with `reason=unmapped_ref`
   (11.1% of the run), quarantine files written (R3), run summary showed the
   spike — **but nobody was alerted** (before fix). Quarantine was visible
   only to someone reading the summary.

## Gap
Quarantine is "never silent" on disk but was silent in Slack. A poisoned
upstream schema change would burn hours before detection.

## Fix
`alerting/notify.py`: quarantine-spike ops alert when quarantined/read > 5%
per run, alerted once per UTC day (state key `ops:quarantine_spike:<date>`).

## Verification
Garbage batch run: `quarantine_pct: 11.1` in the alerter summary, spike
message emitted once; second run same day suppressed (state key present).

## Lessons
- "Written to a file" is necessary but not sufficient for never-silent;
  thresholds need a push channel.
- Rejection granularity is file-level for syntax and row-level for semantics —
  both paths now end in an operator-visible signal.
