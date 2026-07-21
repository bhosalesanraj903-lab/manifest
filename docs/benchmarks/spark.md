# R12 scale sprint: Python vs Spark normalizer

Run: 2026-07-21 · seed 42 · asof 2026-07-21T00:00:00Z ·
MacBook (local[*]), PySpark 4.2, Python 3.13/3.14

| Metric | Value |
|---|---|
| Shipments generated | 200,000 |
| Raw events | 936,230 |
| Generation time | 6.6s |
| **Python normalizer** | **13.2s** |
| **Spark local[*]** | **61.6s** (incl. ~10s JVM/session startup) |
| Silver rows (both) | 903,992 |
| Row-identical output | True |

## Reading

- Correctness: Spark reuses the same `normalize_event` per-row function inside
  `mapPartitions`; outputs are byte-identical after identical sort. Verified
  with `filecmp` on this run (PASS).
- At ~1M events the in-memory Python rebuild is still faster than Spark local
  mode: the dataset fits comfortably in RAM and Spark pays JVM startup,
  pickling, and shuffle overhead. This matches ADR-001's "full rebuild is fine
  at this scale" call.
- Spark earns its keep when (a) bronze no longer fits in one machine's memory
  (~50-100x today's volume), or (b) the job moves to EMR/Glue where the same
  code scales horizontally. The EMR spot-run variant is optional per R12 and
  deferred until data volume justifies the spend.

Reproduce: `make benchmark` (needs Java 17 + `.venv-dbt` with pyspark).
