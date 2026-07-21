# Building Manifest: a supply-chain visibility platform, one requirement at a time (draft)

Phase-boundary write-up covering Phases 0-4. Talking points, to be expanded:

1. **Failure-first design beats feature-first.** The most valuable code in the
   repo is the code that runs when things break: quarantine writes, escalation
   bands, the feed-silence alarm. Both game days found gaps that every
   happy-path test had sailed past.
2. **Determinism is a testing superpower.** Seeding the generator and
   threading `--asof` through every rule made exception logic unit-testable
   and made "Spark output must be row-identical" a one-line `filecmp` assert.
3. **The jitter trick.** ETA v1's error collapsed from 23h to 5h MAE not with
   a model but with an invariant: per-shipment schedule scale cancels in the
   ratio of planned segments. Know your data-generating process.
4. **Spark lost the benchmark on purpose.** 936k events: Python 13s, Spark
   local 62s. Publishing that number is the point — engines are chosen by
   measurement, not fashion (ADR-001, docs/benchmarks/spark.md).
5. **An NL interface that can't hallucinate**: R16 answers only from governed
   dbt marts with attribution on every answer, and refuses everything else.
   The eval set includes questions it MUST refuse.
