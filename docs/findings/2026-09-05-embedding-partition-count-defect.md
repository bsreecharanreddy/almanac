# Findings — the embedding job's first real run: 967 partitions, 967 model loads

**Date:** 2026-09-05
**Source:** `databricks_job.embeddings` run `922451764451708` (task run
`137765794833461`), cancelled after 16,938 s (4h42m); the live driver
`log4j-active.log` and cluster events for `0905-043316-7mu0ky4g`.
**What it changes:** `run_embedding_pipeline_distributed` gains a
`num_partitions` parameter (default 16) and repartitions before
`mapInPandas`; `_embed_partition` loads its encoder lazily, only once a
partition actually has rows to embed.

---

## The symptom

The run was estimated at ~1.7h (design doc §8.3a, STATUS.md's Phase 5
Task 7 row) against the real 14,924,573-text corpus, distributed across
this project's 4-worker job-cluster shape. After 3.5h it was still in
its first Spark stage. The driver log showed individual partition tasks
completing one at a time, each taking **12–14 minutes**:

```
26/09/05 08:16:36 INFO TaskSetManager: Finished task 244.0 in stage 5.0 (TID 1146) in 842130 ms on 10.139.64.13 (executor 2) (274/967)
26/09/05 08:17:45 INFO TaskSetManager: Finished task 317.0 in stage 5.0 (TID 1154) in 717225 ms on 10.139.64.11 (executor 1) (275/967)
26/09/05 08:17:48 INFO TaskSetManager: Finished task 254.0 in stage 5.0 (TID 1152) in 757401 ms on 10.139.64.13 (executor 2) (276/967)
```

**967 total tasks** for this stage — one per partition of the pending-
text DataFrame, at Spark's own default split (no `repartition` call
anywhere in `run_embedding_pipeline_distributed`). At ~13 min/partition
and 4-way concurrency (one task per executor; `Standard_D4ds_v6` gave
each executor a single occupied slot at a time per
`TaskSchedulerImpl`'s own resource-offer log), the stage alone projected
to roughly `967 / 4 × 13 min ≈ 52h` — the run was cancelled well before
that, at 274–277/967 (≈29%) done.

## What it was not

14.9M texts split across 967 partitions is ~15,430 texts/partition. At
Task 1's own measured 151.1 texts/sec (single-threaded CPU,
`docs/STATUS.md`'s Phase 0 Task 8 row), embedding one partition's actual
text is `15,430 / 151.1 ≈ 102 s` — under two minutes. The observed
12–14 minutes per partition is **not** the encode work; something else
dominates by a factor of ~7–8x.

## What it was

`run_embedding_pipeline_distributed`'s `mapInPandas` closure
(`_embed_partition`) called `load_encoder(model_name)` — a fresh
`SentenceTransformer(...)` construction — once per invocation, and Spark
invokes that closure once per partition. **967 partitions meant 967
separate model constructions**, each paying its own weight-load,
tokenizer-init, and thread-pool setup cost, competing with the other
three concurrently-running loads for the same node's CPU and disk I/O.
That per-partition fixed cost, not the ~100s of actual encoding, is what
the 12–14 minute figure mostly measures.

The design doc and the original STATUS.md row (Phase 5 Task 7) both
reasoned about this in terms of *available parallelism* ("16 vCPUs") and
never named *partition count* as a variable in its own right — the
distributed rewrite fixed the single-node bottleneck it set out to fix,
but introduced a new one nobody had reason to look for until a real run
at real partition-count scale existed to show it, the same shape every
other real-defect finding in this project has taken.

## The fix

`run_embedding_pipeline_distributed` now takes `num_partitions: int = 16`
and calls `texts.repartition(num_partitions)` before `mapInPandas` —
bounding the number of `load_encoder` calls to a fixed, cluster-shape-
matched number (4 workers × 4 vCPUs) regardless of how many partitions
the upstream Delta read or `left_anti` join happens to produce.
`_embed_partition` also now loads its encoder lazily, on first non-empty
`pdf`, so a partition that repartitioning left empty never pays for a
model load at all — defensive, not the fix itself.

Pinned by `test_run_embedding_pipeline_distributed_bounds_model_loads_to_num_partitions`:
a monkeypatched, call-counting `load_encoder` asserts `len(calls) <= 2`
against `num_partitions=2` and 8 real rows, proving the bound holds
without downloading real weights.

**Sizing rationale for 16**: at 14.9M texts / 16 partitions ≈ 932K
texts/partition, actual encode time per partition is `932,536 / 151.1 ≈
6,172 s ≈ 103 min` — close to the original ~1.7h estimate once the 16
concurrent partitions run in parallel across the cluster's own 16
vCPUs, with the one-time model-load overhead (now paid 16 times, not
967) adding perhaps another 12–14 minutes to the critical path rather
than driving it.

## Cost of finding it

The cancelled run: 4h42m (16,938s) of cluster time on the 4-worker
`Standard_D4ds_v6` job-cluster shape. At the $2.370/hr rate measured for
this exact shape in Phase 2 (`docs/findings/2026-09-03-photon-ab.md`),
that is **≈$11.15** — an estimate from wall-clock duration, not a
billing-API pull (the same caveat that estimate itself carried).

## Re-run

The re-run (`630533628470951`, 2026-09-05) confirmed this fix — the encode
stage was submitted with exactly 16 tasks, not 967 — but then surfaced a
**separate** defect: the `mapInPandas` Python workers froze, alive but
producing nothing, a fork-time threadpool stall in `tokenizers` / `torch`.
That is its own finding:
[2026-09-05-embedding-worker-fork-deadlock.md](2026-09-05-embedding-worker-fork-deadlock.md).
The full-corpus duration, cost, and row count land there once a run
completes.
