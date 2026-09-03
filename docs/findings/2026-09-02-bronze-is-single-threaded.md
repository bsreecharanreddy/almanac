# Findings — Bronze is single-threaded, and it is 65% of the backfill

**Date:** 2026-09-02
**Run:** Databricks job run `227270110474807` (`databricks_job.backfill`),
workspace `almanac-dbx`, `westus3`. The first backfill run to clear all
eight deploy/runtime defects and process real days end to end
(`2026-09-02-burn-deploy-and-first-run-defects.md`).
**Method:** read the run's own per-day checkpoint files from
`/Volumes/almanac_dbx/burn/checkpoints/tier3/`, which record the per-layer
timings `process_day` measures for itself.

**Status: diagnosis, not measurement.** The timings below are measured.
The *cause* attributed to them is inferred from those timings, the code,
and a documented property of gzip — the confirming observation has not
been taken. The exact check that would settle it is named at the bottom.
Recorded now, while the evidence is fresh, rather than after the run.

---

## What was measured

| Day | Rows | GB gz | Fetch | Bronze | Silver | Total |
|---|---|---|---|---|---|---|
| 2025-07-01 (first, cold) | 3,748,409 | 2.02 | 16.1 s | **241.7 s** | 71.4 s | 329 s |
| 2025-08-15 | 3,695,399 | 1.84 | 12.0 s | **122.5 s** | 55.4 s | 190 s |
| 2025-09-01 | 3,770,527 | 2.16 | 14.8 s | **140.7 s** | 57.9 s | 213 s |

Day 1 carries table creation and JIT warmup; steady state is ~190–215 s
per day. Sustained throughput across the quarter: **~3.2 min/day**, stable
to within a few seconds per report over 65 days — no degradation as the
Delta tables grow.

Steady-state share of a day: **Bronze ~65%, Silver ~28%, fetch ~7%.**

---

## The anomaly

Bronze does *less* work than Silver and takes **2.3x longer**.

| | What it actually does | Steady-state time |
|---|---|---|
| **Bronze** | Read a gzip file, add two literal columns, write Delta | ~130 s |
| **Silver** | Parse 3.7 M JSON documents, normalize three schema eras, shuffle for dedup, evaluate six quality rules, write two tables | ~57 s |

That is backwards, and the size of the gap rules out constant-factor
explanations.

## The diagnosis

`_land_bronze` in `src/almanac/burn/day.py` loops **hour by hour**, and each
iteration issues its own `spark.read.text()` and `write_bronze()` against a
single `.json.gz`:

```python
for hour, result in paired:
    ...
    raw = spark.read.text(spark_path(result.path))...
    write_bronze(partitioned, bronze_path, event_date=..., event_hour=...)
```

**Gzip is not splittable.** One `.gz` file is always exactly one Spark input
partition, therefore one task, therefore one core. Twenty-four of them, in
series. Bronze never uses more than one core of the 4-worker cluster.

Silver is fast for the mirror-image reason: it reads **Delta**, which is
columnar and splittable, so its 24 hour-partitions fan out across the
cluster. The stage doing more work is the stage that is allowed to
parallelize.

A rough consistency check: ~2 GB gz expands to roughly 10 GB of JSON
(**assumed** at ~5x, not measured — 3.7 M events at ~2.5 KB each gives the
same order). 10 GB in ~130 s is ~77 MB/s, which is ordinary single-core
gzip decompression throughput. Consistent with the diagnosis; not proof of
it.

## What this means for the cluster

The 4-worker cluster is **largely idle for about two thirds of every day's
processing**. Adding workers would not help — the limiting resource is one
core, and more cores do not make a non-splittable file splittable. This
also corrects an earlier reading of the same numbers, which took "Bronze
is the expensive layer" to mean the write was expensive and the cluster
saturated. The write is not expensive; the read is serialized.

## The fix, when it is time

One read instead of twenty-four:

```python
raw = spark.read.text([spark_path(r.path) for _, r in ok_hours])
```

24 paths in one call gives 24 input partitions, which fan out across the
cluster. `event_hour` then comes from `_metadata.file_path` rather than the
loop variable, and the day is written once with
`replaceWhere event_date = '...'`.

**The idempotency story survives.** The write grain moves from hour to day,
which is already the checkpoint grain — so recovery granularity is
unchanged, and `replaceWhere` remains exactly-once per replay.

**Expected payoff, unmeasured:** Bronze from ~130 s toward the tens of
seconds, roughly halving per-day time. Stated as a hypothesis so the
implementing change can falsify it — the same discipline as the Photon A/B
(§8.2), which was designed to permit a null result.

## Deliberately not done now

Run `227270110474807` was 65 of 92 days in when this was written and
healthy. Restarting it to save wall-clock would have discarded ~3.5 hours
of completed work to chase an unmeasured improvement, on a credit that
expires 2026-09-24. The finding is recorded; the change belongs to a
scoped task with its own before-and-after measurement.

## To confirm before quoting this as measured

Per design doc §13, this is written up as a diagnosis. What would settle
it, in one observation: **the Spark UI's task count for a Bronze stage.**
The diagnosis predicts **exactly 1 task per hour-file read**. If a Bronze
read shows more than one task, the non-splittable-gzip explanation is
wrong and the timing gap needs another cause.

A second, cheaper check: the Bronze Delta table's file layout. One parquet
file per `(event_date, event_hour)` partition is what a single output
partition produces.
