# Findings — `compute_author_activity`'s self-join is O(N²) per author, and the real quarter proved it

**Date:** 2026-09-04
**Source:** Three `databricks_job.build_features` runs over the real 341M-row
Silver quarter (`924195167084181`, `48552036331781`, `485360890184517`); a
live executor thread dump and the AQE physical plan from the third, before
it was cancelled.
**What it changes:** `almanac.features.groups.compute_author_activity` is
reformulated from a self-join to a time-ordered running aggregate. No
change to the feature's meaning.

---

## The symptom

Every run hung in the same Spark stage: `write at
WriteIntoDeltaCommand.scala:113` for the `author_activity` table. Of 17
write tasks, ~10 finished in 6–58 s (≈1 M shuffle records, ≈8 MB out
each); the remaining ~7 never completed. They landed, by hash of
`author_login`, on 2 of the 4 executors — those 2 executors completed
**zero** tasks and every task on them showed an identical duration since
launch.

`spark.speculation` + `spark.sql.shuffle.partitions=400` +
`spark.sql.adaptive.skewJoin.enabled` (added between runs 1 and 2) changed
nothing.

## What it was not

- **Not Azure Storage throttling.** The storage account's `Transactions`
  metric split by `ResponseType` over both hang windows showed **zero**
  `ServerBusyError` / `ServerTimeoutError`; a handful of `ClientOtherError`
  (Delta existence probes) and transient `AuthenticationError`.
- **Not GC.** Total GC time per executor was 7–10 s across the whole run.
- **Not OOM / disk.** Executor memory and disk use were ≈0.
- **Not a lock.** The stuck task threads were `RUNNABLE`, not `BLOCKED` or
  `WAITING` on a monitor.

## What it was

The live thread dump of a stuck task:

```
Executor task launch worker for task 11 in stage 21.0  [RUNNABLE]
  org.apache.spark.unsafe.array.ByteArrayMethods.arrayEquals
  org.apache.spark.unsafe.map.BytesToBytesMap.safeLookup
  org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap.getAggregationBufferFromUnsafeRow
  ...GeneratedIteratorForCodegenStage7.hashAgg_doConsume
  ...FileFormatWriter.executeTask
```

The thread is spinning inside a hash aggregation, consuming an input
stream that will not end. (The task metrics read `0.0 s` CPU only because
a task reports metrics on completion or a progress heartbeat, and these
tasks never complete one.)

The AQE physical plan for the `author_activity` write:

```
Execute WriteIntoDeltaCommand
  WriteFiles
    HashAggregate                                   -- groupBy(author_login, opened_at).agg(count, avg)
      Project
        SortMergeJoin LeftOuter                      -- the self-join: this ⋈ prior on author_login
          Sort <- ShuffleQueryStage  rowCount=1.32E7 -- "this": 13.2 M PRs opened
          Sort <- ShuffleQueryStage  rowCount=1.32E7 -- "prior": the same 13.2 M
  ResultQueryStage  Statistics(sizeInBytes=596.7 TiB, isRuntime=true)
```

**596.7 TiB** is AQE's runtime-measured size of the self-join output. A
few bot logins (`dependabot[bot]`, `renovate[bot]`, `github-actions[bot]`)
open a large share of every PR on GitHub, so for those `author_login` keys
`this ⋈ prior` is a near-Cartesian product — O(N²) in that author's PR
count — feeding the downstream aggregation. Human-author partitions hold a
few dozen prior PRs each and finish in seconds.

`spark.sql.adaptive.skewJoin.enabled` does not help: AQE splits a skewed
partition on one side and joins each slice against the whole other side,
but for a **self-join** both sides carry the same skewed key, so every
split still meets the full opposing partition — same total work, more
tasks.

## The fix

`compute_author_activity` needs, per PR opened at `opened_at` by author
`a`: the count and merge rate of `a`'s prior PRs that had **closed**
before `opened_at`. That is a running aggregate over one ordered
timeline, not a join:

1. Two record kinds per author — a **resolution** at each prior PR's
   `closed_at` (carrying its merged outcome), and a **query** at each PR's
   `opened_at`.
2. Order by `(event_ts, tie_rank)` with the query ahead of a resolution at
   an equal timestamp — this is what makes `closed_at < opened_at` strict.
3. Running `sum` of resolutions and of merged outcomes over
   `rowsBetween(unboundedPreceding, currentRow)`, read at each query row.

O(N log N) per author, and a 300 k-row bot partition is a sort, not a
Cartesian product. `compute_repo_activity` already uses this exact
running-window shape and ran fine on the same quarter.

## One behaviour change, in the direction of correctness

The old `groupBy(this.author_login, this.opened_at)` over the self-join
output counted each qualifying prior **once per PR the author opened at
that same timestamp**. An author (or a bot) opening K PRs in one second
got `prior_pr_count = K × true_count`; the merge *rate* was unaffected
(a mean is invariant to uniform duplication). The running-aggregate form
counts each prior once. Pinned by
`test_author_activity_counts_each_prior_once_for_a_same_instant_burst`,
which fails on the old code (`3` vs `1`) and passes on the new.

The strict-boundary and per-author-scoping properties are unchanged and
now have their own tests; the plan's original worked example
(`test_author_activity_prior_pr_count_and_merge_rate`) is untouched and
still green.

## Cost of finding it

Three cancelled feature-build runs across two sessions, ≈$13 of the trial
credit. The third run carried `cluster_log_conf` (added first, as
diagnostics, not a fix) and the live thread-dump + AQE-plan capture from
it is what named the component — consistent with the project's rule that
an unattributable failure gets a diagnostics commit before a fix commit.
