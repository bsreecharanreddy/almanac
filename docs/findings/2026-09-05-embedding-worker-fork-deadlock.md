# Findings — the embedding job's second real run: the encode stage's Python workers froze

**Date:** 2026-09-05
**Source:** `databricks_job.embeddings` run `630533628470951` (task run
`1090891242586607`), cluster `0905-131624-1ozy6vpc`, cancelled after
90 min of execution; the delivered driver `log4j-active.log` /
`stacktrace.log` and executor `stderr` from
`/Volumes/almanac_dbx/burn/cluster_logs/0905-131624-1ozy6vpc/`.
**What it changes:** the embeddings job cluster now sets
`TOKENIZERS_PARALLELISM=false` and `OMP_NUM_THREADS=1`; `_embed_partition`
sets the same two defensively and prints per-batch progress to stderr;
`run_embedding_pipeline_distributed` / the CLI gain `--limit` for a bounded
proof run.

---

## What was fixed first, and worked

The [partition-count defect](2026-09-05-embedding-partition-count-defect.md)
fix landed: `repartition(16)` before `mapInPandas`. This run's encode stage
(`ResultStage 7`, `write at WriteIntoDeltaCommand`) was submitted with
**exactly 16 tasks** — `Submitting 16 missing tasks from ResultStage 7 ...
for partitions Vector(15, 8, 4, 0, 9, 2, 5, 10, 11, 12, 1, 13, 14, 6, 7)` —
one per slot across the 4-worker / 16-vCPU cluster, not 967, not the
upstream read's 860. That part of the design is settled.

## The symptom

Stage 7 started 13:48:08. By 13:56 each executor had created its output
parquet files (`part-00001`, `-00004`, `-00005`, `-00007` on executor 0)
and written a first batch. Then every task stopped.

- `HangingTaskDetector`, executor 0, from 14:04 onward, once per 10 s:
  `Task 1752 is probably not making progress because its metrics ...
  shuffle.read.recordsRead -> 28909 ... output.recordsWritten -> 0 ...
  has not changed since Sat Sep 05 14:03:55 UTC 2026`. Task 1748 the same,
  frozen at `recordsRead -> 37632` since 14:17:15.
- `MapInBatchEvaluatorFactory$MapInBatchEvaluator: Idle timeout reached
  for Python worker (timeout: 600 seconds). No data received from the
  worker process: handle.map(_.isAlive) = Some(true)` — the worker
  process was **alive** and **silent**.
- The task threads were `RUNNABLE`, blocked in
  `FileFormatWriter.executeTask` on `sun.nio.ch.EPollSelectorImpl` /
  `java.io.BufferedInputStream` — i.e. waiting to read the next Arrow
  batch from the Python worker's pipe.
- No Python traceback anywhere in the delivered executor `stderr`. The
  per-task PySpark worker `stdout`/`stderr` (where a fork warning would
  print) lives in node-local `/databricks/spark/work/` and was not
  delivered — it went with the cluster on termination.

A frozen `shuffle.read.recordsRead` is the tell. A *slow* encode still
advances that counter as it pulls the next batch; a counter stuck at
28,909 for 50 minutes with the reader thread blocked and the worker alive
is a **stall inside the Python worker**, not slow compute. 28,909 texts at
Task 1's measured 151.1 texts/s is ~3 minutes of encode work — the worker
had produced nothing for an order of magnitude longer.

Cancelled at 14:53:57, 90 min execution (`execution_duration` 5,413,000
ms) + 7.4 min setup. **≈$3.85** at the $2.370/hr rate Phase 2 measured for
this 4-worker `Standard_D4ds_v6` shape (`2026-09-03-photon-ab.md`) — a
wall-clock estimate, not a billing pull. With the partition-count run's
≈$11.15, ≈$15 of the trial credit spent reaching a first real embedding
run, still with no embeddings table.

## What it is — and what is still provisional

**Proven (n=1 run, multiple corroborating signals within it):** a
Python-worker-side stall. Frozen read metric + blocked reader thread +
`isAlive=true` worker + no output is unambiguous.

**Provisional — the exact mechanism.** The signature matches the
well-documented deadlock where HuggingFace `tokenizers` (its Rayon
threadpool) or `torch` (OpenMP) is used, the process forks, and the
threadpool locks are then in an inconsistent state in the child — which is
exactly what a `mapInPandas` Python worker is (forked from
`pyspark.daemon`). `tokenizers` itself prints *"The current process just
got forked, after parallelism has already been used. Disabling parallelism
to avoid deadlocks"* and can still hang. But n=1, and the one log that
would name it (the worker's own stderr) was not delivered, so the
mechanism is **not** claimed as settled. The next run's per-batch stderr
logging (now added) will show whether the stall is at the first `encode`
call (fork deadlock) or somewhere else.

### Live validation (2026-09-05)

- `TOKENIZERS_PARALLELISM=(true|false)` as the fix for the fork deadlock:
  huggingface/transformers#5486, huggingface/tokenizers docs — consistent
  across every source, independent of which UDF API is used.
- `pyspark.ml.functions.predict_batch_udf` is the current-generation Spark
  4.0 / Databricks API for distributed model inference (Spark 4.0 release
  notes; NVIDIA "Accelerate Deep Learning ... with Apache Spark"; multiple
  2025 write-ups use it for HF SentenceTransformer embedding). It manages
  per-worker model caching and decouples batch size from partition count,
  and would remove the `repartition` shuffle and the `num_partitions`
  knob entirely. It does **not** by itself make `tokenizers`/OpenMP
  fork-safe — the env var is still required. First-party doc page
  (`spark.apache.org/.../pyspark.ml.functions.predict_batch_udf.html`)
  returned partial content on fetch; treat the caching detail as
  provisional until read in full.

## The fix

1. **Job cluster `spark_env_vars`**: `TOKENIZERS_PARALLELISM=false`,
   `OMP_NUM_THREADS=1`. Set on the cluster so `torch` sees `OMP_NUM_THREADS`
   at import and `tokenizers` sees `TOKENIZERS_PARALLELISM` at first
   tokenization. `OMP_NUM_THREADS=1` is also correct on merit: 16
   single-thread Python workers on 16 vCPUs is full utilisation without the
   oversubscription 16 workers × 4 OMP threads would cause.
2. **`_embed_partition`** sets the same two with `os.environ.setdefault`
   (backup for a worker forked before cluster env propagated;
   `TOKENIZERS_PARALLELISM` is read at first use so this still takes
   effect) and prints `[embed] <n> texts, <s>s, <rate> texts/s` to stderr
   per yielded batch.
3. **`--limit`** on `run_embedding_pipeline_distributed` and the CLI: a
   bounded proof run confirms the fork fix on a paid cluster before the
   scoped run.
4. **`--since-date`** (Bronze `event_date` lower bound): how the real run
   is *scoped* — see "Throughput and scope" below.

**`predict_batch_udf` is deferred, not rejected.** The blocker is
orthogonal to the UDF API (both need the env var), and a second rewrite of
`run_embedding_pipeline_distributed` mid-phase, under a finite expiring
credit, carries its own risk. Revisit if a scoped run still stalls with
the env vars in place.

## Proof run — the fork fix held

Run `572931281110752`, `--limit 200000`, 2026-09-05. **TERMINATED /
SUCCESS**, 83.9 min total (6.9 min setup + 77 min exec), **≈$3.31** at
$2.370/hr. `ResultStage 10` (the `mapInPandas` encode) submitted with
**exactly 16 tasks**, all 16 completed — `ResultStage 10 ... finished in
1,192,187 ms`. `[embed]` progress lines flowed the whole time; no
`HangingTaskDetector`, no idle timeout, no freeze. 400,000 rows written
(200 K PR + 200 K issue) to `almanac_dbx.embeddings.pr_issue_embeddings`,
registered. The tokenizers/OpenMP fork fix works.

## Throughput and scope — why the full corpus is not built

The proof run's encode measured **~10.5 texts/s per partition, ~168
texts/s aggregate** across the 16-vCPU cluster (16 tasks × 12,500 texts,
~1,000–1,190 s each). The full 14,924,573-text corpus at that rate is a
**~25-hour** CPU-bound encode.

The design doc's / this doc's earlier "~1.7 h" figure was wrong: it took
Task 1's **151.1 texts/s** (Phase 0, `docs/STATUS.md`) as a *per-core*
rate and multiplied by 16. Task 1's number was a multi-threaded local
measurement; the real per-core rate on `Standard_D4ds_v6` under
`OMP_NUM_THREADS=1` is ~10.5/s — **~14× lower**. This is a Gate-1 error in
Task 1's finding that propagated into the partition-count doc's sizing;
noting it here rather than silently.

GPU would make this trivial (a single T4 ≈ 30–60 min) but **every GPU VM
family is 0/0 quota in `westus3`** — `az vm list-usage --location westus3`
shows `Standard NCASv3_T4 Family vCPUs 0/0`, `NCADS_A100 0/0`,
`NCadsH100v5 0/0`, etc. — an Azure quota-increase request, not a
same-session option. CPU headroom is `Standard Ddsv6` 20→48 vCPUs, ≈2.7×,
→ ~9 h / ≈$35 of the ~$150 remaining trial credit.

**Decision (2026-09-05):** the index is built over **recent events**, not
the full corpus. `--since-date 2025-09-20` was measured at **~1.73 M**
qualifying opened-PR+issue texts (query in the doc below) — ~2.9 h encode,
≈$7. Every Phase 5 capability (distributed pipeline, the two real defects
found via cluster-log diagnostics, Vector Search integration,
point-in-time-correct retrieval, the champion-gated lift comparison) is
demonstrated identically at this size. The full-corpus build is a
one-parameter change (`embeddings_since=""`) plus a bigger or GPU cluster
— a compute-scale step, not missing work, the same shape as the roadmap's
other deliberately-not-built extension points.

Measurement command:

```sql
SELECT count(*) FROM delta.`abfss://bronze@.../events`
WHERE event_date >= '2025-09-20'
  AND get_json_object(raw_json,'$.payload.action') = 'opened'
  AND ( (get_json_object(raw_json,'$.type') = 'PullRequestEvent'
         AND get_json_object(raw_json,'$.payload.pull_request.title') IS NOT NULL
         AND get_json_object(raw_json,'$.payload.pull_request.body')  IS NOT NULL)
     OR (get_json_object(raw_json,'$.type') = 'IssuesEvent'
         AND get_json_object(raw_json,'$.payload.issue.title') IS NOT NULL
         AND get_json_object(raw_json,'$.payload.issue.body')  IS NOT NULL) )
```

## Re-run — the scoped run

```sh
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
cd infra/terraform
terraform apply -target=databricks_job.embeddings -var 'embeddings_since=2025-09-20'
databricks jobs run-now 79790319052446 --no-wait
```

Run `603111972146916`, 2026-09-05: **TERMINATED / SUCCESS**, 135.8 min
(6.3 min setup + 129.4 min exec), **≈$5.36** at $2.370/hr. **1,459,551 new
texts embedded** — the fork fix held the whole run (`[embed]` progress
flowing across all 16 tasks, both the PR and issue passes, no freeze).
`almanac_dbx.embeddings.pr_issue_embeddings` now holds **1,859,551 rows**
(1.46 M scoped + the proof run's 0.4 M; the ~270 K gap from the 1.73 M
estimate is proof-run rows the `left_anti` text_hash dedup correctly
skipped). Encode throughput held at ~12–14 texts/s per partition, a touch
above the proof run's 10.5. Cumulative embeddings spend across the two
cancelled runs, the proof, and this scoped run: **≈$24** of the trial
credit.
