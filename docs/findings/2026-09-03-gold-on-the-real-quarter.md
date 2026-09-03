# Findings — Gold on the real quarter, and the stale pointer that nearly faked it

**Date:** 2026-09-03
**Run:** Databricks job run `655249155948620` (`databricks_job.gold`),
task run `422424908001880`, **SUCCESS**.
**Method:** the run's own dbt log for the node results, `jobs get-run` for
billed duration, and `databricks tables get` afterwards to confirm *what
was actually read*.

Until this run, `dim_repo`, `fact_pull_request` and `agg_repo_daily` had
only ever been built over the 2,000-row committed fixtures. The Tier 3
backfill lands Bronze and Silver and stops there (`process_day` is fetch →
Bronze → Silver), so 341,060,851 rows and $11.96 had bought two layers
**nothing downstream had ever read**.

---

## The result

```
Finished running 2 incremental models, 1 snapshot, 19 data tests,
  1 view model in 0 hours 11 minutes and 18.21 seconds (678.21s).
Done. PASS=23  WARN=0  ERROR=0  SKIP=0  NO-OP=0  REUSED=0  TOTAL=23
```

| | |
|---|---|
| Cluster setup | 382 s |
| Execution | 803 s |
| Billed | 1,185 s = **0.329 cluster-hours** |
| Cost | **$0.78** at $2.370/cluster-hour |

Against the $184 credit this is 0.4%. The backfill plus this build plus
both A/B attempts remain comfortably inside the 40% cap §4.5 set.

## What the tests actually establish

The three worth naming, because they are the project's premises rather
than generic dbt hygiene, and this is the first time any of them met real
data:

- **`assert_label_point_in_time`** — the label may not be computed from
  anything that happened after the prediction point. This is the property
  the whole project exists to get right, and it now holds at 341M-row
  scale rather than on 2,000 crafted rows.
- **`assert_label_exclusion_accounts_for_every_row`** — conservation: every
  excluded row is accounted for rather than silently dropped. The same
  invariant Silver asserts as `clean + quarantined == input`, enforced
  again at the Gold boundary.
- **`relationships_fact_pull_request_repo_id__repo_id__ref_dim_repo_`** —
  referential integrity between the fact and the SCD2 dimension, across
  the real repo population rather than a handful of fixture repos.

Plus the enforced `not_null` column contracts on `repo_id`, `pr_number`
and `author_is_bot`. **Fixture-tested models met real data and nothing
broke** — which is a real result, not a foregone one: the contracts are
enforced, so a wrong output *type* at scale would have reddened the build.

## What this run does *not* establish

**SCD2's multi-version path is still unexercised.** `dim_repo` is a dbt
snapshot, and a first build captures only the initial version of every
row. The 3,792 renames measured in Phase 0 — including the case-only ones
and the repo renamed twice — do not appear, because detecting them needs a
*second* run over a different window, comparing against committed snapshot
state.

That in turn needs a metastore outliving the cluster, and this run's is
deliberately ephemeral on `/local_disk0`. Ephemeral is *correct* for a
first full build (nothing to be incremental from) and wrong for the
second. Choosing the durable store — Unity Catalog, or Derby over a FUSE
volume whose file locking this project has never verified — is an open
decision, recorded rather than defaulted into.

---

## The stale pointer, which would have faked the whole thing

`register_silver_sources` issues `CREATE TABLE IF NOT EXISTS`. That is
right for idempotent re-registration and **wrong the moment a pointer
already exists and is stale.**

Before this run, `almanac_dbx.silver.events` existed and pointed at
`abfss://silver@.../photon_ab_photon/clean` — the Photon A/B arm's
**one-day, 2 GB slice**, left behind by run `693303490917119`. A "real
quarter" Gold build against that would have:

1. resolved `source('silver', 'events')` to one day of data,
2. built every model over it,
3. passed all 23 tests, because the tests assert *invariants*, not volume,
4. and reported `SUCCESS` in exactly the log format above.

Nothing in the run would have looked wrong. The failure mode is a correct
pipeline producing a confidently wrong answer.

The same hazard applied to the outputs: `gold.*` were MANAGED tables
holding both A/B arms' contaminated output, and the models are
`incremental` with `merge` — so a real-quarter build would have *merged
into* one-day arm data rather than replacing it.

**Fix applied before triggering:** dropped all six stale objects
(`silver.events`, `silver.events_quarantine`, and the four `gold.*`) via
`databricks tables delete`, which is a Unity Catalog metadata operation
and needs no compute. The `silver.*` are EXTERNAL, so only the pointers
went and the underlying data was untouched; the `gold.*` were MANAGED and
their contaminated contents were meant to go.

**Verified after the run rather than assumed:**

```
almanac_dbx.silver.events  EXTERNAL
  -> abfss://silver@almanaclakekoctmh.dfs.core.windows.net/events/clean
```

`events/clean`, the 92-partition quarter — not `photon_ab_*`. This check
is the only thing separating "Gold ran on the quarter" from "Gold ran on a
day and said SUCCESS", and the two are indistinguishable from the log.

**Prevented for next time**, in the same commit as the A/B fix: both A/B
arms now run under per-arm schemas (`ab_standard_*`, `ab_photon_*`), which
isolates them from each other *and* keeps them clear of the real
`silver`/`gold` that this job owns. A future A/B can no longer leave a
stale pointer where the Gold job expects the quarter.

## The generalisable bit

`IF NOT EXISTS` converts "create" into "create or keep whatever is
already there". At a metastore boundary that is a **silent read
redirection**, not a no-op — and it fails in the direction that looks like
success. This is the second time in two days the same shape has cost
something here: the first was `CREATE TABLE IF NOT EXISTS` letting one A/B
arm win a shared table name so the other arm read its data
(`2026-09-03-photon-ab.md`).

The rule worth carrying: **when a registration step is idempotent, verify
what it resolved to, not that it succeeded.**
