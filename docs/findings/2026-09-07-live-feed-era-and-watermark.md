# The live feed breaks two assumptions the streaming ingest was built on

**Date:** 2026-09-07
**Phase:** 6, Task 9
**Status:** gate met; three assumptions fixed, one design question left open

**Phase 6's exit gate is met.** A live GitHub event reached the online store
and *changed* a served feature value, demonstrated end to end across two
windows rather than asserted:

| | |
|---|---|
| Repos whose served values **changed** | **84** |
| New repos added | 684 |
| Repos lost | 0 |

```
repo 1125421191:  events_prior_24h=11  @ 02:26:54
               →  events_prior_24h=18  @ 04:51:02
```

The second value could only exist because of events polled 25 minutes after
the first was read out of Postgres.

**Offline↔online consistency is exact**, checked against the raw JSONL rather
than against the pipeline's own account of itself: 4,925/4,925 distinct
`repo_id` and 3,721/3,721 distinct `actor_login`.

The first real live window (30 polls, 6,801 events, 2026-09-07 02:03–02:34
UTC) falsified two claims this project had recorded as settled. Both were
measured on a *single* prior sample; both are corrected here with the
larger one.

## Finding 1 — the feed is not REDUCED_V3-only (fixed)

`stream.ingest._reject_non_reduced_era` failed the whole stream on its
first batch:

```
ValueError: streaming ingest received a non-REDUCED_V3 event;
the live feed cannot produce one, so this is a real anomaly
```

Its docstring claimed "The live feed is REDUCED_V3-only (measured
2026-09-06)". Against the 30-poll window:

| `schema_era` | count | share |
|---|---|---|
| `reduced_v3` | 6,798 | 99.956% |
| `modern_v2` | 3 | 0.044% |
| `legacy_v1` | 0 | 0% |
| null `created_at` | 0 | 0% |

The three:

```
PublicEvent       id=14539118947  created_at=2025-08-26T05:12:05Z
PublicEvent       id=14539725076  created_at=2025-06-09T15:12:47Z
PullRequestEvent  id=14539833174  created_at=2021-07-30T18:12:32Z
```

**Why this happens:** GitHub stamps event ids on *delivery*, not on
occurrence. Those ids sit in the same range as their neighbours in the same
response, adjacent to events created seconds earlier. An arbitrarily old
`created_at` can therefore arrive at any moment, and no amount of polling
sooner would avoid it.

**The guard was also guarding the wrong thing.** Its own rationale names
the hazard as `pipeline.dedup`'s id-less-event handling — and `dedup`
passes null `event_id` rows straight through, so duplicates would
accumulate unnoticed. But all three offending events carried valid ids, and
`pipeline.eras` already handles every era correctly. Rejecting them lost
real data to protect an invariant they never threatened.

**Fix:** narrowed to `_reject_id_less_event` (assert `event_id IS NOT
NULL`), and the era mix is now *reported* by a `non_reduced_count`
`observe()` metric instead of assumed. Rate measured rather than asserted,
which is the point.

## Finding 2 — a distinct late event is silently dropped (open)

`stream_events` applies `withWatermark("created_at", "10 minutes")` +
`dropDuplicatesWithinWatermark(["event_id"])`. The existing test
`test_a_row_older_than_the_watermark_is_counted_even_though_it_is_dropped`
established that a late row is dropped, and justified it in its own
assertion message:

> "poll 3's row is too late to land — correct here, since it is a genuine
> duplicate of poll 1's row, **not data loss**"

That justification holds only for duplicates. It does not hold for the live
feed. Measured over the same 6,801 events, lag = `polled_at - created_at`:

| Event-time lag at poll | share |
|---|---|
| exactly 300 s (the API's hard floor) | ~81% |
| > 10 min (the watermark) | 18.0% |
| > 1 day | 16.4% |

These are **not** duplicates — they are first-sightings of unique events.
A new test (`test_a_distinct_late_event_is_dropped_not_just_deduplicated`)
confirms the consequence directly: a never-before-seen `event_id` whose
event time is behind the watermark is counted by `late_event_count` and
**absent from Silver**.

### Confirmed in production, and the prediction came first

Whether those rows are lost depends on where the watermark sits when their
batch runs — which made the session's two windows a natural experiment. The
prediction was written down *before* the second one ran.

| | Cycle A | Cycle B |
|---|---|---|
| Checkpoint | fresh (watermark at 0) | **resumed (watermark advanced)** |
| Events polled | 6,801 | 1,206 |
| Distinct repos polled | 4,925 | 943 (845 new) |
| Repos reaching the online store | 4,925 | 684 new |
| **Repos lost** | **0** | **161** |

Union of distinct `repo_id` across all 35 polls: 5,770. Served by the online
store: 5,609. The 161 missing are exactly that difference, and all belong to
cycle B — the run whose watermark had been restored from the checkpoint.

So the design does not lose data on a cold start and does lose it on every
run after: a first run that looks correct and a steady state that quietly
is not. The earlier reading — "an accident of batching" — was right about
the mechanism and too optimistic about the consequence.

**Two independent measurements agree exactly.** Spark's `observe()` reported
`late_events=217` for cycle B; counting `polled_at - created_at > 600s` in
the raw JSONL gives 217 of 1,206 (18.0%). For cycle A, `observe()` gave
1,206 of 6,801 (17.7%) against 18.0% client-side on a 439-event subsample.

### Options, not yet decided

1. **Widen the watermark** past the observed tail. Simple, but the tail
   reaches 9+ days, so dedup state retention becomes the cost.
2. **Drop `dropDuplicatesWithinWatermark`** and dedup on write (Delta
   `MERGE` on `event_id`) — unbounded dedup without unbounded Spark state,
   at the cost of a merge per batch.
3. **Accept the loss and document it**, since capture is already best-effort
   against a firehose with no completeness guarantee.

Option 2 matches what the batch path already does and is the current
recommendation, but it is a design change and is not being made under Task
9's own gate.

## Finding 3 — `publish_table` does not create its catalog (fixed)

With ingest fixed, `features` succeeded and `publish` failed:

```
NotFound: Catalog 'almanac_lb_online' does not exist.
```

`variables.tf` had asserted the opposite — *"created by publish_table"* — in
a comment written from the API's shape rather than from a run. A Lakebase
online table lands in a **Database Catalog**: a UC catalog backed by a
Postgres database on the instance, created by
`databricks_database_database_catalog` (note the doubled word; the provider
namespaces it under the `database` service). It must exist before the
publish.

Fixed by adding that resource and referencing it from the publish task's
`--online-schema` argument, so Terraform orders the catalog before the job
that writes into it. `terraform validate` could not have caught this and did
not; only a real publish did.

**Two side confirmations from the same run**, both previously unproven:

- **`ALTER COLUMN … SET NOT NULL` works on Databricks-managed Delta.** OSS
  Delta 4.4.0 refuses it on a populated table, which is why Task 6 split the
  CDF statement (locally round-trippable) from this one (not). The
  `features --register` stage ran it for real and succeeded.
- **The corrected ingest guard passes live data**, including the 3
  `modern_v2` events that aborted the previous run.

## Task 9's measurements

### Freshness, decomposed

Task 1 warned that an undecomposed freshness number "will read as pipeline
latency when it is mostly GitHub's". Measured over cycle B's 976 floor-cohort
events (those at the API's 300s minimum, excluding late arrivals):

| Component | p50 | Share |
|---|---|---|
| GitHub feed delay | 302 s | 54% |
| Almanac poll → served | 260 s | 46% |
| **End to end (created_at → readable in Lakebase)** | **561 s** | |

Range: min 434 s, p95 688 s, max 690 s.

Only **121 s** of Almanac's 260 s is pipeline work — ingest 68 s, features
35 s, publish 18 s. The remaining ~139 s is the bounded-window design: an
event captured by the first poll waits for the fifth to finish before ingest
starts at all. A continuous trigger removes that term; it is not latency
anything in the code is spending.

### Capture fraction

1,206 events over 5 polls = 241.2/poll ≈ **13,783/hour**, against §12's
archive-measured 155–162K/hour:

| Baseline | Capture |
|---|---|
| 155,000/hr | 8.9% |
| 162,000/hr | 8.5% |

Task 1 measured 7.1–7.4% on its own 30-poll window. Both are `n=1` windows at
different times of day; the honest statement is "the same order, ~7–9%", not
a revision of Task 1.

### Duplicates

**Zero**, across all 6,801 events of cycle A: 6,801 distinct `event_id` from
6,801 rows. This extends Task 1's "zero overlap between consecutive polls"
from adjacent pairs to a whole 30-poll window.

### Cost

The Lakebase instance ran ~3.5 hours at CU_1 (created 01:55 UTC, stopped
02:49–03:19 during a test run, destroyed ~05:5x).

**The DBU rate itself is still unmeasured**, for a reason worth recording:
`system.billing.usage` is *empty* in a newly created metastore — 0 rows
total, not merely 0 for this workspace — and in the main westus3 metastore it
lags roughly a day (latest `usage_date` was 2026-09-06 while this ran on
09-07). So the measurement cannot be taken during the window it measures.
It is deferred rather than lost: usage records survive resource deletion, and
`workspace_id = 7405616381250124` was captured before teardown so the rows
remain attributable. This is the third time this project has hit a gap in
Databricks' own price/usage surfaces.

## Method

Every number above is from the 30 files the poll stage wrote, retained at
`stream_landing` and copied locally before analysis. `n = 6,801` events
across 30 polls, one 31-minute window, one time of day — enough to prove
*existence* (a single non-REDUCED_V3 event falsifies "cannot produce one"),
not enough to pin the *rate*. The 0.044% and 16.4% figures should be
treated as one sample, not as constants.

## Corrections to earlier claims

- `stream/ingest.py`'s "the live feed is REDUCED_V3-only (measured
  2026-09-06)" — **wrong**, corrected in place. Drawn from a window that
  happened to contain no old-`created_at` events.
- During this session I stated that late events are not dropped because the
  watermark only bounds dedup state retention. **Wrong**, and the repo's own
  test already said so. The failing run could not have shown it either way,
  since the guard fired on batch 0 where the watermark had not advanced.
- `infra/terraform-lakebase/README.md` told the reader to
  `export DATABRICKS_HOST="https://$(terraform output -raw workspace_url)"`;
  the output already carries the scheme. Fixed.
