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

### Decided (Task 10): dedup on write, not on event time

Three options were open:

1. **Widen the watermark** past the observed tail. Simple, but the tail
   reaches 9+ days, so dedup state retention becomes the cost — trading a
   correctness bug for a memory one.
2. **Drop `dropDuplicatesWithinWatermark`** and dedup on write with an
   insert-only Delta `MERGE` on `event_id`.
3. **Accept the loss and document it**, since capture is already best-effort
   against a firehose with no completeness guarantee.

**Option 2, implemented in Task 10.** It is Databricks' own documented
pattern for this exact problem (*"Handle deduplication during stream
processing… an insert-only merge query in `foreachBatch`… with automatic
deduplication"*, Microsoft Learn, page dated 2026-08-24, read 2026-09-07),
and it matches what the batch path already does. The stream becomes
stateless: no watermark, no state store, nothing dropped for arriving late.

**One departure from the documented example is deliberate and necessary.**
The docs bound *both* sides of the merge:

```sql
ON logs.uniqueId = new.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS
WHEN NOT MATCHED AND new.date > current_date() - INTERVAL 7 DAYS THEN INSERT *
```

Copying that verbatim would have reintroduced the very bug being fixed — the
*source*-side bound refuses to insert old events at all. Only the target side
is bounded here.

**And the docs' wall-clock bound was itself wrong for this pipeline**, which a
test caught rather than review. `current_date() - 30 days` excluded fixture
data dated months earlier from the match, so duplicates sailed through and
`test_a_very_late_duplicate_is_suppressed_and_still_counted` failed with
`['1', '1', '2']`. The bound is now the **batch's own `event_date` span**,
which is exact rather than heuristic: `event_date` is derived from
`created_at`, so a duplicate necessarily carries the same one and cannot fall
outside the range. It prunes at least as hard (a poll cycle spans minutes)
and removes a wall-clock dependency that would have made replaying archive
data behave differently from a live window.

**Exactly-once changed mechanism and was re-verified, not assumed.**
`txnAppId`/`txnVersion` are `DataFrameWriter` options and do not apply to
MERGE, so Delta no longer skips an already-committed batch; the batch is
reprocessed and inserts nothing, because every row matches on `event_id`.
`test_stream_exactly_once.py` asserts on outcome — row sets from an
interrupted run against an uninterrupted one — so it still tests the real
contract, and both cases pass unchanged.

### Verified against the real failing conditions, not only in tests

The unit tests run on local Spark against fixtures. The loss above happened
on **ADLS-backed Delta with a restored cloud checkpoint**, so the fix was
re-run there, replaying the same 35 poll files that produced it:

| Phase | Input | Checkpoint | Rows in Silver |
|---|---|---|---|
| 1 | 30 files | fresh | **6,801** |
| 2 | +5 files | **resumed** | **8,007** |

Phase 2 is the whole point. It is the case the old code failed — a restored
watermark had advanced past the incoming events — and 6,801 + 1,206 = 8,007
means **all 1,206 late events landed**, where the watermark path lost 161
repos' worth. `observe()` reported 217 late events in phase 2, matching the
client-side count exactly, as it did during the original run. Distinct
`event_id` = 8,007 against 8,007 rows, so nothing was double-inserted either:
the MERGE suppresses genuine duplicates while keeping late first sightings.

**Two harness bugs, both mine, and only the second was interesting.** The
first was writing a function call from memory instead of reading its
signature. The second matters: the run "failed" on `distinct repo_id 5771 !=
5770` — but **Spark counts `NULL` as a distinct value**, and 2 of the 8,007
events carry no repo object at all. The pipeline was right and the assertion
was wrong. It now counts non-null repos and asserts `repoless_rows == 2`
explicitly, which is a stronger check than the one that "failed": those two
rows carry valid `event_id`s and must land.

Run on an ephemeral job cluster against the existing westus3 workspace — no
Terraform, no Lakebase, no online store. Verifying the write path did not
require re-provisioning the serving path.

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

### Cost — measured after the fact, as designed

The Lakebase instance ran ~3.2 hours at CU_1 (created 01:55 UTC, stopped
02:49–03:19 during a test run, destroyed ~05:1x).

**The rate could not be read during the window it measures**, which is why
it was deferred rather than skipped: `system.billing.usage` is *empty* in a
newly created metastore — 0 rows total, not merely 0 for this workspace —
and in the main westus3 metastore it lags roughly a day. Capturing
`workspace_id = 7405616381250124` before teardown is what made the rows
attributable afterwards. The rows landed overnight and the measurement was
taken on 2026-09-07:

```sql
SELECT date_format(usage_start_time,'MM-dd HH:mm'), usage_quantity
FROM system.billing.usage
WHERE sku_name = 'PREMIUM_DATABASE_SERVERLESS_COMPUTE_US_CENTRAL'
ORDER BY usage_start_time
```

**Billing lands in 10-minute buckets, and the idle draw is flat to four
decimal places.** `n = 14` consecutive full buckets (02:00–02:40 and
03:20–05:00), every one of them **0.1420 DBU**, zero variance:

| Quantity | Value |
|---|---|
| Idle draw, CU_1 | **0.852 DBU/hour** (0.1420 × 6) |
| SKU | `PREMIUM_DATABASE_SERVERLESS_COMPUTE_US_CENTRAL` |
| List price | **$0.59 / DBU-hour** |
| **Idle cost** | **$0.503/hour → $12.06/day** |
| Total for the instance's whole life | 2.1357 DBU = **$1.26** |

**Stopping the instance stops the meter completely.** The 02:50 → 03:10
gap contains **no usage rows at all** — not reduced rows, none — which is
the direct evidence that the stop-before-publish decision was worth making
rather than merely prudent. A stopped Lakebase instance bills nothing for
compute.

**The store was the cheap part.** Across the whole centralus workspace,
DBU spend at list price was **$3.69**, of which the online store was $1.26;
the largest single line was $1.62 of serverless SQL warehouse — the ad-hoc
queries run to *verify* the store, not the store itself.

**All-in, and §2's ratio holds.** Azure Cost Management reports **$2.71**
across the two `almanac-lb-*` resource groups over 09-06 → 09-07:

| Service | Cost | Share |
|---|---|---|
| Azure Databricks | $1.32 | 49% |
| Virtual Machines | $0.83 | 31% |
| Storage | $0.28 | 10% |
| NAT Gateway | $0.25 | 9% |
| Virtual Network + Bandwidth | $0.03 | 1% |

I first wrote that this stack was "entirely serverless, so §2's 53% ratio
does not transfer" — **wrong, and checking took one query.** The publish job
ran on a classic job cluster (`PREMIUM_JOBS_COMPUTE`, 2.25 DBU), which means
real VMs behind a NAT gateway. Databricks is 49% of the bill here against
§2's 53%: the ratio transfers almost exactly, and the general rule
(**a DBU figure is roughly half the true cost**) survives a second,
independently provisioned stack.

The $1.32 Azure figure and the $3.69 Databricks figure are **not reconciled**
and are not presented as if they were. Two candidate causes, neither verified:
09-07 is a partial, still-ingesting day in Cost Management, and
`list_prices` is list price while Cost Management reports actual billed cost.
Naming them beats implying agreement that was not demonstrated.

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
- **"The Lakebase rate is not in Databricks' price catalog"**
  (`2026-09-06-events-api-and-online-store-rates.md` §3) — **wrong, and it
  was wrong the first time too.** `system.billing.list_prices` has carried
  `PREMIUM_DATABASE_SERVERLESS_COMPUTE_*` for every region since
  `price_start_time = 2025-06-11`, ~15 months before this ran. The search
  terms were `LAKEBASE`, `POSTGRES`, `ONLINE`, `OLTP` — none of which appear
  in the SKU name, because **Databricks SKU names are meter names, not
  product names.** Checking Phase 5's identical claim shows the same failure:
  `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_3` has been priced since
  2013-01-01. So the "pattern" §3 asserted — newer serverless products are
  unresolvable before provisioning — was never a pattern; it was the same
  search mistake twice, generalized from `n=2` where both samples were the
  same error. Corrected in place at both sites.
- **The $0.26/DBU-hour retail-catalog rate was off by 2×.** Databricks' own
  catalog prices `..._US_WEST_3` at **$0.52**, and the region actually used,
  `..._US_CENTRAL`, at **$0.59**. The product-name inference ("Premium
  Database Serverless Compute is the Lakebase meter") was right; the rate
  taken from Azure's Retail Prices API was not. **Prefer
  `system.billing.list_prices` over the retail API** — it is the meter that
  actually bills, and it was queryable all along.
- The predicted range was $6.24–$25/day. Measured **$12.06/day**, inside it,
  but for compensating reasons rather than a good prediction: the DBU draw is
  *below* the assumed 1 DBU/hour (0.852) while the price is *above* the
  assumed $0.26 (0.59).
