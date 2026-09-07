# The live feed breaks two assumptions the streaming ingest was built on

**Date:** 2026-09-07
**Phase:** 6, Task 9
**Status:** one fixed, one open and needing a decision

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

### The caveat that keeps this from being a live outage

Whether those rows are actually lost depends on where the watermark sits
when their batch runs. Today the job polls to completion *first*, then runs
ingest with `available_now=True`, so all 30 files are read in one batch
whose watermark has not advanced — and they survive. **That is an accident
of batching, not a property of the design.** In the continuous shape
`write_stream_silver`'s own docstring calls "Task 9's real shape", the
watermark would have advanced and roughly one event in six would vanish.

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
