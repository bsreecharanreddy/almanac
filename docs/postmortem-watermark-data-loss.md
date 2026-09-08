# Postmortem: the streaming watermark silently dropped 18% of the live feed

**Date of incident:** 2026-09-07, Phase 6 Tasks 9→10
**Severity:** data loss, silent, in the steady state only
**Status:** fixed (ADR-0004), verified, and the test that endorsed it rewritten

---

## Summary

The streaming ingest deduplicated on `event_id` inside a 10-minute
watermark. A watermark does not only deduplicate — **it drops**. Any
record whose event time falls behind it is discarded, whether or not it
has ever been seen before.

Against the real GitHub Events feed, **18.0% of a window arrives behind a
10-minute watermark** and **16.4% carries an event time over a day old**.
Those are not duplicates; they are first sightings of unique events.

The bug did not fire on a cold start and fired on every run after —
**a first run that looks correct and a steady state that quietly is
not.**

## Impact

Measured across two consecutive live windows in one session:

| | Cycle A | Cycle B |
|---|---|---|
| Checkpoint | fresh (watermark at 0) | **resumed (watermark advanced)** |
| Events polled | 6,801 | 1,206 |
| Distinct repos polled | 4,925 | 943 (845 new) |
| Repos reaching the online store | 4,925 | 684 new |
| **Repos lost** | **0** | **161** |

Union of distinct `repo_id` across all 35 polls: **5,770**. Served by the
online store: **5,609**. The 161 missing are exactly that difference, and
every one belongs to cycle B — the run whose watermark had been restored
from a checkpoint.

No production system depended on this; the loss was confined to a
demonstration window. Had it not been caught, the streaming layer would
have shipped with a defect that is invisible on any first run and
therefore invisible to exactly the kind of smoke test people write.

## Timeline

1. **Design (§4.6)** specifies dedup on `event_id` inside a watermark —
   the same key the batch path uses.
2. **Phase 6 Tasks 2–3** implement it. A test is written asserting that a
   late row is dropped, and **justifying** that behaviour in its own
   assertion message.
3. **Task 9, first live window (cycle A).** Cold checkpoint. Everything
   reconciles: 4,925 repos polled, 4,925 served. Looks correct.
4. **The lag distribution is measured** over the same 6,801 events —
   18.0% beyond the watermark, 16.4% over a day old — and the
   consequence is recognised as a hypothesis, not yet a fact: whether
   those rows are actually lost depends on where the watermark sits when
   their batch runs.
5. **The prediction is written down before the second window runs.**
6. **Cycle B, resumed checkpoint.** 161 repos lost, as predicted.
7. **Task 10** replaces the mechanism with an insert-only Delta `MERGE`.

## Root cause

`dropDuplicatesWithinWatermark` was chosen for deduplication, but a
watermark is a **completeness** mechanism, not a deduplication one.
Adopting it imported a drop policy that nobody asked for and that the
design never stated.

The batch path deduplicates on `event_id` with no notion of lateness at
all. Reaching for the streaming API that had "dropDuplicates" in its name
imported semantics the batch equivalent does not have — the mistake is
matching on the name rather than on the guarantee.

## Why it was not caught earlier

**This is the uncomfortable part: the repo's own test asserted the buggy
behaviour, and explained why it was fine.**

`test_a_row_older_than_the_watermark_is_counted_even_though_it_is_dropped`
established that a late row is dropped, and justified it in its assertion
message:

> "poll 3's row is too late to land — correct here, since it is a genuine
> duplicate of poll 1's row, **not data loss**"

**That justification is true for the fixture and false for the feed.** In
the test, the late row *was* a duplicate, so dropping it was correct and
the comment was accurate. The test then generalised from its own fixture
to the mechanism, and wrote the generalisation down as reassurance.

A reader reviewing that test would see a deliberate, explained decision
rather than a defect. It is worse than an untested path: it is a **tested
path with a wrong explanation attached**, which converts the bug from
something a reviewer might notice into something a reviewer is actively
told not to worry about.

Three secondary reasons:

- **The fixtures could not produce the failure.** Every late row in the
  test data was a duplicate, so the distinct-late-event case did not
  exist to be tested.
- **A cold start hides it.** The first run is always correct, and first
  runs are what get demonstrated.
- **Nothing counted what was missing.** `late_event_count` counted late
  events, which sounds like the same thing and is not: an event can be
  late and still land. Only comparing the polled set against the served
  set reveals loss.

## What would have caught it earlier

In rough order of cost:

1. **A test with a distinct late event, not a duplicate one.** The single
   cheapest fix. The test that now exists —
   `test_a_distinct_late_event_is_dropped_not_just_deduplicated` — is
   four lines different from the one that endorsed the bug.
2. **Reading the watermark's contract rather than its name.** Spark
   documents that records behind the watermark are dropped. Nothing about
   this needed a live feed to discover; it needed one careful read.
3. **A conservation assertion in the replay harness.** The batch-equality
   gate compares outputs, not populations. `distinct input keys ==
   distinct output keys` would have failed on any replay with a resumed
   checkpoint.
4. **Measuring the lag distribution before choosing the watermark
   width.** 10 minutes was assumed reasonable; the real tail reaches 9+
   days. The width was never derived from data.

## What was done

**ADR-0004.** `dropDuplicatesWithinWatermark` removed entirely;
cross-batch deduplication moved to an insert-only Delta `MERGE` on
`event_id` in `foreachBatch`. The stream becomes stateless — no
watermark, no state store, nothing dropped for arriving late — and a
duplicate arriving a week late is still caught.

Two alternatives were rejected explicitly rather than by omission:
**widening the watermark** past the observed tail trades a correctness
bug for an unbounded-state one, and **accepting the loss** was available
(capture is best-effort against a firehose with no completeness
guarantee) but was not taken, because a silent steady-state loss is not
the same as a documented sampling ceiling.

The endorsing test was rewritten rather than deleted, so the distinction
it got wrong — duplicate-late versus distinct-late — is now the thing it
pins.

## What made this diagnosable

Worth recording, because it is repeatable:

**The prediction was written down before the confirming run.** Cycle B
was not a re-analysis of cycle A; it was a natural experiment whose
outcome was stated in advance. That makes the causal claim checkable
rather than reconstructed, and it is the difference between "we found a
correlation in the logs" and "we predicted 161 and got 161".

**Two independent measurements agreed exactly.** Spark's `observe()`
reported `late_events=217` for cycle B; counting `polled_at - created_at
> 600s` directly in the raw JSONL gave **217 of 1,206**. Neither number
came from the other. When an instrumented metric and a hand count of the
source agree to the unit, the mechanism is understood.

**An earlier reading was recorded and then corrected.** The first
explanation — "an accident of batching" — was right about the mechanism
and **too optimistic about the consequence**. It is kept in the finding
rather than overwritten, because the distance between those two readings
is the actual lesson.

## Lessons

1. **A test that asserts a behaviour is not evidence the behaviour is
   correct.** It is evidence someone once thought so. When a test's
   assertion message contains a justification, that justification is a
   claim like any other and can be wrong — and it is more dangerous than
   silence, because it pre-empts the question.
2. **A fixture that cannot produce the failure cannot rule it out.**
   Every late row in the test data was a duplicate, so the failing case
   was unreachable by construction.
3. **Match on the guarantee, not the name.** `dropDuplicatesWithinWatermark`
   deduplicates *and* drops. The batch path's dedup does not drop.
4. **Cold starts hide steady-state bugs**, and cold starts are what
   demos run.
5. **Measure the parameter you are about to assume.** A 10-minute
   watermark was a guess; the tail was 9+ days.
