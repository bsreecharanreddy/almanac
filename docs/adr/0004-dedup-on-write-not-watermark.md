# ADR-0004: Deduplicate on write with a Delta `MERGE`, not a Spark watermark

**Status:** Accepted 2026-09-07, replacing the design's original
mechanism. Implemented in `almanac.stream.ingest`.

## Context

Design doc §4.6 specified streaming dedup on `event_id` inside a
watermark — the same key Silver's batch dedup uses — via
`dropDuplicatesWithinWatermark`.

## Decision

No `withWatermark` / `dropDuplicatesWithinWatermark` in the streaming
path. Cross-batch dedup is an **insert-only Delta `MERGE`** per
micro-batch in `write_stream_silver`.

## Why the original mechanism was abandoned

It was built first, then measured against the live feed. A watermark does
not only deduplicate — **it drops**. Any record whose event time falls
behind the watermark is discarded, not merely de-duplicated. Against the
real GitHub Events feed:

- **18.0%** of a window arrives behind a 10-minute watermark
- **16.4%** carries an event time **over a day old**

So the specified mechanism would have silently deleted roughly a fifth of
the live stream in exchange for deduplication it was not even needed for.
That is a correctness failure that looks like a throughput graph.

## Alternatives considered

| Alternative | Why not |
|---|---|
| A **much longer watermark** sized to the observed lateness | Watermark state grows with the window; a >1-day watermark to catch the 16.4% makes state unbounded in practice, and still drops the tail. |
| **No cross-batch dedup**, relying on batch Silver | Violates the idempotency rule — "what happens if this reruns?" must answer "the same thing" — for the live table itself. |

## Consequences

`MERGE` is the documented Databricks pattern for deduplicating a stream
into Delta (checked live 2026-09-07), and it is unbounded in event time:
a duplicate arriving a week late is still caught.

A related guard came with it: a batch whose key column is entirely null
cannot be deduplicated by `pipeline.dedup`, so that case is raised as a
real anomaly rather than silently passing through.

**Evidence:** `docs/findings/2026-09-07-live-feed-era-and-watermark.md`,
`src/almanac/stream/ingest.py`.
