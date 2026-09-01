# Findings — a third schema era, and why we must not use recent data

**Date:** 2026-09-01
**Trigger:** the question "are we taking the latest data from 2026?"
**Answer: no, and we must not.** Evidence below.

---

## What changed

Between **2025-10-08 and 2025-10-15**, the `payload.pull_request` object
in the GitHub Events firehose was reduced from **48 fields to 5**.

| Field | ≤ 2025-10-08 | ≥ 2025-10-15 |
|---|---|---|
| `pull_request` key count | **48** | **5** (`base`, `head`, `id`, `number`, `url`) |
| `merged`, `merged_at` | present | **gone** |
| `created_at`, `closed_at` | present | **gone** |
| `draft` | present | **gone** |
| `user` (PR author) | present | **gone** |
| `title`, `body` | present | **gone** |
| `additions`, `deletions`, `changed_files` | present | **gone** |

Bisected by sampling one hour per date and counting keys on the first
`PullRequestEvent`:

```
2025-01-15  48    2025-10-01  48    2025-10-15   5    2026-01-15   5
2025-04-15  48    2025-10-08  48    2025-10-22   5    2026-03-15   5
2025-07-15  48                      2025-10-29   5    2026-05-15   5
2025-09-15  48                      2025-11-05   5    2026-08-28   5
```

## The volume collapse is real too

Comparing one hour in each era (2025-03-15 14:00 vs 2026-08-28 14:00):

| Measure | 2025-03-15 | 2026-08-28 | Change |
|---|---|---|---|
| Events in the hour | 227,376 | 54,232 | **−76%** |
| Mean bytes per event | 2,748 | 687 | **−75%** |
| `PullRequestEvent` | 12,420 | 369 | **−97%** |
| `PullRequestReviewEvent` | 2,323 | 90 | **−96%** |
| Bot share (`[bot]` suffix) | 18.2% | 2.4% | **−87%** |
| `PushEvent` share of firehose | 66.5% | 95.9% | — |

Note the comparison *understates* the drop: 2025-03-15 was a **Saturday**
and 2026-08-28 a **Friday**, so the recent weekday should carry *more*
activity, not a quarter as much.

Also observed: `PublicEvent` absent from the 2026 sample, and a new
`DiscussionEvent` type present.

## Why this is fatal for recent data

Every one of these is load-bearing in the current design:

- **`merged` is gone** → `fact_pull_request`'s merge outcome (§4.3) cannot
  be determined at all
- **`user` is gone** → the label is "first response from *someone other
  than the author*" (§5.1). Without the author, the label is uncomputable
- **`draft` is gone** → §5.1 requires drafts to be excluded from SLA time
- **`title` and `body` are gone** → Phase 5's embedding pipeline has
  nothing to embed
- **`created_at` is gone** → PR age, a core feature, is unavailable
- **`additions`/`deletions`/`changed_files` are gone** → every PR-size
  feature disappears

**Post-October-2025 data cannot support this project's model.** Not
"degraded" — structurally unable.

## Consequences for the design

**1. The modeling window must end before 2025-10-08.** The currently
planned Q1 2025 window is safely inside the rich era. The *most recent*
fully-rich quarter is **2025-07-01 → 2025-09-30**.

**2. There are three schema eras, not two.** The design's §12 trap 7 and
`SchemaEra` enum know about `legacy_v1` (pre-2015) and `modern_v2`
(2015→). A third, `reduced_v3` (2025-10 →), now exists. This is a net
*gain* for the project's schema-evolution story: three real breaks in one
dataset, with the most recent one undocumented anywhere and found by
measurement.

**3. Recommended split of responsibility:**
- **Bronze and Silver ingest all three eras.** That is the schema-evolution
  demonstration and it gets stronger, not weaker.
- **The feature platform and model are scoped to the rich era only**,
  because the label does not exist outside it. Stated as a deliberate,
  measured scope boundary rather than an unexplained window choice.

**4. This is why "use the most recent data" is the wrong instinct here** —
and being able to say exactly that, with the bisected date and the field
counts, is worth more than a fresher dataset would have been.
