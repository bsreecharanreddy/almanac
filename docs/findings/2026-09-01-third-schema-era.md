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

## The volume change is PR-specific, not general

**Corrected 2026-09-01.** An earlier version of this document claimed
total event volume fell ~76%. That was wrong — it generalized from a
single hour (`2026-08-28-14`, 54,232 events) that turns out to be
anomalous. Four more 2026 hours were sampled:

| Hour | Total events | `PullRequestEvent` | PRs opened | Reviews |
|---|---|---|---|---|
| 2026-08-12 14:00 | 162,301 | 770 | 265 | 228 |
| 2026-07-15 14:00 | 160,644 | 364 | 130 | 102 |
| 2026-06-10 14:00 | 154,171 | 532 | 209 | 141 |
| 2026-08-28 14:00 | *54,232* | 369 | 151 | 90 |
| 2026-08-19 14:00 | *3,511* | 268 | 120 | 88 |
| **2025-08-13 14:00** | **167,303** | **13,181** | **6,618** | **4,943** |

**Total firehose volume is essentially unchanged** — ~155–162K events per
hour in 2026 against 167K in 2025. Push and create events continue at
normal rates.

**What collapsed is pull-request activity specifically:** roughly
**25–50× fewer** PR events, and **~30–40× fewer opened PRs** (≈130–265/hr
against 6,618/hr). Review events fell comparably.

Two of the six sampled hours (54,232 and 3,511 events) are clearly
**truncated captures** — a live instance of §12 trap 5, and a reminder
that any single hour is a bad basis for a claim. The lesson repeats from
the bot-classification finding: do not generalize from one sample.

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

**2. The scarcity is in labels, not in events.** A 2026 slice still
carries plenty of *events*; it carries ~30–40× fewer *labelled PRs*. That
is what makes it unsuitable as training data, independent of the payload
reduction.

**3. There are three schema eras, not two.** The design's §12 trap 7 and
`SchemaEra` enum know about `legacy_v1` (pre-2015) and `modern_v2`
(2015→). A third, `reduced_v3` (2025-10 →), now exists. This is a net
*gain* for the project's schema-evolution story: three real breaks in one
dataset, with the most recent one undocumented anywhere and found by
measurement.

**4. Recommended split of responsibility:**
- **Bronze and Silver ingest all three eras.** That is the schema-evolution
  demonstration and it gets stronger, not weaker.
- **The feature platform and model are scoped to the rich era only**,
  because the label does not exist outside it. Stated as a deliberate,
  measured scope boundary rather than an unexplained window choice.

**5. This is why "use the most recent data" is the wrong instinct here** —
and being able to say exactly that, with the bisected date and the field
counts, is worth more than a fresher dataset would have been.
