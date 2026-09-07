# Data contract and SLA

What a consumer of Almanac's published tables is entitled to rely on, what
may change without warning, and what this system does not promise.

**Every number here cites the finding that measured it, and states its `n`.**
Nothing below is a target, an aspiration, or a round figure someone liked the
look of — if a row has no citation, it is a rule the code enforces rather than
a measurement, and the "Enforced by" column says where. Where a measurement
rests on a single window, it says so; a single window is not a rate.

Distinct from `tests/integration/test_contracts_enforced.py` and
`tests/integration/test_gold_contracts.py`, which make a breach *fail a build*.
Those decide what the pipeline may write. This document says what a reader may
depend on, which is a larger claim and mostly cannot be expressed as an
assertion.

---

## 1. Governed surfaces

Six tables carry a contract. Anything else in the lake is internal and may
change without notice, including Bronze.

| Surface | Kind | Key | Contract enforced by |
|---|---|---|---|
| `gold.fact_pull_request` | batch fact | `(repo_id, pr_number)` | dbt `contract: enforced`, `on_schema_change: fail`, `relationships` to `dim_repo`, and `assert_fact_pull_request_grain.sql` on every build |
| `gold.agg_repo_daily` | batch aggregate | `(repo_id, activity_date)` | dbt `contract: enforced`, `on_schema_change: fail`, `relationships` to `dim_repo`, and `assert_agg_repo_daily_grain.sql` |
| `features.author_activity` | offline feature | `(author_login, event_time)` | `almanac.contracts` — shape + Delta CHECK |
| `features.repo_activity` | offline feature | `(repo_id, event_time)` | `almanac.contracts` — shape + Delta CHECK |
| `features.pr_static` | offline feature | `(repo_id, pr_number)` | `almanac.contracts` — shape + Delta CHECK |
| `features.repo_stream_activity` | online feature | `(repo_id, event_time)` | `almanac.contracts` — shape + Delta CHECK |
| `features.actor_stream_activity` | online feature | `(actor_login, event_time)` | `almanac.contracts` — shape + Delta CHECK |
| streaming Silver | live event table | `event_id` | `almanac.contracts` — shape + Delta CHECK |

**Bronze is deliberately not contracted.** It is raw event JSON plus landing
metadata, kept replayable rather than stable, and its shape follows whatever
GitHub sent. A consumer reading Bronze is reading an implementation detail.

---

## 2. Freshness

### Streaming Silver and the online feature store

Measured end to end — `created_at` on the event to readable in Lakebase —
over **976 floor-cohort events** (those at the GitHub API's 300-second
minimum, excluding late arrivals), 2026-09-07:

| Component | p50 | Share |
|---|---|---|
| GitHub feed delay (**upstream, not ours**) | 302 s | 54% |
| Almanac poll → served | 260 s | 46% |
| **End to end** | **561 s** | |

Range: min 434 s, p95 688 s, max 690 s.
Source: `docs/findings/2026-09-07-live-feed-era-and-watermark.md`.

**The upstream floor is stated because the SLA cannot be better than it.**
GitHub's events API does not publish an event until roughly 300 seconds after
it occurs. Any freshness promise below ~302 s is a promise about someone
else's system, and this project will not make one. Of Almanac's own 260 s,
only **121 s is pipeline work** (ingest 68 s, features 35 s, publish 18 s);
the remaining ~139 s is the bounded-window design — an event captured by the
first poll waits for the fifth to finish before ingest begins. A continuous
trigger removes that term. It is not latency the code is spending.

> **This is an `n=1` window.** One 5-poll cycle on one day. It is the best
> number this project has and it is enough to state a floor, but it is not a
> distribution, and a consumer should not treat p95 688 s as a percentile
> established over time.

### Batch Gold and offline features

**No freshness SLA, on purpose.** These are rebuilt by an explicitly
triggered job, not on a schedule, because the lake holds a fixed historical
quarter rather than a moving window. `dbt` `build` is idempotent, so a
consumer's guarantee is reproducibility, not recency — see §5.

---

## 3. Completeness

| Property | Measured | `n` | Source |
|---|---|---|---|
| Duplicate events in streaming Silver | **zero** — 6,801 distinct `event_id` from 6,801 rows | one 30-poll window | `2026-09-07-live-feed-era-and-watermark.md` |
| Offline↔online consistency | 4,925/4,925 distinct `repo_id`; 3,721/3,721 distinct `actor_login` | one window, checked against raw JSONL | same |
| Rows rejected by quality rules | **0 of 341,060,851** | full 92-day Q3 2025 backfill | `2026-09-02-zero-quarantined.md` |
| Share of the GitHub firehose captured | **~7–9%** | two `n=1` windows at different times of day | same + Phase 6 Task 1 |

**The capture fraction is the one a consumer is most likely to misread.**
Almanac polls a sampled public feed; it does not see every GitHub event and
has never claimed to. Two windows measured 7.1–7.4% and 8.5–8.9% against an
archive-measured 155–162K events/hour. The honest statement is "the same
order, ~7–9%" — not a revision of one by the other, and not a rate.

**Late events are kept, not dropped.** 18.0% of a window arrives more than
10 minutes behind its event time and 16.4% carries an event time over a day
old, so a watermark that discarded them would discard real data — it did,
once, costing 161 repos before the dedup moved to an insert-only MERGE. A
consumer may therefore see a row appear whose `created_at` is arbitrarily
old. That is correct behaviour, not a defect.

---

## 4. Correctness invariants

Enforced at write time, so a violation fails the run rather than reaching a
reader. Full list in `almanac/features/runner.py`, `almanac/stream/runner.py`
and `almanac/stream/ingest.py`.

- **Keys are never null and never duplicated.** Non-nullity is a Delta CHECK
  constraint on the table itself; uniqueness is asserted per surface in the
  test suite, because uniqueness is not row-local and no constraint can
  express it.
- **A rate is a rate.** `prior_merge_rate` and `bot_share_to_date` are in
  `[0, 1]` or null.
- **A null means unknown, never zero.** `prior_merge_rate` is null exactly
  when the author has no prior closed PR; the four streaming activity features
  are null together or present together. Folding "no history" into "no
  activity" would teach a model an outcome it could not have had.
- **Time runs forwards.** `secs_since_last_event` is never negative.
- **Counts nest.** `events_prior_1h ≤ events_prior_24h`;
  `bot_events_to_date ≤ events_total_to_date`.

---

## 5. Reproducibility

**A feature vector computed as-of T is reproducible byte-for-byte from the
same Delta version, indefinitely.** This is the strongest guarantee here and
the one the project exists to demonstrate. It holds even after later data —
including a late-arriving correction whose own event time precedes T — has
been appended, *provided the Delta version is pinned*. A live re-query
filtered on `event_time` is leak-free but **not** reproducible, because more
history can arrive between builds.

Proven by `tests/integration/test_features_leakage.py`, which appends a late
correction and reproduces the original vector from the pinned version.

---

## 6. Retention

| Artifact | Retention | Note |
|---|---|---|
| Delta tables (all tiers) | indefinite | subject to `VACUUM`; no retention job runs |
| Column lineage (`system.access.*`) | **rolling 1 year** | Catalog Explorer and the lineage API retain indefinitely for lineage captured after 2024-09-01 |
| Inference log (`serving_logs.pr_review_sla_risk_payload`) | indefinite | `force_destroy = false`; the one artifact here that cannot be re-derived at any price |
| Streaming checkpoints | until deleted | deleting one replays the landing zone |

**The inference log's series begins when capture was switched on, and not
before.** Nothing can recover predictions made earlier.

---

## 7. Schema stability

### What will not change without a major version

- A contracted column being **removed** or **renamed**.
- A contracted column's **type widening or narrowing**.
- A **key** changing — its columns, or what a row means.
- An invariant in §4 being **weakened**.

### What may change without notice

- **New columns appended** to any contracted surface. A consumer selecting
  `*` and assuming a column count will break; that is not a contract breach.
- Anything in **Bronze**, the landing zone, or the quarantine tier.
- Physical layout: partitioning, file sizes, `OPTIMIZE`/`ZORDER`, whether a
  table is managed or external.
- The **set of checks** on a table, in the tightening direction. A new
  constraint may reject data an older writer would have produced.

### Schema eras are upstream, and not a version of ours

GitHub's own payload has three shapes, and Almanac normalises all three into
one Silver schema:

| Era | From | Consequence |
|---|---|---|
| `legacy_v1` | before 2015-01-01 | no `issue.pull_request`; `push_distinct_size` null |
| `modern_v2` | 2015-01-01 | the full payload |
| `reduced_v3` | 2025-10-15 | **no merge outcome, no PR author history, no PR text** |

**`reduced_v3` events are ingestible but not modelable.** The label in §5.1
of the design doc is uncomputable from them, which is why the streaming
feature groups compute entity *activity* and nothing else. A consumer
building on the live path gets activity features; a consumer needing
outcomes must use the historical quarter.

An era boundary is a fact about GitHub, not a version bump here. A fourth era
would land the same way: normalised into the same Silver schema, with
whatever it cannot supply left null.

---

## 8. What this does not promise

Stated plainly, because a contract that only lists guarantees reads as
though the gaps do not exist.

- **No availability SLA.** There is no on-call rotation, no uptime target, and
  no alerting on staleness. The streaming path runs in bounded windows that a
  human starts.
- **No completeness against GitHub.** See §3: ~7–9% of the firehose.
- **No freshness guarantee for batch surfaces.** They are rebuilt on request.
- **No guarantee that a served prediction is current.** The serving endpoint
  reads the online store; if the streaming window is not running, values are
  as fresh as the last window.
- **Local runs contribute no lineage.** The lineage artifact is generated from
  Unity Catalog, which records only work executed on Databricks. An edge's
  absence is not evidence it does not exist in code.

---

## 9. How to check any of this yourself

Every claim above is either enforced by a command or measured by one.

```bash
make check          # every contract assertion, including the deliberate breaches
make lineage        # regenerate the column-lineage artifact (needs a workspace)
make dbt            # rebuild Gold; a contract breach reddens the build
```

The findings cited in each section carry the exact query or command that
produced their numbers, so a reader can re-run them rather than take them on
faith. Where a measurement cannot be re-run — the torn-down Lakebase window,
for instance — the finding says so.
