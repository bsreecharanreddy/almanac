# Changelog

Phase-level history. `docs/STATUS.md` carries the task-level verification
log; this is the summary a reader wants first.

Every number here is measured. Where one was later found wrong, it is
corrected **in place with the original kept**, because a retraction that
deletes its own evidence is not a retraction.

## Unreleased — Phase 8 (ship)

- **The champion was re-scored on a temporal split and the published
  number went down.** `0.612 → 0.4661` PR-AUC against a baseline that also
  moved (`0.2845 → 0.2650`): the figure carried since 2026-09-04 was
  optimistic by **24%**. The winning configuration changed too,
  `default → is_unbalance` — so a random split would have shipped the
  wrong hyperparameter config, not merely an inflated score.
- **Window characterization, with a refusal.** The 2026 firehose is
  intermittently degraded — the same hour-of-day swings **50×** in size
  across 14 consecutive days — so a window chosen by recency is unusable.
  Quality is measured before use and a bad window is refused by name.
- **Offline drift** between the training and scoring windows, closing
  §10's longest-standing gap. Schema drift is reported *instead of*
  covariate drift, with a stated response rather than a chart.
- **`pr_merged` recovered from `action`.** The reduced era replaced
  `pull_request.merged` with a distinct `action='merged'`; the parser read
  only the field, leaving the column NULL across the whole era.
- **The PR-opened spine fan-out fixed** — 25 PRs carried at 4× weight
  because two builders emitted per *event* where the grain is the *PR*.
- **A published rate corrected**, and the first draft of the correction
  corrected first: the −97% PR-volume claim was not a small-sample error
  as initially written, but a **non-stationary** quantity re-quoted as
  current.
- **The quarantine path fired for the first time in 341M+ rows** — 100
  ForkEvents whose `repo` node the 2026 archive ships as an empty object.
  Quarantined not dropped, `_failed_rules` analysable by rule, and the split
  conserves: bronze−silver moved 62 → 162, being 62 dedup plus exactly 100.
- Repository made public; branch protection set to `deletion` +
  `non_fast_forward` only.

## Phase 7 — governance, reporting, reproducibility (PR #14, `62e006d`)

14 planned tasks plus an unplanned 15th.

- **Found a leakage bug in the registered champion** — the train/test
  split was random where the design requires temporal — **by writing the
  limitations document, not by testing.** The leakage suite was green
  throughout and was not wrong: it tested row time; the bug was on split
  time.
- Contracts became executable rather than declarative, each proven by a
  breach that actually fails the build.
- Column lineage resolved **by path**, published with the edges the
  catalog cannot see stated on it.
- Three AI/BI dashboards as committed JSON, demonstrated against the real
  quarter inside a narrow paid window, then torn down. The window found
  four defects nothing offline could.
- **§9's exit gate measured: 4 m 28 s** from clone to a green run on a
  cold cache, against a 15-minute bar.
- 88% coverage on transformation and feature logic, gated at 85% in CI.

## Phase 6 — streaming ingest and the online store (PR #13, `1cff101`)

- Live poller, streaming Silver, two online feature tables served from
  Postgres. Gate demonstrated across two live windows: **84 served values
  changed, 684 added, none lost.**
- **A watermark bug that was silently discarding distinct late events**,
  costing 161 repos in a real run — predicted, confirmed, then replaced
  with stateless dedup-on-write.
- The live feed captures **~7%** of real event volume: a politeness
  interval honoured on purpose, which is why a replay harness carries the
  correctness proof.

## Phase 5 — embeddings and vector search

- 1.86M embeddings over the real quarter; a live ANN index queried for
  real. Three genuine bugs found and fixed before any downstream number
  was quoted.
- **A `STANDARD` Vector Search endpoint does not scale to zero** — a flat
  4 DBU/hour whether queried or idle, measured, then torn down.

## Phase 4 — model, MLflow, serving

- **A measured null result, published not buried**: LightGBM lost to a
  per-segment median by 51% on the regression target.
- Reframed to classification, which won by a measured margin, registered
  live in Unity Catalog with a `@champion` alias.
- Serving endpoint measured: warm **p50 263.5 ms / p95 376.8 ms**, cold
  start **51.96 s**.

## Phase 3 — the feature platform

- Point-in-time-correct offline store, as-of joins, and the leakage suite.
- An **O(N²) self-join** found at real scale — twice. The second, in
  `as_of_join` itself, was the consequential one.

## Phase 2 — Gold, and the Azure burn

- **341,060,851 rows** over Q3 2025, 92 of 92 days, zero missing hours,
  for a measured **$11.96** — 38% of the estimate.
- Kimball Gold with SCD2 repos and an accumulating snapshot.

## Phase 1 — Bronze and Silver

- Config-driven runner, idempotent writes, quarantine by rule.
- **Zero of 341M Q3 2025 events were ever quarantined** — a result that reads
  as a broken panel and is not. *(Phase 8 fired it for real: 100 ForkEvents
  in the 2026 window, whose `repo` node the reduced era ships as `{}`. The
  mechanism's first firing on real data, and the source's fault, not the
  parser's.)*

## Phase 0 — exploration

- A **third schema era** found by asking rather than assuming, which
  changed the design before code was written against the old one.
- Bot classification: volume-ranked sampling gave **86.7% false
  positives**, because a sample drawn along the axis being measured proves
  nothing.
