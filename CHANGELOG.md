# Changelog

Phase-level history. `docs/STATUS.md` carries the task-level verification
log; this is the summary a reader wants first.

Every number here is measured. Where one was later found wrong, it is
corrected **in place with the original kept**, because a retraction that
deletes its own evidence is not a retraction.

## Phase 9 — the agent layer, 2026-09-10 (targets `v1.1.0`, not yet released)

Strictly additive and strictly read-only: nothing under it changed, and
`v1.0` stays tagged where it was.

- **Four tools, one contract.** `get_features`, `predict`, `explain` and
  `versions`, typed once in `schemas.py`, served over MCP, and called only
  through an allow-list gateway that records every call. Per-feature
  contributions, promised by design doc §6 and never built, now exist and
  are served by `explain`.
- **The refusal reached a second tool before it could matter.** A window
  missing a feature the champion reads is refused by `predict` — and by
  `explain`, which would otherwise have published a contribution for a
  feature that does not exist: the same fabrication, through the
  explanation path.
- **A fallback that falls back only on a transient failure.** The
  framework's default routes any API error, 4xx included, to the fallback
  model, which would let every answer come from a model nobody chose.
  Tested on four failure shapes, then held live: a permanent 403 raised the
  primary's own refusal without consulting the fallback.
- **The paid window took five runs.** The tools held on the real feature
  store: `versions()` matched the live registry, point-in-time held at two
  instants with a byte-identical recompute, and the same score —
  **0.0038260014552166737** — came back on three separate clusters. Both
  scoring tools refused a real 2026 entity, but on feature-table coverage
  rather than the `is_draft` drift the design predicted (`n = 1`).
- **Neither configured model can serve the agent in this workspace.**
  Every Claude endpoint returns 403 for a Databricks-set rate limit of 0
  while reporting `READY`, and `gpt-oss-120b` replies in a shape
  `OpenAIChatModel` cannot parse. Llama 3.3 70B answered once, recorded in
  the evidence as a substitute.
- **Every number in that answer came from a tool; one claim did not.** It
  said the model was trained on the Delta version the tools *read* — v92,
  where the champion trained on v91. The transcript is committed as the
  first real case for Phase 10's grounding verifier.
- **The audit record timed the wrong thing.** The model's one record per
  run read **212.4 s**, of which about 7.4 s was the model: it spanned both
  tool calls and named only the last model to answer. Now one record per
  request, mutation-tested six ways.
- **Cost not yet known.** `system.billing.usage` is read on or after
  2026-09-11, and the token reconciliation the plan asked for cannot run in
  a workspace whose `system.serving.endpoint_usage` has never had a row.

## v1.0 — Phase 8 (ship), 2026-09-09

Tagged after the history rewrite described in the last two entries below.
**Every SHA cited in entries before this one predates that rewrite** and no
longer resolves; they are left as written, because the record of what was
true at the time is the point of keeping them.

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
- **The pseudonymity guard could not see the file it lives in.** The check
  that fails the build on an actor identifier in a published artifact had
  a contributor's machine name in its own docstring, and a second in its
  test. Its surfaces stopped at docs, dashboards, terraform and
  `.gitignore` — `src/` and `tests/` were never scanned, in a repo that
  had already gone public. The control was not buggy; it was pointed at
  the wrong files, and a control with the wrong scope passes cleanly and
  forever. The checklist already carried the item that would have caught
  it, unticked.
- **History rewritten before the tag.** `git filter-repo` over all **199**
  commits stripped three identifier strings, verified from a fresh clone
  of the public remote at **0 hits**, with the tip tree hash unchanged —
  the rewrite touched history and not one current file. Author and
  committer identities, dates and messages preserved.

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
