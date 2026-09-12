# Changelog

Phase-level history. `docs/STATUS.md` carries the task-level verification
log; this is the summary a reader wants first.

Every number here is measured. Where one was later found wrong, it is
corrected **in place with the original kept**, because a retraction that
deletes its own evidence is not a retraction.

## v1.2.0 — Phase 11, the local demo, 2026-09-11

A Streamlit app scoring against a committed snapshot of the registered
champion, no cloud account needed to run it, deployed publicly at
[almanac-demo.streamlit.app](https://almanac-demo.streamlit.app/) for
anyone to click through.

- **The OpenMP segfault, never seen before because scoring had only ever
  run on Databricks.** Importing `mlflow` before `lightgbm` segfaults with
  no traceback — 10 of 10 runs on macOS arm64. This demo is the first
  thing in the repo to score on a laptop rather than a cluster. Fixed with
  an import-order guard, verified by reversal: swap the two imports and
  the regression test fails.
- **The feature-coverage measurement makes the governing invariant
  visible instead of asserted.** One archived hour holds almost no prior
  history, so most point-in-time features are legitimately null —
  rendered directly beside the queue rather than hidden, with the null
  count and the reason stated together.
- **The plan's own Dockerfile did not work, twice, and only running the
  built container caught either time.** `--extra demo` alone omitted
  lightgbm/mlflow; the fixed version still transitively imported pyspark
  two hops down (`panels.py -> bounded_agent -> gateway -> mcp_server`),
  past an import-graph test that checked only three files' own direct
  statements, not what they import. Fixed by moving the two functions the
  panel actually needed into a new pyspark-free module, and by rewriting
  the regression test as a transitive first-party-only AST walk — a
  subprocess-import version was tried first and rejected, because
  `mlflow` itself pulls in pyspark whenever pyspark happens to be
  installed in the environment doing the testing, which is a property of
  that environment, not of the demo's own code.
- **The same container measured 11.1GB**, almost all of it a
  `sentence-transformers` dependency (torch, transformers, CUDA wheels)
  pulled in for a Phase 5 embedding pipeline the demo never imports. A
  minimal `ml-scoring` extra cut it to 2.33GB with identical scoring
  output.
- **The deploy target changed after the container worked, on cost
  alone.** Creating a Docker Space on Hugging Face requires a paid
  personal plan, checked directly against Hugging Face's own docs rather
  than assumed. Streamlit Community Cloud is free and a better fit, since
  the demo already is a Streamlit app — no Dockerfile needed at all. The
  pivot needed a `sys.path` fallback (Community Cloud never runs `uv`, so
  the project package is never installed into its environment) and a
  pinned `requirements.txt` exported from the same trimmed extras,
  verified against a completely clean, `uv`-free `pip install` venv
  before trusting it would work on the actual platform.
- **A fifth tab, added after the phase's own deploy work, found three
  more defects the same way — by running it, not trusting a test.** An
  interactive architecture walkthrough indexes the project's real build
  story (this segfault, the leakage bug Phase 7 found, the watermark
  postmortem) rather than restating it — every node links to the ADR or
  finding where a claim was actually measured, enforced by a test that
  rejects a digit anywhere in a node's own text. `AppTest` cannot execute
  a third-party component's frontend canvas at all; a second, unrelated
  bug survived that and only broke in a real browser — `st.session_state`
  reused the flow component's own widget key, so Streamlit silently
  overwrote a stored state object with the component's raw return value
  on rerun. A third was found only by a person actually clicking the
  deployed page: the walkthrough's links rendered as inline code, never
  real hyperlinks, and a node click did nothing. All three fixed and
  reverified against the live site, not only locally.
- **CI found a defect the local suite could not, because it was the first
  run on a machine other than the one that built the committed
  artifacts.** 180 of the demo's 189 fixture rows tie exactly on
  predicted `breach_risk` (a sparse-feature hour scores most rows
  identically), and the queue's sort had no tiebreak — rank among tied
  rows silently followed whatever order Spark's `toPandas()` collected
  them in, stable on any one machine and never guaranteed across
  machines with different core counts. Fixed with a deterministic
  secondary sort on `(repo_id, pr_number)`, a real unique key over the
  fixture population.
- **Real, not staged.** No feature phase changed; nothing here re-trains
  or re-registers a model. The demo scores the same registered champion
  (version 2) that Phase 4 rescored and Phase 8 shipped.
- **What stayed rejected.** A real-quarter export was considered and
  dropped — it needed a billable window, a feature recompute inside it, a
  cost read after it, and a publication-safety review of data that has
  never been public in that form, for the least visible gain of anything
  in the design. The demo scores three committed fixture hours instead,
  labelled as exactly that rather than implied to be more.

## v1.1.0 — Phases 9 and 10, 2026-09-10

Tagged on `main` at `762a330`. Two phases in one release, because the
second exists to check the first: Phase 9 built the agent layer, Phase 9's
paid window found it making one false claim, and Phase 10 is the
deterministic verifier that catches that class of claim. Both are strictly
additive and strictly read-only, so everything `v1.0` describes is
unchanged underneath them.

### Phase 9 — the agent layer (PR #20, `6fc99cb`)

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
- **The window cost $5.59** in DBUs on 2026-09-10, read 2026-09-11 from
  `system.billing.usage` once the lag had passed
  (`docs/findings/2026-09-11-agent-layer-window-cost.md`). The token
  reconciliation the plan asked for cannot run in a workspace whose
  `system.serving.endpoint_usage` has never had a row, and is recorded as
  uncloseable here rather than dropped.

### Phase 10 — the grounding verifier (PR #21, `b73834d`)

Phase 9's window produced an answer where **every number came from a tool
and one claim still did not**. The answer said the model was *trained on*
the Delta version the tools had *read*. Nine build tasks, all offline.

- **Checking the number is not enough; the relationship around it is a
  separate check.** A verifier that only asks "does this literal appear in
  a tool result" passes the false claim, because 92 did appear. So claims
  are typed, and each type names the field it must trace to: "trained on"
  must trace to the training version, not the read one.
- **The cause was fixed, not only the symptom.** `ModelProvenance` carried
  a field named `delta_versions` that did not say what it was versions
  *of*. Renamed to `read_delta_versions` across the schema and its four
  construction sites, so the field name itself refuses the confusion.
- **A directional check, because a sign error reads as fluent prose.** A
  contribution that decreases risk described as increasing it quotes a real
  number and inverts its meaning. Each directional statement is checked
  against the sign of the contribution it names.
- **Retry once, then abstain.** An ungrounded answer is retried a single
  time and then returns an `Ungrounded` outcome. The agent says it cannot
  answer rather than publishing a claim that failed its own check.
- **No model call, so it is a test rather than a new CI job.** The verifier
  is regex and structural traversal over the run's own transcript.
  Deterministic by design, per design doc §6's "deterministic, not
  LLM-as-judge": a build gate needs a yes or no, not a score. It runs in
  the existing offline `pytest` step, and every run writes its trace to
  disk.
- **The verifier is mutation-tested**, one killing mutation per check,
  because a grounding check that cannot fail is worse than none.

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
