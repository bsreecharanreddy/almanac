# Almanac local demo — design

**Date:** 2026-09-11
**Status:** proposed, awaiting approval
**Scope:** Phase 11, targeting `v1.2.0`
**Depends on:** `v1.1.0` (`762a330`) — the champion, the grounding verifier,
and the committed transcripts all predate this phase
**Open decisions:** none remaining; see §11

---

## 1. The decision, stated plainly

**Build a Streamlit demo that runs with no cloud account, and deploy it
publicly to Hugging Face Spaces.**

It reads committed artifacts and scores with a committed copy of the
registered champion. It starts no SparkSession, holds no credentials,
and makes no network call at view time.

**What changes:** the repo gains a demo surface and a public deployment.

**What does not change:** the medallion, the feature platform, the
champion, the streaming path, the contracts, the agent layer. Nothing
under `v1.1.0` is modified. This phase is additive and read-only, with
one exception recorded in §6.1.

## 2. The gap this closes

The README says, accurately, **"No persistent public demo."** Everything
billable is torn down on purpose and the cost of each teardown is
measured. That is the right call for spend and the wrong outcome for a
reader who has five minutes and wants to see the thing work.

The evidence in this repo is strong and entirely static: screenshots of
dashboards that no longer exist, findings with their `n` and their
method, a transcript of an agent answering once. A reader must take all
of it on trust or clone the repo and run a test suite.

**A public link is the one artifact this project has never had.** It is
also the cheapest one left to build, because the two things worth showing
— a real model scoring real vectors, and an agent's claim being
mechanically checked — both already exist and neither needs a cluster.

## 3. What this is not

Stated first, because the failure mode of a portfolio demo is pretending
to be a product.

- **Not a serving path.** The champion is loaded from a file, in-process.
  Model Serving still exists and is still the real path; this is not it.
- **Not trained here.** §5.3 measures why the fixtures cannot support
  training. No model is fitted in this phase.
- **Not the 341M-row quarter.** Every number the demo renders is computed
  from one archived hour per era, and the app says so on every panel that
  shows a count.
- **Not a second source of truth.** Where the demo and `docs/findings/`
  disagree, the findings win. The demo cites them; it does not restate
  them.

## 4. The four panels, and where each gets its data

| Panel | Source | Computed when |
|---|---|---|
| **Intervention queue + feature coverage** | Committed demo artifact, scored by the committed champion | Build step |
| **Per-feature contributions** | Committed champion, live, via `contribution_frame` | View time |
| **Agent replay + grounding** | Committed transcripts and golden set, via `almanac.agent.grounding` | View time |
| **Medallion** | Committed demo artifact of Bronze/Silver/quarantine counts | Build step |

### 4.1 Intervention queue, with its coverage panel beside it

The queue ranks the fixture's pull requests by predicted breach risk.
Measured 2026-09-11 on the two eras the local run lands: **137 opened
pull requests** out of 3,997 Silver rows.

Beside it, and given equal weight, the **feature coverage panel**:

| Feature | Non-null of 137 |
|---|---|
| `is_bot_author`, `opened_day_of_week`, `opened_hour` | 137 |
| `is_draft` | 80 |
| `prior_pr_count` | 17 |
| `events_total_to_date`, `bot_events_to_date`, `prs_opened_to_date`, `bot_share_to_date` | 13 |
| `prior_merge_rate` | 0 |

**Seven of ten features are null for roughly ninety percent of rows, and
one for all of them. This is the governing invariant working, not a
defect**, and the panel says so in those words. A feature named "to date"
may read only events strictly before its own `as_of`; one archived hour
contains almost no prior history to read. The queue is therefore ranking
on three usable features, and the panel states that rather than letting a
plausible-looking ordering imply otherwise.

This is the design's deliberate centrepiece. A reader who understands
leakage recognises it immediately; a reader who does not is shown it in
ten seconds, which no paragraph in the README has ever achieved.

### 4.2 Per-feature contributions

Selecting a queue row computes its contributions live, through
`almanac.model.contributions.contribution_frame` — already a pure
function over anything exposing LightGBM's `pred_contrib` interface, so
this phase adds no scoring logic. The panel renders each feature's pull
against the model's own baseline, strongest first.

This is the repo's stated rule made visible: *an LLM never produces a
number that a decision depends on; an explanation is generated from the
model's actual feature contributions.*

### 4.3 Agent replay and grounding

Replays the Phase 9 window's committed transcript through
`almanac.agent.grounding`, showing each numeric literal traced to the
tool return it came from, each typed claim checked against the field that
claim type requires, and each directional statement checked against the
sign of its contribution.

Then it replays the direction-flipped copy and shows the verifier
**failing** it. A verifier that is only ever shown passing is
indistinguishable from one that cannot fail.

No model call. Both transcripts and both grounding traces are committed.

### 4.4 Medallion

Bronze rows read, Silver rows written, quarantine rows and their
`_failed_rules`, split by schema era. Produced by the build step, not at
view time. It demonstrates the pipeline's shape; `make dbt` remains the
way to actually run it.

## 5. The data path

### 5.1 Build step and app are separate processes

```
make demo-build   # Spark: fixtures -> Silver -> spine -> features -> score
                  # writes committed artifacts under demo/data/
make demo         # Streamlit: reads demo/data/, scores live with the champion
```

**The app never imports `pyspark` and never starts a JVM.** That is what
makes it start instantly, deploy to a free Space, and stay debuggable.

### 5.2 The artifacts are committed, and a test proves they regenerate

The build step's outputs are committed to the repo. They derive
deterministically from committed fixtures and a committed model, so they
are reproducible by construction — and a test regenerates them and
asserts the result is **byte-identical** to what is committed.

This is the project's own governing invariant applied to its demo: *the
same inputs produce a byte-identical artifact, a year later.* It also
means the Space needs no build pipeline and no Spark, and that a stale
artifact fails the suite instead of silently shipping.

### 5.3 Why nothing is trained here

Measured 2026-09-11: the modern fixture carries 149 `PullRequestEvent`s
in its hour, the legacy 106, the reduced 131. A label needs a first human
response *after* the pull request opens, and one hour contains almost
none. Training on this would produce a number, and the number would mean
nothing. §3 of `CLAUDE.md` forbids displaying such a number as though it
did.

The committed champion is the real one: version 2 under the `@champion`
alias, run `5f6a71bd9e4f4b6d8f7ea0a6454c0ca5`, the same pair the Phase 9
window read back from the live registry.

| Property | Value |
|---|---|
| Artifact total | 432 KB |
| `model.skops` | 404,577 bytes |
| Trees | 100 |
| Features | 10 |

The artifact carries its own MLflow signature naming all ten features, so
the demo's feature contract is pinned by the model file rather than
restated beside it.

## 6. Two defects this phase must contain

### 6.1 The OpenMP segfault — the one change to existing code

**Measured 2026-09-11, macOS arm64, Python 3.12:**

| Import order | Successful runs |
|---|---|
| `lightgbm` before `mlflow` | 10 of 10 |
| pandas, numpy, then `mlflow` | 0 of 10 |
| numpy then `mlflow`, no explicit `lightgbm` | 0 of 3 |

Loading the champion and scoring it **segfaults deterministically** —
signal 11, no traceback — unless `lightgbm` is imported before `mlflow`.
When mlflow imports lightgbm lazily, after numpy has bound its own
OpenMP runtime, a second copy loads and the process dies.

This never surfaced before because scoring only ever ran on Databricks
runtime. It surfaces now because this phase is the first to score on a
laptop.

**The fix is structural, not a comment.** One import module establishes
the order once and is imported first by everything that touches the
model. A regression test fails the build if any module in the demo
package reaches `mlflow` ahead of `lightgbm`. A comment cannot fail a
build, and this repo has a documented history of conventions that
survived only as habit until a context boundary broke them.

This is the phase's one change to code outside the demo package, and it
is a bug fix rather than a feature.

### 6.2 Fixture-scale numbers that look like measured ones

Every panel showing a count or a score carries the scale it was computed
at. The app states on load that it runs one archived hour per era, not
the 341,060,851-row quarter.

Phase 8 found four defects that a green 543-test suite could not, and one
of them was a dashboard rendering **200 healthy-looking rows** against a
horizon 340 days wrong. Plausible rendering of a wrong number is this
project's most expensive recorded failure mode, and a public demo is the
highest-exposure place it could recur.

## 7. Identity: what never renders

`docs/pseudonymization.md` is explicit — human GitHub logins are never
published, and `owner/repo` names are not either, because the owner half
is usually a person. Measured in the modern fixture: **1,485 distinct
actor logins and 1,845 distinct repository names.**

Those fixtures are already public in this repo as raw archive data. What
changes here is *rendering them as a readable ranked list*, which the
policy treats differently from storing them.

- The queue renders `repo_id` and rank position. Never a login, never an
  `owner/repo`.
- Bot versus human stays a **toggle**, because it is a property rather
  than an identity.
- The build step strips identity columns from the artifacts it writes, so
  the published file cannot carry what the app declines to draw.
- `pseudonymity.py`'s surfaces are extended to cover `demo/` and the
  Space's deploy folder.

That last point is the lesson this repo has already paid for once: the
check was not broken when it missed a leak, it was **aimed at the wrong
files**. A control with the wrong scope passes cleanly and forever.

## 8. Testing

| Layer | What must be covered |
|---|---|
| Build step | Artifacts regenerate byte-identically (§5.2); identity columns absent from every written file |
| Import order | Any demo module importing `mlflow` before `lightgbm` fails the suite (§6.1) |
| Scoring | The committed champion loads offline and scores a known vector to a pinned value |
| Panels | Driven headlessly with Streamlit's `AppTest`, not verified by screenshot |
| Contracts | The artifacts are governed surfaces and get contract tests like every other one |

`streamlit` enters as a new optional-dependency group, so the default
install and CI's existing jobs are unaffected unless they ask for it.

## 9. Deployment

**Local first, Spaces last.** The Space is the final task, so the phase
delivers a working local demo even if deployment stalls.

Hugging Face Spaces over Streamlit Community Cloud, for two reasons: an
explicit Dockerfile pins the native dependency stack rather than leaving
it to a hosted resolver, which §6.1 says is exactly what this app needs;
and the Space is its own repository, so what it publishes is an explicit,
reviewable set of files rather than whatever the main repo happens to
contain.

The Space runs Linux, where the §6.1 crash does not occur. **That is a
reason to keep the guard, not to drop it** — the bug's whole danger is
that it is invisible in the environment where the app is deployed and
fatal in the one where a reader clones it.

Free tier. No billable resource is created by this phase.

## 10. Exit gate

- [ ] `make demo-build` regenerates the committed artifacts byte-identically
- [ ] `make demo` opens all four panels with no SparkSession and no network call
- [ ] The champion scores a pinned vector to a pinned value, offline
- [ ] The import-order regression test fails when the order is reversed
- [ ] The grounding panel shows the flipped-direction transcript being **rejected**
- [ ] No login and no `owner/repo` appears in any committed demo artifact
- [ ] `pseudonymity.py` covers `demo/` and the deploy folder
- [ ] Full `make check` green
- [ ] The Space is live and its URL is in the README
- [ ] The README's "No persistent public demo" paragraph is rewritten to match

## 11. Decisions taken, and what was rejected

| Decision | Rejected alternative | Why |
|---|---|---|
| Commit the champion artifact | Download it at build time | A demo that phones Databricks is not a demo that runs without a cloud account. 432 KB is affordable. |
| Streamlit on Hugging Face Spaces | Vercel | Streamlit needs a long-lived server holding a WebSocket per viewer. Vercel runs serverless functions and static output, and cannot host it. |
| Fixture data only | Export a real-quarter slice | The export needed a billable window, a feature recompute inside it, a cost read after it, and a publication-safety review of data that has never been public in that form. It was the most expensive item in the design and bought the least visible gain. |
| Sparse features shown, not hidden | A larger committed fixture | The sparsity *is* the point-in-time invariant made visible. Enlarging the fixture to hide it would trade the best panel for repo weight. |
| Four panels | The point-in-time panel as a fifth | §4.1's coverage panel already makes that argument, and the committed transcript already carries the byte-identical recompute. |

**The counter-argument, recorded rather than dismissed:** a demo on 137
rows invites the reading that the platform only ever handled 137 rows.
The mitigation is §6.2's labelling plus the queue panel linking directly
to the 341,060,851-row backfill finding. If that proves insufficient in
practice, the rejected export is a separate phase with its own window,
not a reason to build it now.

## 12. Risks

- **The demo becomes a second stale record.** This repo has nine recorded
  instances of a hand-maintained statement outliving its truth. The
  mitigation is §5.2: the artifacts are tested, so staleness fails the
  suite rather than shipping quietly.
- **A public surface is a maintenance obligation.** If the Space breaks,
  it breaks in public. Accepted deliberately, and the README will link it
  as a demo rather than as infrastructure.
- **`AppTest` coverage may prove shallow** for panels that are mostly
  layout. Where it cannot assert something meaningful, the phase records
  that rather than asserting something trivial to claim coverage.
