# Almanac (CLAUDE.md)

Read this before doing anything in this repo.

## What this project is

Almanac is an **ML platform for work-queue risk**: work items arrive in a
queue, some breach their service expectation, and a model predicts which
ones early enough for a human to intervene.

It is built on GitHub's public event firehose (GH Archive) because that
dataset is real, large, free, genuinely messy, and carries a real schema
break. **The domain is incidental and that is the point** — the same
architecture serves a support-ticket queue, a claims backlog, or a fraud
review queue. Nothing in the platform layer knows the items are pull
requests.

Positioned for **AI/ML platform engineering** roles, not data
engineering. Where a choice would strengthen the DE story at the expense
of the ML-platform story, the ML-platform story wins. The DE core is
still built in full — an ML platform with no data platform under it is a
notebook — but it is a means, not the deliverable.

## Read this first

**`docs/design/2026-09-01-almanac-system-design.md`** is the
authoritative architecture, phasing, and definition-of-done document.
Read it before making any structural decision. It carries the data model
(§4), the feature platform (§4.4), the endpoints (§6), the
credit-deadline phasing (§9), the goals (§10), the settled scope
decisions (§11), and the dataset's known traps (§12).

`docs/STATUS.md` is the authoritative record of where implementation
stands against that plan — current position, what was last verified
green, what is next. It updates **in the same commit as the work it
describes**, never as a follow-up, or it drifts and stops being
trustworthy.

`docs/plans/` holds per-phase implementation plans, written from the
design doc and approved before any code for that phase is written.

Don't duplicate their content here. This file is orientation, not
architecture.

## The one governing principle

**Point-in-time correctness.** Every feature computed for a work item at
time T uses only events with `created_at < T`.

This is where label leakage lives. It is invisible in code review and
impossible to bluff, and it is the entire reason this project exists. If
an implementation choice would let a feature see data from after its own
timestamp — even in a test fixture, even "just for now" — that is a
design bug, not an implementation detail to work around.

The invariant, stated so it can be tested: **a feature vector computed
`as_of` T must be reproducible byte-for-byte from the same Delta version,
a year later.**

## Current status

**Phase 11, a local demo deployed publicly, is merged to `main` via
PR #22 and tagged `v1.2.0`** (annotated, on `4292efb`, 2026-09-11). A
Streamlit app scores against a committed snapshot of the registered
champion — no cloud account needed to run it — live at
[almanac-live.streamlit.app](https://almanac-live.streamlit.app/). Found
by actually running things rather than trusting a test at every stage:
an OpenMP segfault never seen before because scoring had only run on
Databricks (importing `mlflow` before `lightgbm` crashes with no
traceback, 10 of 10 runs); a Docker container that "worked" per an
import-graph test but transitively imported pyspark two first-party hops
down, because that test checked only three files' own direct statements;
the same container measured at 11.1GB, almost all of it an unrelated
Phase 5 dependency the demo never imports, cut to 2.33GB; a mid-build
platform change from Hugging Face Spaces to Streamlit Community Cloud on
cost alone, once Docker Spaces turned out to need a paid personal plan;
and, on a fifth tab added after the deploy work, `AppTest`'s inability to
execute a third-party component at all, a `session_state` key collision
that only broke in a real browser, and a dead-links bug caught only by a
person clicking the actual deployed page. **The reproducibility test
itself was the same shape as the pseudonymity-guard incident below**:
every line of it worked, and it had simply never run anywhere but the
one laptop that also built the artifact it checked, until CI ran this
diff on a different machine and found that 180 of 189 fixture rows tie
exactly on one predicted score with no tiebreak in the rank — the
queue's row order had silently followed Spark's collection order,
stable on that one machine, never guaranteed across others. Fixed with a
deterministic secondary sort key before merging, not after. Full suite
green: **751 passed in 1:14:55**, serial
(the repo's own documented CI fallback — `-n 4` drove this machine's
load average to 156 and then 365, from subprocess-spawned Spark sessions
stacking on top of xdist workers; the Makefile already documented an
earlier load-average-35 failure mode before this one). **The predicted
follow-up risk materialized exactly as flagged**: the live app tracked
`phase-11-local-demo`, this repo's own convention deleted that branch on
merge, and Streamlit Community Cloud's settings UI has no branch field to
retarget an existing app to `main` — only App URL and Python version.
Deleting and recreating the app on the same `almanac-demo` subdomain then
failed at deploy time with a GitHub-access error traced through three
layers: a revoked OAuth grant, a missing GitHub App install, and finally
a stale subdomain reservation from the deleted app that a fresh name
sidestepped. Redeployed at
[almanac-live.streamlit.app](https://almanac-live.streamlit.app/),
tracking `main` directly, and reverified live — node click, real
hyperlinks, all five tabs — before trusting it.

**Phase 9, the agent layer, and Phase 10, its grounding verifier, are both
merged to `main` and tagged together as `v1.1.0`** (annotated, on
`762a330`, 2026-09-10). Phase 9 (PR #20, `6fc99cb`) added four
read-only tools over MCP, a tool gateway and a model gateway in front of
them, and a bounded agent — additive and read-only, so `v1.0` stays tagged
where it was and everything below remains true of what it describes.
**Its paid window found that neither configured model can serve the agent
in this workspace**: every Claude endpoint returns 403 for a
Databricks-set rate limit of 0 while reporting `READY`, and the fallback's
replies do not parse. Llama 3.3 70B answered once, recorded as a
substitute. **Every number in that answer came from a tool, and one claim
did not** — "trained on" a Delta version the tools only read. **Phase 10
(PR #21, `b73834d`) is the fix**: a deterministic verifier that checks not
just that a number is real but that the *relationship* claimed around it
is the one that field actually means, plus a directional check and a
retry-once-then-abstain policy — regex and structural traversal over the
run's own transcript, no model call, so it runs as a normal offline test
rather than a new CI job. The Phase 9 window's cost is read — **$5.59**
in DBUs on 2026-09-10, from `system.billing.usage`
(`docs/findings/2026-09-11-agent-layer-window-cost.md`), and `v1.1.0`
carries both phases.

**The project is complete and tagged `v1.0`.** Every phase is merged to
`main`: Phase 7 via PR #14, Phase 8 via PR #16, and a pre-tag PII fix via
PR #17. **The history was then rewritten** (`git filter-repo`, all 199
commits, three identifier strings stripped), so every SHA written down
before 2026-09-09 no longer resolves — including the ones in this file.
§9's exit gate is measured: **clone to a green run in 4 m 28 s** on a cold
`uv` cache against a 15-minute bar. §10 reconciles to **24 done, 10 partly,
0 not done, 1 not assessable** (`docs/goal-reconciliation.md`).

*(This block read "Phase 8 … unpushed" through the merge of Phase 8 itself,
and `docs/STATUS.md`'s Current position read "Phases 0–6 are complete"
through the whole of Phase 8 — the **sixth and seventh** times a record in
this repo was true when written and quietly stopped being true, after the
README, this section's own two earlier lapses, the §10 tally, and
`streaming.tf`'s Task 9 comments. Seven instances is no longer a run of bad
luck; it is the predictable behaviour of any hand-maintained status line,
and the only thing that has ever caught one is the positive check below —
"what changed today?", never "is it still accurate?", which a stale file
passes trivially. An **eighth** followed almost immediately: this
section's own Phase 9 paragraph read "is complete on branch
`phase-9-agent-layer`, targeting `v1.1.0`" through Phase 9's own merge
(PR #20) and the whole of Phase 10's build and merge (PR #21), until this
same positive check caught it again. A **ninth** on 2026-09-11, and this
one spanned three files at once: `CHANGELOG.md` headed its Phase 9 entry
"targets `v1.1.0`, not yet released", the README said `v1.1.0` "tags both
phases together next", and this very section said "as soon as that tag is
cut" — with `v1.1.0` annotated on `main` since the day before.
`docs/STATUS.md` alone was correct, because it is the one file this repo's
conventions require to move in the same commit as the work. The lesson is
not "check harder"; it is that **the tag is the work**, and the three
reader-facing artifacts were treated as a follow-up to it. The same
positive check caught it, again, and only because someone asked what had
changed.)*

**The last defect found was in the guard against defects of its own kind.**
`pseudonymity.py` exists to fail the build when an actor identifier reaches
a published artifact, and it carried a contributor's machine name in its
own docstring. Its surfaces listed docs, dashboards, terraform and
`.gitignore` but not `src/` or `tests/`, in a repo that had already gone
public. **The check was not broken — every line of it worked, aimed at the
wrong files.** A control with the wrong scope does not fail; it passes,
cleanly and forever, and its green result reads as evidence of absence.
The readiness checklist already carried the item that would have caught it
("confirm it covers every surface that is about to become public"), and the
repo went public with that box unticked: the gap was an unrun control, not
a missing one.

**Phase 8's paid window found four defects that a green 543-test suite could
not**, all of them needing real data through the full stack: Gold silently
dropped **every** reduced-era merge (`merged_true` 0 → 70,624); the serving
endpoint was **live** on the retracted champion rather than merely pinned to
it; a dashboard aged every PR against a horizon **340 days** wrong while
rendering 200 healthy-looking rows; and the quarantine path fired for the
**first time in 341M+ rows** (100 ForkEvents whose `repo` node the 2026
archive ships as `{}`). Training/serving skew is measured at **100.00%
agreement** across 3,459 keys, in a separate centralus stack because
Lakebase is not offered in the main workspace's region.

**Phase 7's most consequential output is a defect it found in Phase 4.**
`train_classifier` split randomly where §4.5 requires temporally and calls
a random split "itself a leakage bug" — in the code that produced the
registered champion. The leakage suite was green throughout and was *not
wrong*: it tests row time, and the bug was on split time. Fixed in Task 15
(`temporal_split`, whole-week boundaries, mutation-tested), and **re-scored
in Phase 8 Task 1** (2026-09-08, run `817800814439176`): **0.612 → 0.4661
PR-AUC**, so the published figure was optimistic by **24%**. The winning
configuration changed too — `is_unbalance` rather than `default` — so a
random split would have shipped the wrong *config*, not just an inflated
score. `skills/almanac-leakage-review` exists because of this.

The full medallion has run on
Q3 2025 — 341,060,851 rows for $11.96, then Gold over that quarter for $0.78.
Phase 6 made the platform live: a poller against GitHub's public Events API,
streaming Silver, two online feature tables published to a Lakebase store and
served from Postgres. Its exit gate was demonstrated across two live windows
(84 served feature values changed, 684 added, none lost) and the billable
stack was torn down at a measured idle rate of $12.06/day — read the day
after, since `system.billing.usage` lags and cannot be queried during the
window it measures. **Every exit-gate row is `[x]`.**

`docs/STATUS.md` holds it at task granularity — deliberately not duplicated
here, because two places recording the same thing means one of them is wrong.
This section has proved that twice now: it read *"implementation not
started"* through all of Phase 0, and then sat on *"Phase 2 … on branch
`phase-2-gold`"* through Phases 3, 4, 5 and most of 6. Neither was ever
*wrong* when written, which is exactly why the "don't let it go stale" rule
does not catch it — see the positive-check rule in Conventions below.

## Phase plan

See §9 of the design doc for the full table and its gates. At a glance:

| Phase | Window | What |
|---|---|---|
| 0 | Wk 1 | Exploration, measurement, real schema diff, Azure provisioned early |
| 1 | Wk 2 | Bronze + Silver, config-driven runner, idempotency proven |
| 2 | Wk 3–4 ⚠️ | Gold + **Azure burn before credits expire 2026-09-24** |
| 3 | Wk 5–6 | Feature platform, as-of joins, leakage suite |
| 4 | Wk 7–8 | Baseline, model, MLflow, serving, drift + skew |
| 5 | Wk 9–10 | Embeddings, vector index as feature infrastructure |
| 6 | Wk 10–11 | Streaming: live poller, late arrival, exactly-once, online store |
| 7 | Wk 12–13 | Governance, lineage, contracts in CI, BI, docs |
| 8 | Wk 13 | Tag `v1.0`. Stop. |
| 9 | After `v1.0` | Agent layer: four read-only tools over MCP, two gateways, a bounded agent — `v1.1.0` |
| 10 | With Phase 9 | Grounding verifier: deterministic checks on the agent's own claims — `v1.1.0` |
| 11 | After `v1.1.0` | Local demo: Streamlit app scoring a committed champion, no cloud account needed, deployed publicly — `v1.2.0` |

## Data engineering patterns — non-negotiable

These are correctness rules, not style preferences. Each has a specific
failure mode behind it.

- **Bronze never transforms.** Payload stays a JSON string; parsing is
  per-type in Silver. A new event type must never be able to break
  ingestion.
- **`ingested_at` is never conflated with `created_at`.** Event time and
  processing time are different clocks. Conflating them is the naming
  trap that silently corrupts every temporal analysis downstream.
- **Every write is idempotent.** `replaceWhere` on partition columns for
  Bronze, `MERGE` for dimensions and accumulating snapshots. "What
  happens if this reruns?" must always have the answer "the same thing."
- **Bad records are quarantined, never dropped.** Failures carry a
  `_failed_rules` **array**, not a boolean, so quarantine is analyzable
  by rule.
- **Wrap every quality-rule condition in `coalesce(cond, False)`.** Under
  three-valued logic a NULL passes neither `== True` nor `== False`, and
  the record vanishes from both the valid set *and* quarantine. This has
  cost people entire datasets. It gets a regression test.
- **Assert the split, never assume it:**
  `valid.count() + quarantine.count() == scored.count()`.
- **Pure transforms are separated from I/O.** `DataFrame in, DataFrame
  out`, no reads or writes inside transformation functions. I/O lives at
  the edges. This is what makes the whole thing testable, and it is the
  single most important structural rule in the repo.
- **Null-safe equality (`<=>`) in every SCD2 comparison.** Plain `<>`
  misses null-to-value transitions, which is exactly what a newly
  populated `language` column looks like.
- **Never quote a number that was not measured.** Not file sizes, not row
  counts, not latencies, not costs. An estimate presented as a
  measurement is the fastest way to lose an interview, and this rule
  applies to the README, the docs, and every commit message.

## AI/ML engineering patterns — non-negotiable

- **A baseline ships before a model.** No model is accepted without a
  measured comparison against a naive baseline. A model that fails to
  beat its baseline is a documented finding, not something to bury.
- **No feature sees the future.** See the governing principle. Every
  feature definition gets a leakage test.
- **Training/serving skew is monitored, not assumed absent.** The same
  feature computed offline and online must agree; where it cannot, the
  divergence is measured and reported.
- **Retrieval is feature infrastructure here, not a chatbot.** The
  vector index lives inside the feature platform. Its success metric is
  *downstream model lift*, never citation groundedness.
- **An LLM never produces a number that a decision depends on.** If a
  natural-language explanation is generated, it is generated *from* the
  model's actual feature contributions — the LLM narrates a computed
  result, it never computes one.
- **Every prediction is traceable to the data that produced it** — model
  version, feature-set version, and the training-data Delta version.
  Reproducibility is a property of the system, not of good intentions.
- **A null result is a finding.** If embeddings produce no measurable
  lift, that gets written down and kept, not quietly deleted.

## Testing policy

**No implementation code is committed without tests, and the full suite
runs before every push** — the whole suite, not just the area that
changed. The point is catching regressions in earlier phases, which is
exactly what a multi-phase project breaks silently.

| Layer | Tooling | What must be covered |
|---|---|---|
| Transforms | `pytest` + `chispa` | Pure functions, table-driven. Null-in-rule-column → quarantined not vanished; duplicate `event_id` → deduped; legacy record → `legacy_v1` handler; unknown type → routed not dropped |
| Ingestion | `pytest` | Missing file vs. empty file vs. failed job are distinguished; rerun produces identical output |
| Dimensional | `pytest` | SCD2 rename → old row closed, new row current, exactly one `is_current`; accumulating snapshot → out-of-order arrival preserves earliest timestamp |
| **Feature platform** | `pytest` | **Leakage suite: no feature reads data at or after its own `as_of`. Same `as_of` → byte-identical vector on recompute** |
| Model | `pytest` + MLflow | Beats baseline by a measured margin; serving output matches offline scoring on the same input |
| Streaming | `pytest` | Late arrival, duplicates, watermark advancement |
| Contracts | CI | The data contract fails the build, not a document |

Use a session-scoped `SparkSession` fixture in `conftest.py`.

## Language and tooling

Python 3.12+ throughout. Current-generation tooling only: `uv`, `ruff`,
`mypy --strict`, `pytest`, Pydantic v2. PySpark for distributed compute,
`delta-spark` for the table format, dbt scoped to Gold only.

Local development is a **single container** running `pyspark` +
`delta-spark` with `.master("local[*]")`. Not a Spark master/worker
Compose cluster — at this data volume it is slower than local mode and
costs days on executor OOMs that teach nothing.

## Cost discipline

The Azure spend is real money on a deadline. Rules:

- **Job clusters only.** Never an all-purpose cluster on a schedule —
  the single most common way a portfolio project generates a surprise
  bill.
- **Auto-termination on anything interactive.**
- **`terraform destroy` between working sessions.** The apply/destroy
  cycle is a cost control that happens to also be a demo.
- **Every resource tagged** `project`, `env`, `owner`.
- **Budget alert configured before the first apply**, not after.
- Cost per run is measured and reported, like every other number.

## Development workflow

```
new subsystem?
├── yes → brainstorm → dated doc in docs/design/ → approval
│         → implementation plan in docs/plans/ → then code
└── no  → is there an approved plan task for it?
          ├── yes → TDD: failing test → implement → full suite green
          │         → STATUS.md row in the SAME commit → one commit per task
          └── no  → stop and ask; don't freelance scope

before every push:  full test suite + lint, both green
after closing out a debugging saga or making a real decision:
                    → add it to the interview story bank gist (see below)
```

## Conventions

- **One branch per phase, named for the phase's subject, carrying the
  whole phase** — its plan doc *and* every task — pushed once at the end
  as a single PR. `phase-6-streaming` is the model: 18 commits, one push,
  PR #13. **Never commit phase work to `main` directly.** Written down
  2026-09-07 after both halves went wrong in one go: Phase 7's branch was
  first named `phase-7-plan`, copying the older split-branch habit from
  PRs #11–12 that Phase 6 had already superseded, and its planning commit
  landed on `main` before being moved. Neither was recoverable from this
  file, because neither was in it.
- **One commit per completed task**, not one bundled commit per phase.
  Each carries its own code, its own tests, and its own green suite.
- **`docs/STATUS.md` updates in the same commit as the work.**
- **The README's architecture diagram updates in the same commit as any
  change to what it depicts.** A diagram nobody keeps current is worse
  than no diagram, because it misleads instead of being silent.
- **Changes stay scoped to the task.** Don't reformat or "improve"
  adjacent code while touching a file for something else. If you notice
  pre-existing dead code, say so rather than deleting it silently.
- Conventional commit prefixes (`feat:`, `fix:`, `docs:`, `test:`,
  `refactor:`).
- **Commit messages are plain ASCII: `--`, never an em-dash.** Docs and
  the README use `—` freely and should keep doing so; the git log does
  not. Written down 2026-09-08 because it had held for **six consecutive
  commits purely as pattern-matching against the previous message**, and
  broke on the seventh (`6e1c5eb`) the moment nothing was there to match.
  A convention that lives only in practice is a habit, and a habit does
  not survive a context boundary — the same failure this file already
  records for the README and for "Current status". **Earlier commits are
  left as they are**; this applies going forward, so `6e1c5eb` stays the
  one that names the rule by breaking it.
- The README never claims something is built when it is not, and never
  carries an unmeasured number.
- **The three reader-facing artifacts get refreshed proactively, not on
  request: `README.md`, `CLAUDE.md`, and the story-bank gist.** Standing
  instruction from the user, 2026-09-01, and it is incident-backed rather
  than precautionary: the README read *"design approved, implementation
  not started — nothing below is built yet"* through **all nine Phase 0
  tasks**, a green CI pipeline, 69 tests, and live cloud infrastructure.
  The two rules above it were already in force and did not catch it,
  because each is phrased as *don't let it become wrong* — and the README
  never became wrong, it simply stopped being updated while the project
  moved.

  So the check is positive, not negative. At every natural stopping point
  — a task done, a STATUS.md row, a phase boundary — ask of each of the
  three: **what changed today that a reader of this file would want to
  know?** Not "is it still accurate?", which a stale file passes trivially.

  | Artifact | Refresh when |
  |---|---|
  | `README.md` | status changes, a phase completes, the architecture diagram's subject changes, a finding lands that a reader would want up front |
  | `CLAUDE.md` | a convention is set, a skill/hook is added or deferred, a policy changes |
  | story-bank gist | anything went wrong, was measured, or was decided against a real trade-off — the raw material for a STAR answer |

## Interview story bank

A **secret** GitHub Gist holds the STAR-format story bank for this
project — real scenarios, what broke, what was tried, what was measured.
It is deliberately **not** in this repo: deleting a file in a later
commit still leaves it recoverable from git history once the repo is
public, which defeats the point.

The gist ID lives in `.claude/story-bank-gist-id` (gitignored).

**Trigger discipline, learned the hard way on a prior project:** the
"remember to update the story bank" trigger has a documented history of
never firing on its own — it needed a direct prompt every single time
across many sessions. Treat it as unreliable by default. Check in on
gist-worthiness at natural stopping points — end of a task, a STATUS
update, a commit — rather than waiting to remember.

## `.claude/` tooling — added only for real, already-settled things

**Nothing here is anticipatory.** Tooling gets written when a real,
specific, repeatable lesson has already cost something — not for a
problem this repo might have someday. Anticipatory skills are worse than
no skills, because they train the habit of ignoring them.

Currently present:

- **`hooks/story-bank-reminder.sh`** — fires on commit, non-blocking.
  Purely incident-derived: on a prior project this trigger failed to
  self-fire on every single occasion over roughly a dozen sessions, and
  the story bank only ever got updated when asked directly. A mechanical
  reminder is the fix; care demonstrably was not.

- **`skills/almanac-design-decision`** — evidence sufficiency, live
  validation, and where a decision gets recorded. Incident-derived, and
  the incidents are the *same mistake twice*: the 2026 volume claim
  generalized from **one** truncated hour (wrong; corrected in `535cbeb`),
  and the "VM SKUs are restricted subscription-wide" claim generalized
  from **two** regions (wrong; centralus and westus3 offer the SKU, and
  the error nearly triggered a serverless rewrite of design doc §8.1).
  Both were ~90 seconds of extra measurement from being caught, and both
  were stated as conclusions before that measurement ran. **A third landed
  2026-09-09**: "all 375 commits authored by the GitHub noreply address"
  was written into a STATUS row, a checklist, a PR body and the story bank.
  The repo has **199** commits; 375 was the count of identity *fields*
  (`%ae` plus `%ce`) across all refs. The conclusion was right and the
  number counting it was not — gate 1 is *state what the `n` is actually
  over*, and this was an `n` over the wrong unit entirely. Caught while
  re-measuring before the history rewrite, and corrected in place. Its gate 1
  ("state the actual `n`, along the dimension you are generalizing over")
  is the part that addresses the failure; gates 2 and 3 codify the
  already-standing web-validation and where-it-gets-written rules rather
  than adding anything new.

- **`skills/almanac-leakage-review`** — **the trigger fired.** It sat on
  the deferred list below with an explicit condition — "write it the first
  time a leakage bug actually gets through" — and on **2026-09-08** one had:
  `train_classifier` used `train_test_split(random_state=42)` where §4.5
  requires a temporal split and calls a random one "itself a leakage bug",
  in the code that produced the registered champion. **The leakage suite
  was green throughout and was not wrong** — it tests that each row's
  features precede that row's own `as_of`, which held. It tested one axis;
  the bug was on another. The skill's gate 1 is that table of axes — row
  time, split time, entity overlap, label construction, target definition
  — and which of them anything actually covers. Gates 2–4 are
  incident-derived too: mutation-test the new test (`temporal_split` was
  broken three ways, `canonicalize` five of seven), ask whether the fixture
  can even express the failure (the model frames carried **no**
  `as_of_timestamp`; the streaming fixture's late rows were **all**
  duplicates), and treat a justification written into an assertion message
  as a claim that can be wrong — the watermark bug survived review because
  its test explained why the drop was fine.

- **`skills/almanac-code-style`** + **`hooks/code-style-reminder.sh`** —
  **the one deliberate exception to "nothing here is anticipatory."**
  Written 2026-09-02 at the user's direct request, not from a named
  Almanac review incident — unlike its sibling below (still deferred),
  this one has no incident behind it and says so in its own frontmatter
  rather than being dressed up as if it did. The skill is a
  simplify/name/edge-case/dedup/composition/mapping-dispatch/generator/
  context-manager/comment-discipline checklist applied while writing code;
  the hook is a mechanical `git commit` reminder in the same register as
  `story-bank-reminder.sh` — it cannot judge code quality, only remind
  that the judgment should have happened. **Comment discipline** (item 10,
  added 2026-09-02): one-line docstrings, why-not-what comments, no
  narrative paragraphs or measured-number essays in code — that story
  lives in `docs/`. The Phase 0–1 modules were swept against it once in a
  dedicated pass (2026-09-02); it applies to new code from there on.

- **`skills/almanac-paid-window`** — **incident-derived, and the incident is
  a single night.** Written 2026-09-08 after a paid window in which: a
  targeted apply hung **44 minutes** and was written up as a *vendor outage*
  when `docs/STATUS.md` had said for two days that Lakebase is not offered in
  that region; dashboard screenshots were about to be captured from a panel
  that rendered **200 healthy-looking rows** against a horizon 340 days
  wrong; and a teardown verified once reported two dashboards still `ACTIVE`
  that a re-read showed were propagation lag. Five gates: read the record
  before spending, pre-flight offline, capture perishable evidence **before**
  teardown *and verify what you are about to capture*, verify teardown
  independently **then again**, and do not kill a hung create blindly. The
  mechanisable half is `almanac.infra.lakebase_window`, whose `check_region`
  refuses an unsupported region before terraform is invoked — its message
  names the *symptom*, because an unsupported region does not fail cleanly,
  it hangs and looks exactly like an outage.
  **Two gate bullets added 2026-09-10**, both from Phase 9's window: gate 2
  now checks the cluster's library set and not only the wheel (run 1 died at
  import on `lightgbm` with the wheel hash-verified identical), and gate 3
  now requires writing evidence as it is produced (run 2 lost ten minutes of
  tool evidence to a crash at the end; run 3 kept them).

Deliberately deferred until earned, with the trigger that would justify
each:

*(**`skills/almanac-leakage-review` was on this list and has been written**
— see above. Its trigger fired 2026-09-08, in Phase 4's model layer rather
than Phase 3 as predicted.)*
- **A cost-guard hook on `terraform apply`** — write it if a session
  actually ends with resources left running. The discipline is stated
  above; mechanize it once it is proven that stating it was not enough.
- **A recurring-CI-failure procedure** — there is no CI yet and no
  failure history to generalize from.
- **An incident-derived code-quality review skill**, distinct from
  `almanac-code-style` above — write it once Almanac has accumulated its
  own named review incidents (a real bug a generic style pass wouldn't
  have caught, the way `canopica-code-review`'s six do for its sibling
  project). `almanac-code-style` covers general simplification and
  readability; this one would cover Almanac-specific pattern mistakes,
  and Almanac currently has zero of those to ground it in.
- **A test-coverage skill** — deliberately *not* planned. Coverage is
  already enforced mechanically by CI and the testing policy above. A
  skill would be a second place recording the same rule, and two places
  recording one rule means one of them is wrong.

## graphify

This project builds a local knowledge graph under `graphify-out/` — god nodes, community structure, cross-file relationships. It is **gitignored**: the post-commit hook rebuilds it on every commit, so a tracked copy would be perpetually dirty and its curated community labels would decay. On a fresh clone, run `/graphify` once to build it.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
- `graphify hook install` wires this into `git commit`/`git checkout` directly (AST-only rebuild, no API cost) — one-time per clone, since git never versions `.git/hooks/`.
