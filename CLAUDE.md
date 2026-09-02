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

**Phase 0 and Phase 1 complete and merged to `main`; Phase 2 in
progress — 4 of 9 tasks, on branch `phase-2-gold`.** `docs/STATUS.md` holds it at
task granularity — deliberately not duplicated here, because two places
recording the same thing means one of them is wrong, and this section
proved that the hard way: it read *"implementation not started"* through
all of Phase 0.

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
| 6 | Wk 10–11 | Streaming: watermarks, late arrival, exactly-once |
| 7 | Wk 12–13 | Governance, lineage, contracts in CI, BI, docs |
| 8 | Wk 13 | Tag `v1.0`. Stop. |

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
  were stated as conclusions before that measurement ran. Its gate 1
  ("state the actual `n`, along the dimension you are generalizing over")
  is the part that addresses the failure; gates 2 and 3 codify the
  already-standing web-validation and where-it-gets-written rules rather
  than adding anything new.

- **`skills/almanac-code-style`** + **`hooks/code-style-reminder.sh`** —
  **the one deliberate exception to "nothing here is anticipatory."**
  Written 2026-09-02 at the user's direct request, not from a named
  Almanac review incident — unlike its sibling below (still deferred),
  this one has no incident behind it and says so in its own frontmatter
  rather than being dressed up as if it did. The skill is a
  simplify/name/edge-case/dedup/composition/mapping-dispatch/generator/
  context-manager checklist applied while writing code; the hook is a
  mechanical `git commit` reminder in the same register as
  `story-bank-reminder.sh` — it cannot judge code quality, only remind
  that the judgment should have happened.

Deliberately deferred until earned, with the trigger that would justify
each:

- **A point-in-time / leakage review skill** — write it the first time a
  leakage bug actually gets through, which will most likely be in Phase
  3. Writing it now would be guessing at what the bug looks like.
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
