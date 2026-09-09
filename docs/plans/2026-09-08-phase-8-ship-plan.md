# Phase 8 — re-score, a chosen window, drift, and ship

Written from design doc §4.8, which records the six decisions this plan
executes. §9 scopes Phase 8 as "Tag `v1.0`. Stop." — and that still
governs. **Nothing here is a new subsystem.** Every task is either an
integrity debt the project already owes, or scope §9 assigned to an
earlier phase and never delivered.

The public flip is deliberately **not** in this plan. It is a wrap-up
checklist over decisions already made, and it lives in
`docs/plans/2026-09-08-public-repo-readiness-checklist.md`. It runs after
this plan closes.

**One commit per task, `docs/STATUS.md` updated in the same commit,
`make check-fast` per task and the full `make check` before the push.**

---

## Sequencing, and why

Tasks 1–6 are local and cost nothing. Task 7 is the only billable step
and it is a single window, so everything that could possibly be verified
offline is verified before it opens — the discipline Phase 7 Task 10
proved, when pre-flight caught four dead table references that would
otherwise have been found inside the paid window.

**Task 4 runs before Task 1. Corrected 2026-09-08, before any code was
written to this plan.** As first written, Task 1 re-scored the champion and
Task 4 then fixed the PR-opened spine fan-out -- but the re-score goes
through `assemble_training_set`, the same builder the fan-out lives in, so
that order re-scores on data still carrying 25 PRs at 4x weight and ships
the new number with a caveat attached. The original draft half-noticed this
and hedged ("say which order was used"), which is a note where a fix
belongs. Fixed order below; task numbers are left alone so the STATUS log
and this document keep referring to the same things.

Task 2 is unaffected either way: `action='merged'` is reduced-era only, and
the training population is Q3 2025, which is rich-era.

**Order: 4, then 2, 3, 5, 6 (all local and free), then 1 and 7 (both
billable, both after Phase 7 merges).** Task 1 is the one number the project
currently publishes knowing it is wrong, so it goes first among the
billable work.

---

## Task 1 — Re-score the champion on the temporal split

**Why.** Phase 7 Task 15 fixed `train_classifier`'s random split. The
registered champion was trained through the old one, so the published
**0.612 PR-AUC is the random-split number**, optimistic by an unmeasured
margin, and it is quoted in five reader-facing places.

**Do.** Re-run the classification training job over the same Q3 2025
population through `temporal_split`. Same features, same candidate sweep,
same baseline comparison — the split is the only variable.

**Publish whatever it produces, including a drop.** §5.1's null-result
discipline applies to a number getting worse under a correct method
exactly as it applied to the regression model failing its baseline. If it
now fails the baseline gate, that is the finding and the champion is not
re-registered.

**Done when.** The temporal-split number is measured, recorded in
`docs/findings/`, and propagated to README, `CLAUDE.md`, `docs/STATUS.md`,
`docs/limitations.md`, `docs/decision-memo.md`. The old number is
**corrected in place and marked**, never silently replaced — the
design-decision skill's rule for a recorded claim that turned out wrong.

**Cost.** One job run. Prior comparable runs: 12 min, ~$1.

---

## Task 2 — `action='merged'` in the reduced-era parser

**Why.** §4.8 decision 3. The reduced era replaced
`payload.pull_request.merged` with a distinct `action='merged'`, measured
present in every 2026 hour sampled (285/457/283) and absent from the 2025
hour. `payloads.py:150` reads the field, so `pr_merged` is NULL across the
entire reduced era while the information sits unread in `payload.action`.

**Do.** TDD. A failing test first: a reduced-era `PullRequestEvent` with
`action='merged'` and no `pull_request.merged` must yield
`pr_merged = true`; `action='closed'` must yield `false`; the rich era
must keep reading the field and be unaffected.

**Watch out.** Rich-era events have no `action='merged'` at all, so this
is era-conditional, not a blanket `coalesce`. Do not let it fold a
genuine unknown into `false` — the era-bound-null rule stands.

**Done when.** Green, and a re-parse of one measured 2026 hour shows
`pr_merged` populated where it was NULL.

---

## Task 3 — Correct the −97% claim in place

**Why.** `docs/findings/2026-09-02-second-source.md` states 2026 carries
PR events at "−97% against 2025's ~6,618 PRs opened/hour → ~200 opened
PRs/hour on current data". It was derived from **one hour**. Measured
2026-09-08 across **n=5 hours over 4 days**, one recent hour carried
**10,544** opened PRs — *more* than the 2025 hour's 6,702.

**Do.** Correct in place, marked and dated, keeping the original
reasoning visible. State the new `n` in the claim itself. Say which
downstream numbers rested on it — including the REST enrichment sizing
("~4,800 requests, ~1 hr per 2026 day"), which is wrong in the same
direction.

**Do not call this a third `n=1` incident.** Checked 2026-09-08: the
origin finding `2026-09-01-third-schema-era.md` measured **n=5** 2026
hours at 120–265 opened PRs/hour and warned in its own text that "any
single hour is a bad basis for a claim." It is sound for the window it
sampled. What broke is the *extrapolation* — a June–August observation
re-quoted as "on current data". The quantity is **not stationary**, so no
`n` within one period would have caught it. That is a distinct failure
from the two `n=1` incidents, and the correction should say so rather than
flatten it into the familiar story.

---

## Task 4 — The PR-opened spine fan-out

**Why.** Phase 7 Task 10 measured the predictions table at 7,320,196 rows
against a distinct key count of exactly 7,320,121 — 25 PRs at four rows
each, because GH Archive carries two distinct `opened` events for them and
both `build_pr_opened_spine` and `compute_pr_static` emit per *event*
rather than per *PR*. Written up in
`docs/findings/2026-09-08-pr-opened-spine-fanout.md` and deliberately left
unfixed there.

**Do.** TDD, failing test first: two `opened` events for one PR must
produce exactly one spine row, and the earliest `created_at` wins.

**Not a leakage bug** and the fix must not pretend it was — every
duplicate's features already precede its own `as_of`. It is a violation
of "one row per entity", which is a different guarantee.

**Note for Task 1.** If Task 1 runs before this, the re-scored number
carries the 4× weighting on 25 PRs (0.001%, below any metric's
resolution). Say which order was used rather than leaving a reader to
guess.

---

## Task 5 — Window characterization, and the refusal

**Why.** §4.8 decision 2. This is the capability that makes "run it
against any window" true instead of lucky.

**Do.** A command that takes a candidate window, reads its hourly files,
and reports composition — event-type shares, opened-PR count, whether
`pull_request.draft`/`merged` are present, schema era, bot share — then
**refuses** a window that cannot support the pipeline, naming which
threshold failed.

**Thresholds are derived from measurement, not invented.** The 2025
reference hour and the five 2026 hours already measured are the
calibration set; state the `n` on every threshold.

**The refusal is the feature.** A characterizer that only describes is a
report; one that refuses is a gate. Mutation-test it: a degraded window
must actually fail, watched.

**Done when.** Green, and running it over 2026-09-02 (1.5 MB, degraded)
refuses while 2026-09-05 (79.3 MB, 37.3% PR share) passes.

---

## Task 6 — Feature and data drift, offline

**Why.** §10 line 1718 asks for it, line 1911 maps it to the MLOps claim,
and line 1922's resume bullet already asserts it. Unfinished scope the
project advertises.

**Do.** Compare the Q3 2025 training distribution against the chosen
window: per-feature distribution shift, null-rate shift (this is where the
schema break shows up as `is_draft` going wholly unknown), label-rate
shift, and event-composition shift.

**Local and offline.** No cloud, no billable window.

**The alarm must have a stated response.** A champion trained on
`is_draft` cannot score a window where the field does not exist; the
correct action is to refuse to serve, not to re-baseline. Write that down
next to the threshold, or this is a chart nobody acts on.

**Done when.** Green, and the drift report over the chosen window fires on
all three kinds — covariate, schema, semantic — with the 2025-vs-2026
numbers stated.

---

## Task 7 — The demo window (the phase's only billable step)

**Why.** The full-stack demo deferred out of Phase 7 (§4.7), plus the two
things only a live window can produce.

**Pre-flight, before a single resource comes up.** Everything Phase 7
Task 10's pre-flight caught, plus: the chosen window passes Task 5's
characterizer; every dashboard query still resolves against the schema;
`make window-up`'s plan reaches no further than its declared targets.

**In the window.**
1. Run the medallion — Bronze → Silver → Gold — over the **chosen**
   window, alongside the existing Q3 2025 Gold rather than replacing it.
2. Refresh the three dashboards against it and capture evidence.
3. **Training/serving skew**, the online half of §4.8 decision 5 — the
   same feature computed offline and online, compared, divergence
   measured and reported rather than assumed absent.
4. **Phase 6 console evidence** — §4.7 records this as the only remaining
   opportunity, and it must be captured against a *fresh* instance, not
   the one that produced the original measurements.
5. The two dashboard panels currently shipping marked *unavailable*
   become real, or stay marked with the reason restated.

**Tear down and verify four ways** — state list, warehouses, lakeview,
clusters — never from the destroy's exit code. Phase 7 Task 10's
`terraform`-does-not-lock-a-dashboard finding applies: close the browser
editor before applying.

**Capture everything perishable first.** The billing tables lag ~24h and
cannot be read during the window that generates them, so the idle rate is
read the day after or reported as unmeasured.

---

## Task 8 — Close out

- §10 reconciled a final time, item by item, against artifacts rather than
  memory. Two items are expected to move: drift, and the temporal split.
- **Resume bullets** — §10's other remaining *not done*. Line 1922's
  template asserts drift and skew monitoring; it may only claim what
  Tasks 6–7 actually delivered.
- README, `CLAUDE.md`, and the story-bank gist refreshed against the
  positive check — *what changed that a reader would want to know* — not
  the negative one.
- Architecture diagram updated in the same commit as anything it depicts.
- **Tag `v1.0`** (§4.8 decision 6), after the window, before the flip.

---

## Exit gate

- [ ] The champion's temporal-split score is measured and published,
      whatever it is, and the old number corrected in place
- [ ] `pr_merged` populated for the reduced era, from `action`, TDD
- [ ] The −97% claim corrected in place, with its new `n`, naming the
      repeated failure mode
- [ ] The spine fan-out fixed, with the failing test that proves it
- [ ] A degraded window is **refused**, watched failing
- [ ] Drift reported across all three kinds, with a stated response
- [ ] The medallion has run end to end over a window chosen this phase
- [ ] Training/serving skew measured, not assumed absent
- [ ] Phase 6 console evidence captured against a fresh instance
- [ ] Everything billable torn down, verified four independent ways
- [ ] `v1.0` tagged
- [ ] Full `make check` green before the push

---

## Deferred out of Phase 8, on purpose

- **REST enrichment for `draft`** — §4.8 decision 4. Deferred for
  leakage, not effort, with the upgrade path named (timeline API, or a
  distinctly-named current-state column barred from `as_of` joins).
- **Streaming through Gold** — the live path still stops at Silver and the
  online store. Task 7 fills Gold from the archive, which is the complete
  source; the streaming path keeps proving streaming semantics. Stated so
  the README diagram's edge stays honest.
- **Retraining on a recent window** — blocked by the same schema break
  that makes the drift story, and by labels needing ~21h to resolve
  (p90 first response 76,533s). The recent window is **scored, not
  trained on**.

---

## Task 9 — One command for the Lakebase window, and the region guard that would have saved 44 minutes

**Added 2026-09-08, after Task 7's window, at the user's request.** Not
anticipatory: the trigger fired the same night, and CLAUDE.md's rule for
`.claude/` tooling is that it gets written once a specific lesson has
already cost something.

**Why.** `almanac.infra.window` gives one guarded command up and down for
the **westus3 reporting** window. The **centralus Lakebase** stack has a
README and nothing else, so Task 7 hand-ran roughly ten steps — two-stage
apply, deploy wheel/script/config, create the secret scope, run the job,
capture evidence, targeted `force_destroy` apply, destroy, verify — with
four traps that exist only because Phase 6 hit them. Prose that must be
followed exactly, in order, with money running, is a script.

**Not an LLM agent.** Infrastructure lifecycle needs determinism, and the
failure mode of an agent doing teardown is one that *believes* it
destroyed something. Every teardown check that mattered on 2026-09-08 was
a hard assertion; the one soft judgement made that night — "this is a
vendor outage" — was wrong.

**Do.**

1. **`check_region` first, because it is the incident.** A targeted apply
   of `databricks_database_instance` from the westus3 module hung **44
   minutes**: Lakebase names 19 supported regions and `westus3` is not
   among them. The guard refuses *before* terraform is invoked, and its
   message names the symptom — a hang plus "temporarily unavailable"
   against a healthy control API reads exactly like an outage.
2. **Two-stage `up`.** `terraform apply` cannot run cold here: the
   `databricks` provider is configured from a workspace URL that does not
   exist until the workspace does, so the data sources fail to resolve on
   a first plan. Stage 1 is the Azure layer, stage 2 the rest.
3. **Guarded `down`.** Reuse `window.py`'s existing pure checks rather
   than restating them — `check_sync_plan` already generalises the
   `force_destroy` trap, `check_destroy_plan` already refuses a destroy
   that creates, and `survivors` already refuses to trust an exit code.
   The destroy **count** is reported before it is applied.

**Reuse, do not duplicate.** The guards are pure functions over
`PlannedChange` and already take their targets as a parameter. A second
copy of them is the same-fact-in-two-places failure this repo has now
recorded four times.

**Done when.** TDD, RED watched, mutation-tested. `check_region` refuses
`westus3` and admits `centralus`, proven by test. The full suite green.

**Also in this task.**

- **`.claude/skills/almanac-paid-window`** — the judgement a guard cannot
  carry: read `docs/STATUS.md` before provisioning (the 44 minutes),
  capture perishable evidence *before* teardown (billing lags ~24h and the
  workspace id is unrecoverable after), and verify teardown independently
  **then re-verify**, since the first sweep on 2026-09-08 showed two
  dashboards still `ACTIVE` that were propagation lag.
- **Story-bank entry** for the night, in the gist. The runbook stays in
  the repo: a procedure in the gist would be the same duplication the
  guard rule above exists to prevent.
