# Phase 10 — the grounding verifier, and the schema fix the window's false claim demands

**Approved 2026-09-10.** Phase 9 merged to `main` untagged the same day
(`6fc99cb`, PR #20). Task work starts at Task 1.

Written from `docs/design/2026-09-09-almanac-agent-layer-design.md` §6,
which lists seven tasks. This plan executes them and adds two the design
did not have, both earned by Phase 9's paid window
(`docs/findings/2026-09-10-agent-layer-window.md`):

- **A schema fix (Task 1).** The window's one false claim — "trained on
  Delta versions 92" when the champion trained on v91 — formed because
  `ModelProvenance.delta_versions` does not say *what it is versions of*.
  The verifier is the enforcement; the rename is the fix for the cause.
- **A relationship verifier as its own task (Task 4).** Design §6 Task 1
  is "every numeric literal appears in that run's tool results." The
  window proved that is not enough: **every number in the false answer
  did appear in a tool result.** What was wrong was the relationship
  claimed *around* the number — "trained on", when the tool returned the
  version it *read*. Checking the claim type against the field it must
  trace to is a distinct check, not a stricter version of the first.

**This is Phase 2 of `v1.1.0`, not a release of its own.** Phase 9 merges
to `main` untagged; `v1.1.0` is tagged once this phase also lands (design
header, **Scope:** Phases 9 and 10). Nothing here changes the medallion,
the feature platform, the champion, the streaming path, the contracts, or
the four tools' behaviour — the verifier reads tool output, it does not
alter it. The one exception is Task 1's rename, which touches
`schemas.py` and four construction sites in `tools.py` and is a pure
rename with no behavioural change.

Branch `phase-10-grounding-verifier`, carrying the whole phase, one push,
one PR. It already holds two commits made while Phase 9 was in review —
`6a0d4bd` (graphify tooling) and `88cf156` (a stale-ADR-reference fix
graphify surfaced) — neither of them Phase 10 work; the task commits
begin at Task 1. **Nine build tasks and a close-out (Task 10).** **One
commit per task, `docs/STATUS.md` updated in the same commit,
`make check-fast` per task and the full `make check` before the push.**

**This phase's close-out closes the project.** Task 10 is not the usual
three-artifact refresh — it is the `v1.1.0` release: the story-bank gist,
the tag on `main`, the README and its architecture diagram, the GitHub
profile, the goal / resume / limitations docs, and a first-time-reader
narrative in the README (why the project, what it is for, key takeaways
and roadmap). Spelled out as its own task below because a release step
that lives only in "we'll remember at the end" is this repo's
most-recorded failure mode.

---

## Sequencing, and why

**Everything offline.** Phase 10 spends nothing. The verifier runs
against the transcripts Phase 9 recorded and replays for free; the golden
set is fixtures. There is no paid window in this phase and none is
needed — which is the point of Phase 9 having recorded transcripts rather
than leaving the loop to bill on every iteration.

**Order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 (close-out, after the PR merges).**

- **Task 1 (schema rename) is first** because Tasks 3 and 4 read the tool
  returns it renames, and a plan that built the verifier against
  `delta_versions` and then renamed the field would have written the
  wrong field name into the verifier's own rules.
- **Task 2 (per-run trace) is second, not last.** Design §6 lists traces
  as Task 7. But every check from Task 3 on produces trace rows —
  "this number matched `predict.probability`", "this claim found no
  field" — and building the trace structure first means each check
  appends to it rather than being retrofitted. Task 9 formats and
  persists the trace; Task 2 defines it.
- **Tasks 3 and 4 are the verifier core.** Numeric-literal grounding,
  then relationship grounding. 3 before 4 because 4's failure message
  ("the number is real, the claim about it is not") only makes sense once
  3 has already confirmed the number is real.
- **Task 5 (retry-then-abstain)** consumes the verifier, so it comes
  after the verifier answers.
- **Task 6 (golden set)** needs the verifier to score its fixtures, and
  needs Task 5's `Ungrounded` outcome to assert against for the known-bad
  case.
- **Task 7 (directional assertions)** extends the verifier with a third
  check; it is separable from 3 and 4 and depends on neither, but lands
  after the golden set so its first fixture is already in place.
- **Task 8 (mutation testing)** needs the whole verifier present to
  mutate. Design §6 says it "is not optional"; it gets its own task, not
  a bullet inside another.
- **Task 9 (CI gate + trace persistence)** is the last *build* task: it
  wires the green/red decision and the per-run trace file into
  `make check` and `ci.yml`, and needs everything above it to exist.
- **Task 10 (close-out) runs after the Phase 10 PR is merged**, because
  the `v1.1.0` tag lands on `main` — the way `v1.0` did — not on the
  branch. Its one hard gate is external: the Phase 9 window's cost read
  (`system.billing.usage`, on or after 2026-09-11) must be recorded or
  explicitly marked uncloseable before the tag.

---

## Module layout

New code is one module plus fixtures. The verifier is deliberately not
spread across the codebase — it is one function with one report type, the
way `contracts.py` is one enforcement point.

| Module | Holds |
|---|---|
| `src/almanac/agent/grounding.py` | the verifier: `verify(answer, tool_calls) -> GroundingReport`, its numeric / relationship / directional checks, and the rounding rule |
| `src/almanac/agent/schemas.py` | Task 1's rename; new `GroundingReport`, `GroundingTrace`, `Ungrounded` types |
| `src/almanac/agent/bounded_agent.py` | Task 5's retry-once-then-abstain; the new `Ungrounded` terminal outcome |
| `tests/fixtures/golden/*.json` | golden-set questions with expected tool-call sequences and expected outcomes |
| `tests/fixtures/transcripts/2026-09-10-live-predict-explain.json` | already committed by Phase 9; Phase 10's first known-bad fixture |
| `.github/workflows/ci.yml` | Task 9's gate step (offline, replays fixtures) |
| `README.md`, `CLAUDE.md`, `docs/goal-reconciliation.md`, `docs/resume-bullets.md`, `docs/limitations.md` | Task 10's close-out edits |

No new optional-dependency group. The verifier is stdlib plus Pydantic —
number extraction is a regex over the answer text, not an LLM call. That
is the design's central choice (§6, "Deterministic, not LLM-as-judge")
and it is what lets the check be a build gate rather than a probability.

---

## Task 1 — The provenance schema says what it is versions *of*

**Cause, not caller.** `ModelProvenance.delta_versions` and
`FeatureProvenance.delta_versions` both carry the Delta versions the tool
*read*. `VersionsResult.training_data_delta_versions` — distinctly named —
carries what the champion *trained on*. In the window, the agent never
called `versions`, saw only `{"model_version": "2", "delta_versions":
{...}}`, and wrote "trained on Delta versions 92". The field name invited
the reading.

Rename both to `read_delta_versions`. Four construction sites in
`tools.py` (lines ~117, ~157, ~209, and the `FeatureProvenance` at ~117),
plus `schemas.py`, plus every test asserting on the key. Update the
one-line docstrings so they name the distinction rather than bury it in
prose that does not travel in a tool result.

This is a pure rename: no field added or removed, no value changed, no
tool behaviour different. It is Task 1 because it is a prerequisite for
writing Task 4's rules against the right names.

**Test:** the existing tool tests re-pass with the new key; a new
assertion that `GetFeaturesResult` / `ExplainResult` serialize with
`read_delta_versions` and no `delta_versions`. Mutation: revert the
rename in `schemas.py` only and confirm `tools.py` fails to construct.

**STATUS.md:** the rename, and why — one row in the verification log
citing the window's `n = 1` false claim.

---

## Task 2 — The per-run grounding trace

Define the trace before the checks that fill it. A `GroundingTrace` is
the record of what the verifier examined:

- every numeric literal extracted from the answer, with its source
  decision (matched tool call + field + path, matched-after-rounding, or
  unmatched);
- every claim the relationship check parsed, with the field it was
  required to trace to and whether it did;
- every directional assertion and the contribution sign it was checked
  against;
- the final verdict and, if `Ungrounded`, the specific rows that failed.

It is structured data, serializable, and one per agent run. It is not
prose and it is not a log line — it is the artifact a reviewer reads to
see *why* a build went red, and the design's Task 7 ("per-run traces")
is its persistence, handled in Task 9.

**Test:** a trace round-trips through Pydantic; a hand-built answer with
one good and one bad number produces a trace with exactly those two rows.

---

## Task 3 — Numeric-literal grounding, with a rounding rule

Every number in the answer must appear in that run's tool results. The
window's answer is the worked example: `0.003826` is
`predict`'s `0.0038260014552166737`, and **rounding is a transformation**
the verifier has to allow for without allowing everything.

The rule, stated so it can be tested:

- A literal `x` in the answer is grounded by a tool value `v` if `x == v`
  exactly, **or** if `v` rounds to `x` at the number of significant
  figures `x` is written to (`round(v, sig=len(x))`), **or** if `x` is
  `v` truncated at a decimal place. No tolerance band beyond
  representational rounding — `0.0038` grounds `0.003826`, `0.004` does
  not ground `0.0038`.
- Integers match exactly. A version number, a `top_k`, a `pr_number`
  never rounds.
- Numbers inside a quoted feature name (`prior_pr_count`) are not
  literals.

Numbers are extracted by regex over the answer text. Tool values are
walked from the typed tool-call results recorded in the transcript —
every leaf of every `GetFeaturesResult`, `ExplainResult`, `PredictResult`,
`VersionsResult` in the run.

**Test:** table-driven over the rounding rule — exact, rounded-ok,
rounded-too-far, truncated, integer-exact, integer-off-by-one,
number-in-a-name. The live transcript's `0.003826` grounds; a fabricated
`0.005` does not.

---

## Task 4 — Relationship grounding: the claim must trace to the right field

**The check design §6 did not have, and the window proved it needs.**
Every number in the false answer was grounded by Task 3. The defect was
the verb. "Trained on Delta versions 92" is a claim of type
*training-data provenance*, and training-data provenance lives in exactly
one field: `VersionsResult.training_data_delta_versions`. The answer's
`92` traced to `read_delta_versions` instead. Different field, different
meaning, and the tool the agent needed (`versions`) was never called.

The check: for a closed set of claim patterns, the number attached to the
claim must trace to the field that claim type requires.

| Claim pattern in the answer | Must trace to |
|---|---|
| "trained on ... version(s) N" / "training data ... N" | `VersionsResult.training_data_delta_versions` |
| "read ... version(s) N" / "as of Delta version N" | `*.read_delta_versions` |
| "model version N" / "champion ... N" | `VersionsResult.model_version` or `*Provenance.model_version` |
| "score is N" / "breach risk ... N" | `PredictResult.probability` |
| "baseline ... N" | `ExplainResult.baseline` |

A claim of a type in this table whose number traces only to a *different*
field is `Ungrounded` — even though the number is real. A claim whose
required field is absent from the run (no `versions` call) is
`Ungrounded` with "the tool that would substantiate this was not called".

The set is closed and small on purpose. An unrecognized claim shape is
not failed — it falls through to Task 3's number check alone, and the
trace records that no relationship rule applied. Growing the table is a
later task, not a crash.

**Test:** the live transcript now fails here — "trained on ... 92" traces
to `read_delta_versions`, not `training_data_delta_versions`. A repaired
answer that says "read Delta version 92" passes. A `versions`-less run
that claims training provenance fails with the not-called reason.
Mutation: drop the "trained on" pattern and confirm the transcript test
goes green (it should not).

---

## Task 5 — Retry once, then abstain

A failed verification is not a crash and not a silent pass. The bounded
agent gets one more turn: the verifier's failed trace rows are handed
back to the model as an observation — "this claim did not trace; the tool
that substantiates it is `versions`; you have one turn" — and the loop
runs once more.

If the re-run's answer verifies, it is returned with its trace. If it
still fails, the agent returns a new terminal outcome, `Ungrounded`,
carrying the last answer, the failed trace, and the retry count. It never
returns a number it could not ground.

`Ungrounded` joins the existing `AgentOutcome` union (alongside the
completed and turn-bounded variants). It is a structured refusal, the
same shape as `predict`'s drift refusal — the platform makes an
ungroundable answer a typed result, not a thing that ships.

**Test:** a transcript that fails verification twice ends `Ungrounded`
with `retries == 1`; one that fails then passes returns the second
answer. The turn bound from Phase 9 still holds — the retry consumes one
of the budgeted turns, it does not get a free one. Mutation: remove the
abstain and confirm a still-failing run no longer ends `Ungrounded`.

---

## Task 6 — The golden set

Questions with expected tool-call sequences and expected outcomes,
committed as fixtures, replayed offline.

- **The known-bad fixture is already here.**
  `tests/fixtures/transcripts/2026-09-10-live-predict-explain.json` is a
  real transcript that carries a real false claim. Its golden entry
  asserts the verifier returns `Ungrounded` on the relationship check,
  naming `training_data_delta_versions`. This is the fixture the whole
  phase exists to catch, and it was produced by a substitute model on
  real data, not written to fail.
- **Known-good fixtures**: hand-built transcripts where the agent calls
  `versions`, claims only what the fields support, and verifies clean —
  one per tool combination (`predict` alone, `predict`+`explain`,
  all-three, a reduced-era refusal).
- **Expected tool-call sequences**: each golden entry names the calls the
  run should make; a run that answers a provenance question without
  calling `versions` is a golden-set failure even if nothing else is
  wrong. This is design §6's Task 3 and it encodes the window's lesson —
  the agent that skipped `versions` is the agent that made the false
  claim.

**Test:** the golden set runs as a parametrized test; every entry's
actual outcome matches its expected one. Adding a fixture is adding a
JSON file, no code.

---

## Task 7 — Directional assertions

A claim that a factor *raises* risk must not cite a *negative*
contribution, and vice versa. `explain` returns each contribution with a
sign; the answer's directional language ("driven up by", "decreases the
risk", "the strongest contributions ... all of which decrease") must
agree with it.

The window's answer happens to be correct here — it said the top three
"decrease the risk of breach" and their contributions are negative. So
the first fixture for this check is a *deliberately* flipped copy of that
answer: same numbers, "increase" swapped for "decrease", asserted
`Ungrounded`.

**Test:** the flipped fixture fails; the real one passes; an answer with
no directional language is not failed (nothing to contradict). Mutation:
invert the sign comparison and confirm the real transcript starts
failing.

---

## Task 8 — Mutation-test the verifier

**Not optional (design §6).** "A verifier nobody tried to break is a
control of unknown scope, which is the `pseudonymity.py` failure again."
`temporal_split` was broken three ways and `canonicalize` five of seven
before either was trusted.

Break the verifier deliberately, at least one mutation per check:

- number extraction misses scientific notation / a trailing-period
  literal / a percentage;
- the rounding rule widened to a tolerance band (`0.004` grounds
  `0.0038`);
- the relationship table's "trained on" row deleted;
- the "tool not called" branch turned into a pass;
- the directional sign comparison inverted;
- the `Ungrounded` outcome downgraded to a warning.

Each mutation must turn at least one existing test red. A mutation that
kills nothing is a missing test, and the test gets written before the
mutation is reverted.

**Test:** a `tests/unit/test_grounding_mutations.py` that applies each
mutation (monkeypatch or a parametrized broken-verifier) and asserts the
suite catches it — the same shape as Phase 9's contribution off-by-one
mutation test.

---

## Task 9 — The CI gate, and the trace on disk

Two things, both wiring:

- **The gate.** An ungrounded number fails the build. `make check` runs
  the golden set and the transcript replays; any entry whose verdict is
  `Ungrounded` where the golden entry expects grounded (or the reverse)
  is a red build. It is offline — it replays committed transcripts and
  calls no endpoint — so it runs on the 2-core CI runner in the normal
  `pytest` step, not a separate job. Design §6's gate: "a deliberately
  hallucinated number fails CI", and Task 6's flipped fixtures are that
  hallucination, committed.
- **The trace on disk.** Each agent run — in the replay harness and in
  any future window — writes its `GroundingTrace` next to its transcript,
  as `<transcript-stem>.grounding.json`. Design §6 Task 7. The window's
  own run gets one retroactively, committed beside the transcript, so the
  false claim has a machine-readable record of *why* it is false and not
  only the prose in the findings doc.

**Test:** `make check` fails when a known-bad fixture is marked
`expected: grounded`; the trace file is produced and round-trips; the
findings-doc transcript's trace names the relationship failure.

**STATUS.md:** the CI gate is live and the last build task is done —
Current position moves to "Phase 10 complete on `phase-10-grounding-verifier`,
PR open."

---

## Task 10 — Close out `v1.1.0`

**This is the release, and it runs after the Phase 10 PR merges to
`main`.** Every prior phase updated `docs/STATUS.md` per task; this task
is the parts that only make sense once the whole scope is on `main`. It
mirrors Phase 9's Task 12 but carries the tag, because `v1.1.0` covers
Phases 9 and 10 together and neither closed the release alone.

**The one hard gate is external and dated.** The Phase 9 window's cost —
`system.billing.usage` for workspace `7405615444091260`, on or after
2026-09-11 — must be read and recorded, or explicitly marked uncloseable
in this workspace (the token-reconciliation exit-gate row), **before the
tag**. `v1.0`'s discipline was that every number in the release is
measured or bracketed; a `v1.1.0` tagged over an unresolved cost breaks
that on the release commit itself.

Then, in order:

1. **Story bank.** Append the entries this scope earned — at minimum:
   - the graphify post-commit-hook vs tracked-`graph.json` conflict and
     how it resolved (a hook that re-clusters without an LLM cannot
     coexist with a committed curated graph; `graphify-out/` is local
     state), and the `.claudeignore` check that found the feature is a
     documented hallucination;
   - the window's false claim under a green "every number from a tool"
     rule, and why the fix was a schema rename plus a *relationship*
     check, not a stricter number check;
   - anything else that broke, was measured, or was decided against a
     real trade-off during Phase 10.
   The gist ID is in `.claude/story-bank-gist-id`. The trigger is
   documented-unreliable; this task exists so it does not depend on
   remembering.
2. **`README.md`, including the architecture diagram.** Two kinds of edit:
   - **The moving parts.** The agent layer and the grounding verifier
     change what the diagram depicts — a read-only tool surface, two
     gateways, a bounded agent, and now a deterministic verifier on the
     output path. The diagram updates in the same commit as the prose,
     per CLAUDE.md. Status, the finding, and the `v1.1.0` line all move.
   - **The narrative, for a first-time reader** — the sections a hiring
     manager or a stranger opens the repo for, not the phase-by-phase
     record:
     - **Why this project — the problem and the motivation.** Work-queue
       risk on a dataset that is real, large, free, genuinely messy and
       carries a real schema break; point-in-time correctness as the one
       invariant that cannot be bluffed in a review; why the domain is
       deliberately incidental (the same platform serves a ticket queue,
       a claims backlog, a fraud review).
     - **What the project is for.** What a reader can actually do with it
       — clone to a green run in minutes, the medallion over GH Archive,
       the point-in-time feature platform, a served model with a measured
       baseline, and an agent that answers breach-risk questions with
       every number traceable to a tool and mechanically checked.
     - **Key takeaways and future roadmap.** What the build proved — the
       leakage bug a green suite missed, the four defects only real data
       through the full stack surfaced, the agent's own false claim under
       a green "every number from a tool" rule — and where it goes next:
       a larger relationship table, an LLM-judge cross-check, the two
       deferred dashboard panels, a real serving path for the agent's
       model.
3. **`CLAUDE.md`.** Any convention Phase 10 set (the verifier as a single
   enforcement point; `graphify-out/` as local state; whatever the
   mutation-test task codified). The "Current status" block and the
   stale-record tally get their Phase 9/10 entries.
4. **`docs/goal-reconciliation.md`, `docs/resume-bullets.md`,
   `docs/limitations.md`.** Reconcile the agent-layer goals against what
   shipped; add the measured `v1.1.0` resume bullets (no unmeasured
   numbers); state the verifier's real limits in `limitations.md` — the
   closed relationship table, no LLM-judge cross-check, English-only
   claim parsing.
5. **Tag `v1.1.0` on `main`**, annotated, after 1–4 are committed and the
   cost gate is closed. The same shape as `v1.0`: a tag on the merge
   commit, not on a branch.
6. **GitHub profile.** The one step outside this repo: the profile README
   / pinned-repo blurb / the repo's About + topics, updated to say the
   platform now has an agent layer with mechanical grounding. Draft the
   text here; the push to the profile repo is manual and gets confirmed.

**Done when.** All three reader-facing artifacts answer *what changed that
a reader would want to know?*; the tag resolves on `main`; the cost row is
closed or explicitly recorded as uncloseable; the profile text is drafted
and handed over.

---

## Exit gate

Marked against evidence at close-out, not from memory.

- [ ] `read_delta_versions` everywhere `delta_versions` meant "what was
      read"; `training_data_delta_versions` untouched; pure rename,
      mutation-checked
- [ ] Every numeric literal in an answer is matched to a tool field or
      the run is `Ungrounded`; the rounding rule is table-tested at its
      boundary
- [ ] The live transcript's "trained on ... 92" fails the relationship
      check, naming `training_data_delta_versions` as the field it should
      have traced to
- [ ] A provenance answer with no `versions` call fails the golden set
      even if every number is real
- [ ] A failed verification retries exactly once, then returns
      `Ungrounded` — never a number it could not ground
- [ ] The golden set replays offline; the known-bad fixture is the
      committed window transcript, unedited
- [ ] A directional claim that contradicts the contribution sign fails;
      the first fixture is a flipped copy of a real answer
- [ ] Every verifier check has a mutation that turns the suite red
- [ ] `make check` goes red on a hallucinated number, on the 2-core
      runner, with no endpoint call
- [ ] Each run writes a `GroundingTrace` beside its transcript; the
      window run has one
- [ ] Full `make check` green before the push

**Close-out (Task 10, after the merge):**

- [ ] Phase 9 window cost read from `system.billing.usage`, or the
      token-reconciliation row explicitly recorded as uncloseable in this
      workspace — done *before* the tag
- [ ] Story-bank gist carries the Phase 9/10 entries (the graphify-hook
      conflict, the window's false claim, the schema-vs-relationship fix)
- [ ] `README.md` and its architecture diagram show the agent layer and
      the verifier; status and the `v1.1.0` line updated in the same commit
- [ ] `README.md` carries the first-time-reader narrative: why the project
      (problem & motivation), what it is for, key takeaways & future roadmap
- [ ] `CLAUDE.md`, `goal-reconciliation.md`, `resume-bullets.md`,
      `limitations.md` reconciled to what shipped; no unmeasured number
- [ ] `v1.1.0` tagged on `main`, annotated, on the merge commit
- [ ] GitHub profile / repo About text drafted and handed over for a
      manual push

---

## Deferred out of Phase 10, on purpose

**Out of scope entirely**, per design doc §7 — not to be re-litigated
mid-phase: an LLM judge as a second opinion, a groundedness *score*
(the gate needs a decision, not a probability), retrieval-augmented
anything, new tools, retraining, a chat surface, multi-agent review.

**Not deferred, and worth naming.** The relationship table (Task 4) is
small and closed. It would be easy to call that a limitation and leave
the number check alone — which is exactly the scoping that let the window
ship a false claim under a green "every number from a tool" rule. The
table is the phase's reason for existing; it starts small because the
claim shapes this system actually makes are few, not because the check is
half-built.

**One dependency on Phase 9's open thread.** `v1.1.0` is not tagged until
the Phase 9 window's cost is read (`system.billing.usage`, on or after
2026-09-11) and its exit-gate rows for token reconciliation are closed or
explicitly recorded as uncloseable in this workspace. Phase 10 can be
complete and merged before that read; the tag waits for it.
