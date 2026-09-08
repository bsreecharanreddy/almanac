---
name: almanac-leakage-review
description: Use when touching anything that decides what a model may see — feature computation, the as-of join, the training frame, the train/test split, label construction, or a new feature group. Enforces asking which axis of leakage is in play, because this project's leakage suite covers one axis and a real bug got through on another.
---

# Leakage review in Almanac

**Incident-derived, and the incident is the reason this file exists.**
CLAUDE.md deferred this skill with an explicit trigger — *"write it the
first time a leakage bug actually gets through"* — and on **2026-09-08**
one had, for four days, in the code that produced the registered
champion.

## The incident

`train_classifier` used `train_test_split(frame, test_size=0.3,
random_state=42)`. Design doc §4.5 states, in its own words:

> Train/test must be split **temporally**; a random split is itself a
> leakage bug

§5.1 adds that the boundary must fall on a whole week, because
weekday/weekend review latency differs sharply.

**The leakage suite was green the entire time, and it was not wrong.** It
tests that every row's features come from events strictly before that
row's own `as_of_timestamp`. That property held. It still holds.

**The suite tested one axis. The bug was on another.**

## Gate 1 — Which axis?

Leakage is not one thing. Before calling a change safe, say **which of
these it touches**, and what tests that axis:

| Axis | The question | What covers it here |
|---|---|---|
| **Row time** | Do this row's features come from strictly before its own `as_of`? | The leakage suite. Solid. |
| **Split time** | Is every test row later than every train row? | `temporal_split` + `test_model_temporal_split.py` — **added only after the bug** |
| **Entity overlap** | Do the same repos/authors appear on both sides, with autocorrelated features? | **Nothing.** Still uncovered; a temporal split reduces it but does not eliminate it. |
| **Label construction** | Does the label encode something unknowable at `as_of`? | §5.1's five-category `label_exclusion` taxonomy |
| **Target definition** | Is the threshold derived from data that includes the test set? | §5.3 reuses a **fixed** 1,487 s rather than recomputing |

**A green leakage suite answers row time and nothing else.** Saying "the
leakage tests pass" is a true statement about one row of that table.

## Gate 2 — Does the test discriminate?

A test that passes proves nothing until it has been seen to fail.
**Break the implementation and watch it go red**, the way this repo
already does elsewhere:

- `temporal_split` was mutation-tested three ways — random split instead
  of temporal (3 tests red), boundary on any day rather than Monday (2
  red), allow an empty training side (1 red).
- `canonicalize` in the lineage extractor: against a name-only
  implementation, **5 of 7 tests go red**.

If no mutation makes the new test fail, the test is decoration.

## Gate 3 — What can the fixture not express?

**Twice now, a fixture could not produce the failure it was supposed to
guard.**

- The model fixtures carried **no `as_of_timestamp` at all**, so a
  temporal split was not merely untested — it was unrepresentable. The
  frames had no time in them.
- The streaming fixture's late rows were **all duplicates**, so
  "distinct late event is dropped" had no case to fail on. That one cost
  18% of a live window
  (`docs/postmortem-watermark-data-loss.md`).

Ask directly: **is the bad case constructible in this fixture?** If not,
the test cannot fail and its green is uninformative.

## Gate 4 — Is a justification in the assertion a claim, or a wish?

The watermark bug survived review because the test that asserted it
**explained why it was fine**:

> "poll 3's row is too late to land — correct here, since it is a genuine
> duplicate of poll 1's row, **not data loss**"

True for the fixture. False for the feed. A justification written into an
assertion message is a claim like any other and can be wrong — and it is
**more** dangerous than silence, because it pre-empts the question a
reviewer would otherwise ask.

When writing one, state the scope it holds under. When reading one, treat
it as an assertion to check, not as a reason to stop.

## Calibration — things that look like leakage and are not

Do not raise these as findings; they were each checked and are correct:

- **`similar_prior_breach_rate` reads Gold's `fact_pull_request`**, which
  looks like a §3.1 violation and a label leak. It is neither: the
  feature needs *prior* PRs' labels, and the lookup is point-in-time
  filtered. The lineage artifact surfaced this edge on its first run and
  it was checked before being claimed.
- **The spine fan-out** (25 PRs, 75 duplicate rows) is a real defect and
  is **not** leakage — every duplicate row's features still precede its
  own `as_of`. Say which it is; the distinction is the whole point.
- **`compute_author_activity` counts only prior PRs that had already
  closed.** An open prior PR is *unknown*, not *not merged*. Folding it
  into the denominator would be the leak; excluding it is the fix.

## Red flags

| Thought | Reality |
|---|---|
| "The leakage suite passes" | It covers row time. Name the axis. |
| "The features are point-in-time correct, so the model is sound" | Exactly the reasoning that let the random split stand for four days. |
| "The test asserts this behaviour, so it's intended" | The watermark test asserted a bug and explained it. |
| "There's no time column, so a temporal split isn't possible" | `as_of_timestamp` was in the frame the whole time. |
| "A random split is fine, the data isn't a time series" | §4.5 already answered this for this project. |
