# Decision memo: should the breach-risk model go into the review queue?

**Date:** 2026-09-08
**Audience:** whoever owns the review queue this platform is meant to serve
**Decision requested:** deploy `pr_review_sla_risk` v1 to rank incoming
work items for human intervention — yes, no, or not yet.

---

## Recommendation

**Not yet. Deploy it as a ranked triage queue with a human in the loop
*after* one specific re-measurement — and never as an automated action.**

**Confidence: low on the reported numbers transferring to production;
moderate on the ranking being genuinely better than the naive baseline.**

Those two confidences are different on purpose, and the reason is the
whole memo.

---

## What was measured, and it is good

The model beats its baseline by a wide, measured margin on the metric
§5.3 pre-committed to (PR-AUC, chosen because the target is 25.55%
positive and ROC-AUC flatters an imbalanced problem):

| | Average precision | ROC-AUC | Log loss |
|---|---|---|---|
| Breach-rate baseline | 0.2845 | — | — |
| `default` (champion) | **0.6120** | 0.8279 | 0.4219 |
| `is_unbalance` | 0.6074 | 0.8279 | 0.5081 |

**2.15× the baseline.** The gate in `run_training` — register only if the
candidate beats the baseline — fired for real, and `is_unbalance` losing
is itself informative: the obvious imbalance remedy did not help.

Calibration, measured over all 7,320,196 scored rows in Task 10's window,
is **monotonic across all ten deciles** and tracks observed rates closely:

| Decile | Mean predicted | Observed |
|---|---|---|
| 1 | 0.0018 | 0.0006 |
| 5 | 0.1920 | 0.1818 |
| 6 | 0.2341 | 0.2353 |
| 9 | 0.5205 | 0.5225 |
| 10 | 0.6837 | 0.7155 |

And per segment it is close to exact — bots 0.3206 predicted against
0.3216 observed, humans 0.2204 against 0.2197, reproducing §5.3's
independently measured breach rates to the basis point.

**A well-calibrated, monotonic ranker that clearly beats its baseline.**
On the evidence above alone, the answer would be yes.

---

## Why the answer is "not yet"

**The evaluation split is random, and this project's own design document
calls that a leakage bug.**

`train_classifier` uses
`train_test_split(frame, test_size=0.3, random_state=42)`. Design doc
§4.5 states, in its own words:

> Train/test must be split **temporally**; a random split is itself a
> leakage bug

§5.1 adds a second, unimplemented requirement: **temporal splits must
fall on whole-week boundaries**, because weekday and weekend review
latency differ sharply and an arbitrary split point encodes day-of-week.

Neither is implemented. The frame already carries `as_of_timestamp`, so
the column needed to fix it is sitting in the data.

### What this does and does not mean

It is worth being precise, because "leakage" is a word that gets used to
mean everything and therefore nothing.

**The features are point-in-time correct.** `as_of_join` guarantees every
row's features are computed from events strictly before that row's own
`as_of_timestamp`, and the leakage suite tests exactly that. **No feature
sees its own future.** That property holds and is not in question.

**The evaluation is not temporally honest.** A random split lets the
model be scored on pull requests opened *earlier* than ones it trained
on, which is not the situation it will ever face in production. Three
concrete channels:

1. **Entity autocorrelation, the largest.** The same repo and the same
   author appear in both train and test, with overlapping feature
   histories. `author_prior_pr_count` and `repo_events_prior_24h` are
   strongly autocorrelated over 92 days — knowing how a repo behaved in
   July helps predict July.
2. **Regime overlap.** Anything that shifted during Q3 2025 is present on
   both sides of the split, so the model is never asked to generalise
   across time.
3. **Day-of-week**, which §5.1 explicitly warns about.

**So the honest reading of 0.612 is: an optimistic estimate of deployment
performance, by an unmeasured margin.** It could be a small margin. It
could be most of the lift. Nobody knows, and that is the point.

### Why the relative claim survives better than the absolute one

The baseline was fit on **the same split** and enjoys the same advantage.
So "the model beats a per-segment breach rate by 2.15×" is considerably
more robust than "the model achieves 0.612 average precision".

That asymmetry is why the two confidence levels above differ. A ranker
that beats its baseline under identical favourable conditions is probably
still better than that baseline under fair ones. How much better is
exactly what is unmeasured.

---

## The one measurement that would change this recommendation

**Re-run the classification comparison with a temporal split on whole-week
boundaries.** Train on the first N weeks of Q3 2025, test on the
remainder.

- **Cost:** one job run, ≈$1 and ~15 minutes on the 5-VM shape, from
  Phase 4's measured runs.
- **Change required: none — done.** `temporal_split` landed 2026-09-08
  and `train_classifier` now uses it, mutation-tested three ways. Only
  the paid re-run and re-registration remain.
- **Still true as of this memo:** the registered champion `v1` was
  trained through the *old* random split and has not been retrained, so
  every number quoted above is still the random-split number.
- **What it buys:** the deployment-honest number, and a direct
  measurement of how much the random split was worth.

If the temporally-split PR-AUC stays materially above 0.2845, the
recommendation becomes **yes, as a ranked queue with a human in the
loop**, at moderate-to-high confidence. If it collapses toward the
baseline, that is a null result worth as much as a positive one and it
gets written down, per CLAUDE.md's rule that a null result is a finding.

This is cheap, decisive, and it is the single highest-value measurement
remaining in the project.

---

## Constraints that bound deployment regardless of that result

These do not change with a better split. They are properties of the
system as built, and each is stated fully in `docs/limitations.md`.

| Constraint | Consequence for deployment |
|---|---|
| **The served endpoint returns a boolean, not a probability** | Nothing can be *ranked* from the live endpoint today. Scores come from an offline batch job. A production queue needs the signature re-logged. |
| **Live labels are impossible** on the current firehose era | `payload.pull_request` was cut from 48 fields to 5 in 2025-10. The model can be *served* live but cannot be *evaluated* live from this source. |
| **The live feed captures ~7%** of the stream | A politeness ceiling, not a rate limit — but a production queue would need the real event source, not a public sample. |
| **The predictions table violates its own key** — 75 duplicate rows over 25 PRs | 0.001%, below any metric's resolution, but it means "one row per work item" is not currently guaranteed. |
| **Right-censored items are excluded from training** | The model has never seen the "still open, no response yet" population it would be scoring in production. This is a deliberate correctness choice, not an oversight, and it is the deepest structural caveat here. |

That last row deserves emphasis. The trainable population is *closed*
items plus `closed_no_response`. A live intervention queue is made
entirely of **open** items. The model is being asked to score a
population whose outcomes are by definition not yet observable, having
been trained only on ones whose outcomes were. A temporal split would
partly probe this; only a real deployment measures it.

---

## What I am not recommending, and why

**Not automated action** — no auto-escalation, no auto-assignment, no
SLA-clock manipulation. A well-calibrated score is a decision *input*.
The measured error at decile 10 alone (0.6837 predicted against 0.7155
observed) is enough to make an automated threshold arbitrary.

**Not a bot-aware policy without review.** Bots breach *more* than humans
(32.16% vs 21.97%), the opposite of the pre-measurement intuition. A
queue that deprioritised bot-authored items on the assumption they matter
less would be wrong on this data — but "bots breach more" is a fact about
GH Archive, not necessarily about the queue you actually own.

**Not a claim that this transfers domains.** The architecture is
deliberately domain-agnostic — nothing in the platform layer knows the
items are pull requests — but the *model* is fitted to this population,
and the calibration above is a property of GitHub PR review latency in
Q3 2025.

---

## Summary

| | |
|---|---|
| **Recommendation** | Not yet. Re-measure with a temporal split, then deploy as a human-in-the-loop ranked queue. |
| **Confidence, absolute performance** | **Low** — the reported 0.612 is optimistic by an unmeasured margin |
| **Confidence, relative to baseline** | **Moderate** — both sides shared the same favourable split |
| **Blocking measurement** | Temporal, whole-week split re-run. ≈$1, ~15 min. |
| **Blocking engineering** | Re-log the model signature so the endpoint returns a probability |
| **Would reverse this** | Temporally-split PR-AUC collapsing toward 0.2845 |
