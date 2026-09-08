# ADR-0008: Score the quarter in batch, beside the served endpoint

**Status:** Accepted 2026-09-07, implemented Phase 7 Task 9a.

## Context

§7's page 1 ranks open pull requests **by predicted breach risk** so a
human can intervene. §5 promises the same thing. Ranking needs a score.

## The problem, found while seeding the inference table

**The served endpoint returns a class, not a probability.** `train.py`
logs the champion with a signature inferred from `model.predict()`, so
every row in the inference table is a boolean. A boolean cannot rank an
intervention queue.

This surfaced during Task 1 — while sending real invocations to prove
capture worked — rather than during Task 9, which is the difference
between a design question and a rewrite.

## Decision

`almanac.model.score` and `score_runner` batch-score the trainable
population offline from the **same registered champion**, writing
`almanac_dbx.features.pr_breach_predictions` with `breach_risk` beside the
true outcome.

`mlflow.lightgbm.load_model` is used deliberately, **not**
`mlflow.pyfunc.load_model`: pyfunc would hand back the serving behaviour,
which is exactly the boolean being worked around. The flavor's own loader
returns the estimator, which still has `predict_proba`.

The model URI pins a **version, never `@champion` by alias** — a scored
table whose model can change under it is not reproducible, which is the
property the table exists to have.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Re-log the model with a `predict_proba` signature and redeploy the endpoint** | The correct long-term fix, but it invalidates the inference table already accumulating under Task 1 — the one artifact here that cannot be re-derived at any price, since only traffic after switch-on can ever be logged. |
| **Rank by the boolean plus a tiebreaker** | Not a ranking. The page would be sorted by an arbitrary secondary key wearing a risk label. |

## Consequences

Page 1 ranks 7,320,196 scored rows over the whole quarter rather than 24
live predictions, and shows the score **beside the realised outcome**,
which makes the calibration panel possible at all — monotonic across all
ten deciles.

**The gap is real and is not smoothed over**: offline scoring and online
serving now produce different *types* for the same model. That is a
training/serving skew of the most basic kind. Re-logging the signature is
the fix, deferred because of what it would cost the inference log, and it
carries forward to `docs/limitations.md` (Task 12) as an open item rather
than being closed here.

**Evidence:** `docs/findings/2026-09-08-reporting-window-evidence.md`,
STATUS.md's Phase 7 Task 1 row.
