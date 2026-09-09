# The champion, re-scored on a temporal split

**Date:** 2026-09-08
**Run:** `817800814439176` (job `117899697123337`), SUCCESS, 890 s
**Supersedes the headline metric of** `2026-09-04-classification-model-serving-measured.md`

## What was measured

Phase 7 Task 13 found that `train_classifier` used
`train_test_split(random_state=42)` where design doc §4.5 requires a
temporal split and calls a random one *"itself a leakage bug"* — in the
code that produced the registered champion. Task 15 fixed it. This is the
re-score.

Same population (Q3 2025), same features, same candidate sweep, same
baseline comparison. **The split is the variable.**

| | Random split (2026-09-04) | Temporal split (2026-09-08) | Δ |
|---|---|---|---|
| Best candidate | `default` | **`is_unbalance`** | **changed** |
| **PR-AUC** | 0.6120 | **0.4661** | **−0.1459 (−23.8%)** |
| ROC-AUC | 0.8279 | 0.7546 | −0.0733 |
| Baseline PR-AUC | 0.2845 | 0.2650 | −0.0195 |
| Lift over baseline | 2.15× | **1.76×** | −0.39× |

**The published number was optimistic by 24%.** "Optimistic by an
unmeasured margin" — the phrasing carried in five documents since
2026-09-08 — is now a measured margin.

## Three things worth more than the headline

**1. The winning configuration changed.** `default` won under the random
split; `is_unbalance` wins under the temporal one. A random split would
have shipped the wrong *hyperparameter configuration*, not merely an
inflated score for the right one. The candidate sweep (§5.3) was not
decoration.

**2. The baseline moved too**, 0.2845 → 0.2650. It is fit on the training
side of whichever split is in force, so it is not a fixed yardstick. The
lift figures are like-for-like *within* each split; 0.4661 must not be
compared against the old 0.2845.

**3. The gate passed on its own terms.** The model still beats its
baseline by a real margin, so `register_champion` registered **version 2**
and moved the `@champion` alias onto it — the same gate that correctly
*refused* to register Phase 4's regression model when it lost to a
per-segment median. Nothing was forced.

Read back from the live registry on 2026-09-08 rather than inferred from
the run succeeding:

```
databricks model-versions list almanac_dbx.models.pr_review_sla_risk
  -> version 2, status READY, run 5f6a71bd9e4f4b6d8f7ea0a6454c0ca5
databricks registered-models get ... --include-aliases
  -> champion -> version_num 2
```

**Version 1 is the retracted random-split model and must not be served.**
That check found a second, quieter defect: `databricks_model_serving`
still pinned `entity_version = "1"`, so the next `terraform apply` — Task
7's demo window — would have stood up the withdrawn model behind an
endpoint the README describes with the new number. The provider has no
alias form for `entity_version` (databricks/databricks 1.131.0), so the
pin cannot follow the alias on its own; a test now fails the build when
the two disagree.

## Attribution — what else was in this run

The deploy carried three fixes, not one. Honest attribution matters more
than a clean story:

| Fix | Effect on this number |
|---|---|
| `temporal_split` (Task 15) | **the cause of essentially all of it** |
| PR-opened spine fan-out (Task 4) | 25 PRs of 7,320,121 — **0.001%**, below any metric's resolution |
| `action='merged'` (Task 2) | **none.** Reduced-era only; Q3 2025 is rich-era, where the field is read directly |

## Method, and one thing that nearly went wrong

The job runs `python_file` from `/Workspace`, with the code in a wheel —
so **triggering it without redeploying would have re-run the old
random-split code and reproduced 0.612**, looking like confirmation. The
local wheel was also stale (built 00:42; the fix committed 12:02).

Verified rather than assumed, at each step:

```
rebuilt wheel:   temporal_split present True | train_test_split gone True
                 earliest_opened_events True | _merged_from_action True
deployed wheel:  downloaded back, 129,334 bytes both sides,
                 temporal_split present True | train_test_split False
```

The remote hash did not match the local after upload — that was
`export --format AUTO` re-encoding, confirmed by comparing content rather
than waving the mismatch through.

## What this does not establish

- **Not a re-validation of the serving endpoint.** Latency figures in
  `2026-09-04-model-serving-measured.md` are unaffected and stand.
- **Not a statement about 2026 data.** The population is Q3 2025. The
  champion reads `is_draft`, which the reduced era does not carry, so it
  cannot score a current window at all — see `almanac.model.drift`.
- **`n=1` run.** One temporal split at one boundary. The split is
  deterministic and the boundary is a whole week (§5.1), so a re-run
  reproduces it; that is reproducibility, not a distribution over
  boundaries.
