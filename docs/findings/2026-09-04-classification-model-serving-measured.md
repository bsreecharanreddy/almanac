# Findings — Phase 4's classification re-run: a measured, non-null result

> ### ⚠️ Superseded 2026-09-08 — the PR-AUC below came from a random split
>
> The 0.612 / 0.2845 / roc_auc 0.828 figures in this document were produced
> by `train_test_split(random_state=42)`, where design doc §4.5 requires a
> **temporal** split and calls a random one "itself a leakage bug". Re-scored
> on the temporal split: **0.4661 PR-AUC against a 0.2650 baseline**,
> `roc_auc` 0.7546 — a 1.76x lift, and the published figure was optimistic
> by **24%**. The winning configuration changed too, `default` to
> `is_unbalance`.
>
> **Everything else here stands** — the serving latency, the registration
> defect, the segment breach rates. Only the split-dependent metrics moved.
> See `2026-09-08-champion-rescored-temporal-split.md`. The original numbers
> are kept below, unedited, because a retraction that deletes the evidence
> is not a retraction.


**Date:** 2026-09-04
**Source:** Training run `1102177434831425` (job `117899697123337`, task
run `884564330191323`) against the real Q3 2025 quarter; MLflow experiment
`/Shared/almanac/pr-review-sla-risk` (experiment id `3479548709615268`),
runs `d1b694d4f7ea4fce972c29ea913f7714` (baseline),
`e4f29f7f8b264c16b29f081e377c445e` (`default`, the winner),
`90fe184fc0464d308b8fffb4669b4fb5` (`is_unbalance`); Unity Catalog
`almanac_dbx.models.pr_review_sla_risk` version 1.
**What it closes:** the classification path opened by §5.3, executed
through `docs/plans/2026-09-04-phase-4-classification-plan.md`'s Task 13 —
the real-cloud re-run this plan's own next step called for, replacing
Task 9's regression null result with a measured comparison on the same
quarter.

---

## The measured result

Both candidates beat the segment-rate baseline by more than 2x on PR-AUC —
`beats_baseline = True`, not the null result Task 9 recorded for the
regression objective on the same data.

| Run | average_precision (PR-AUC) | roc_auc | log_loss |
|---|---|---|---|
| `baseline` (breach rate per `is_bot_author` segment) | **0.2845** | — | — |
| `default` (plain `LGBMClassifier`) — **winner, registered** | **0.6120** | 0.8279 | 0.4219 |
| `is_unbalance` (`is_unbalance=True`) | 0.6074 | 0.8279 | 0.5081 |

`_best_candidate` picked `default` on the higher average precision (0.6120
vs. 0.6074 — close, `is_unbalance`'s built-in class-weighting bought
nothing measurable on this split); `run_training`'s
`register AND beats_baseline` gate fired for the first time for real and
registered it.

## Unity Catalog registration, verified directly

```
databricks registered-models get almanac_dbx.models.pr_review_sla_risk
databricks model-versions get almanac_dbx.models.pr_review_sla_risk 1
databricks api get /api/2.1/unity-catalog/models/almanac_dbx.models.pr_review_sla_risk/aliases/champion
```

Version 1 is `READY`, sourced from run `e4f29f7f8b264c16b29f081e377c445e`
(the `default` candidate); the `@champion` alias resolves to that same
version, with its metrics (`average_precision`, `roc_auc`, `log_loss`,
`beats_baseline`, `is_best_candidate`) attached to the alias record itself
— `register_champion`'s `set_registered_model_alias` call, exercised for
real for the first time in this project.

## A real defect this run found, that no local test had (see STATUS.md)

Attempt 1 (run `704688669520130`) trained successfully — real signal,
same shape as attempt 2 — and then failed at `register_champion`:
`MlflowException: Model passed for registration did not contain any
signature metadata`, from `UcModelRegistryStore._validate_model_signature`.
Neither `log_training_run` nor `log_classification_run` had ever passed
`signature=` to `mlflow.lightgbm.log_model` — a latent bug in both paths
since Task 6, invisible locally because a `file://` MLflow store doesn't
validate signatures the way Unity Catalog's registry does, and never
triggered before this run because Task 9's regression model lost its own
gate and `register_champion` was never actually called against a live
UC before now. Fixed in `train.py` (`infer_signature` at fit time, carried
on `TrainResult`/`ClassifierCandidateResult`, passed through at log time);
closed with two new local regression tests asserting
`mlflow.models.get_model_info(model_uri).signature is not None`, so this
class of failure is now caught before a future paid run rather than by
one. Full detail in `docs/STATUS.md`'s verification log, same date.

## The population this ran against (§5.3, re-confirmed by this run's own success)

Same measurement §5.3 recorded on 2026-09-03, exercised for real here
rather than re-derived: population = `label_exclusion IS NULL OR
label_exclusion = 'closed_no_response'`, **n = 7,320,121**, measured
breach rate **25.55%** overall (1,869,921 breaches), bot **32.16%**,
human **21.97%**. Threshold **1,487s** (p75, §5.2), passed to the job
explicitly via `--threshold-seconds 1487`, not recomputed.

## What this run verified, real-cloud rather than local

- The full classification code path — `join_breach_label` →
  `build_classification_frame` → `train_classifier`'s comparison sweep →
  `log_classification_run` → `register_champion` — runs end to end
  against the real 341M-row-quarter lake, not just against Task 10-13's
  local fixtures.
- MLflow tracking against the workspace backend (`--tracking-uri
  databricks`) logs a real three-run comparison table (baseline + 2
  candidates) in one experiment, exactly as `log_classification_run`
  is unit-tested to.
- Unity Catalog model registration and `@champion` alias assignment work
  end to end for the first time in this project — Task 9 only proved the
  gate correctly does *not* fire; this run proves it correctly *does*.
- `databricks_model_serving.pr_review_sla_risk`'s Terraform resource now
  has something real to point at (`entity_version = "1"` exists); not yet
  applied — a separate step, since standing up a live serving endpoint is
  a new resource decision, not implied by re-running the training job.

## Cost of this run window

Attempt 1 (failed at registration, after training completed): setup 501s
+ execution 336s ≈ 14 min on 5 VMs (`Standard_D4ds_v6`, 1 driver + 4
workers, Premium Jobs Compute). Attempt 2 (succeeded): setup 442s +
execution 428s ≈ 14.5 min on the same 5-VM shape. Combined ≈ 29 min on 5
VMs, in the same ≈$1/12-min range Task 9's own runs measured — **≈$2** of
the remaining trial credit for this whole re-run window, negligible
against the ≈$150 remaining with the Sep 24 expiry still in range.
