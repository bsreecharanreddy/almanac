# Findings — Phase 4's real cloud run: a measured null result, not a broken pipeline

**Date:** 2026-09-04
**Source:** Training run `283014697409852` (job `117899697123337`) against the
real 341M-row Q3 2025 quarter; MLflow run `fd3f138b111a419f841cb31c14861526`
(experiment `/Shared/almanac/pr-review-sla-risk`); a direct SQL query over
`almanac_dbx.gold.fact_pull_request` via the Serverless Starter Warehouse
(started for this, stopped after).
**What it closes:** Phase 4 Task 9 — the plan's committed real-cloud
verification step, run before the Sep 24 credit expiry rather than after.

---

## The measured result

| Metric | Value |
|---|---|
| `baseline_mae` (naive, median per `is_bot_author` segment) | **71,916.9 s** (≈19.98 h) |
| `model_mae` (LightGBM, default hyperparameters, 10 features) | **108,889.9 s** (≈30.25 h) |
| `beats_baseline` | **False** |

The model is real, trained on the real quarter, and measurably **worse**
than the baseline it has to beat (§5.1's gate). No model registers: the
runner's own gate is `register AND beats_baseline`, both required, and
`almanac_dbx.models.pr_review_sla_risk` does not exist — confirmed
directly (`databricks registered-models get` returns "does not exist"),
not inferred from the metric. `databricks_model_serving.pr_review_sla_risk`
is declared in Terraform for the model that does beat the baseline, but is
**not applied** — `entity_version = "1"` has nothing to point at, and
applying it would be forcing a resource around a null result rather than
recording one (§5.1, the plan's own Task 9 Step 3 instruction).

This is the outcome §5.1's whole "baseline first, always" design exists to
catch, working as designed: a model can lose to the baseline, for real, on
real data, and this run says so rather than a fixture saying it in theory.

## Why, measured rather than guessed

The trainable population's `time_to_first_response_seconds`, queried over
the same `label_exclusion IS NULL` population the training frame joins
against (`almanac_dbx.gold.fact_pull_request`, **n = 2,956,518**):

| Percentile | Seconds | |
|---|---|---|
| p50 (median) | 72 | 1.2 min |
| p75 | 1,487 | 24.8 min |
| p90 | 76,533 | 21.3 h |
| mean | 72,054 | 20.0 h |
| max | 7,921,875 | 91.7 days |

**The target is severely right-skewed**: half of all first responses land
inside two minutes, but the mean is pulled to ~20 hours by a long tail out
to three months. `baseline_mae` (71,917 s) sits almost exactly at the
**mean** (72,054 s) — consistent with a baseline that predicts something
small for the typical fast-responding PR and is then charged close to the
full actual value on every slow-tail PR, which is where MAE on this shape
of distribution is decided.

LightGBM's default objective optimizes a squared-error-like loss, which
weights the multi-day tail far more heavily than a median-based predictor
does. A regressor pulled toward the tail predicts systematically larger
values for the ~50% of PRs that actually resolve in about a minute, which
costs more in MAE across millions of fast rows than it recovers on the
tail. This is offered as the measured shape's most likely explanation, not
as a second thing quietly fixed here: §5.1's own committed discipline ("no
model ships without a measured comparison... not retried until it flips")
means this run's number stands as run, and a log-transformed target or a
different loss is the natural next iteration, deferred rather than
substituted in to change tonight's answer.

## The SLA threshold, measured (§5.2's deferred item)

**p75 = 1,487 seconds (≈25 minutes)** is the percentile recorded as the
candidate SLA breach threshold, computed once from the training
population as §5.2 committed to. Not consumed by any code in this plan —
the threshold is applied by whatever later reads a serving endpoint's raw
predicted-seconds output, which does not exist yet because no model beat
the baseline to be served.

## What Task 9 actually verified, real-cloud rather than local

- `terraform apply` created the live infrastructure this run used: the
  `almanac_dbx.burn.cluster_logs` volume, the `build_features` and
  `train_model` jobs, both now Task 9-corrected (below).
- The v1 feature tables exist on the real lake
  (`abfss://features@almanaclakekoctmh.dfs.core.windows.net/events/`),
  materialised from the real 341M-row Silver quarter.
- A real training run against real Silver + real feature tables + real
  Gold produced a real MLflow run with real, reproducible provenance:
  `sparkDatasourceInfo` on the run records the exact Delta version of every
  input (`events/clean` v91, the three feature tables v0, Gold's fact v1).
- MLflow tracking against the workspace's own backend
  (`--tracking-uri databricks`) works end to end; UC registration's
  code path is exercised by the gate (it correctly does *not* fire), not
  yet by an actual registration (deferred until a model earns one).
- Two real, real-scale defects were found and fixed along the way, both
  the same shape and both load-bearing corrections to the Phase 3 feature
  platform, not to this run's numbers:
  `docs/findings/2026-09-04-author-activity-self-join.md` (the
  self-join's O(N²) bot-author blow-up, and the same shape in
  `as_of_join` itself) and
  `docs/findings/2026-09-04-gold-is-a-metastore-table-not-a-path.md`
  (a local-vs-cloud divergence in where dbt materialises Gold).
- Cost, cluster changes, and the config correction they justify are all
  in the verification log.

## Cost of the whole Task 9 window

Three cancelled feature-build runs (≈$13), one cancelled single-node
training run (102 min, ≈$0.30), one successful feature-build run (14 min
on 5 VMs, ≈$1), one successful multi-node training run (12 min on 5 VMs,
≈$1). **Total ≈$15** of the ≈$150 remaining trial credit, with the Sep 24
expiry still 20 days out.
