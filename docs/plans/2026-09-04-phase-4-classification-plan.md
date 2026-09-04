# Phase 4 continued: the classification path (implementation plan)

Written from design doc §5.3 (`docs/design/2026-09-01-almanac-system-design.md`),
which has the full evidence and decisions this plan just wires into code.
Extends `docs/plans/2026-09-03-phase-4-model-mlflow-plan.md` (Tasks 1–9,
done) rather than replacing it — same phase, same product question, same
registered model name. Tasks 10–13 below; the existing plan's Task 10
("Wrap-up") becomes Task 14 and now covers both the regression and
classification results.

Written and executed in one continuous session rather than plan-then-
separately-reviewed-then-executed, unlike Tasks 1–9 — leaner per-task
detail than that plan (interfaces and test intent, not full inline code)
since there is no independent review pass between writing this and
implementing it. TDD discipline, `make check` before every commit, and a
STATUS.md verification-log row per task stay unchanged.

## Task 10: `join_breach_label`, and a shared feature-frame builder

**Files:**
- Modify: `src/almanac/model/dataset.py`
- Test: `tests/unit/test_model_dataset.py`, `tests/integration/test_model_dataset_versions.py` (extend both)

**Interfaces:**
- `_build_feature_frame(spark, *, silver_path, features_path, silver_version=None, features_version=None) -> DataFrame` — extracted from `build_training_frame`: Silver → spine → `assemble_training_set`, no label. `build_training_frame` calls it and is otherwise unchanged (existing tests must stay green untouched).
- `join_breach_label(training_frame: DataFrame, fact_pull_request: DataFrame, *, threshold_seconds: int) -> DataFrame` — the §5.3 population and derivation: inner-join on `(repo_id, pr_number)` against rows where `label_exclusion IS NULL OR label_exclusion = 'closed_no_response'`, deriving `breach` per §5.3's two-branch rule (`time_to_first_response_seconds > threshold_seconds` for the trainable rows; `(closed_at - opened_at) >= threshold_seconds` for `closed_no_response`).
- `build_classification_frame(spark, *, silver_path, features_path, gold_table, threshold_seconds, silver_version=None, features_version=None, gold_version=None) -> pd.DataFrame` — same shape as `build_training_frame`, using `_build_feature_frame` + `join_breach_label`.

**Tests to write:** a fixture exercising all three population rows —
trainable-and-breach, trainable-and-not, `closed_no_response`-open-long-
enough (breach), `closed_no_response`-closed-fast (not a breach) — plus
one row each of `author_unobserved`/`right_censored`/`draft` proving they
are excluded. Extend the existing version-pinning integration test's
pattern for `build_classification_frame`.

## Task 11: The breach-rate baseline

**Files:**
- Modify: `src/almanac/model/baseline.py`
- Test: `tests/unit/test_model_baseline.py` (extend)

**Interfaces:**
- `fit_naive_baseline(frame, *, label_col=..., segment_col=..., agg: Literal["median", "mean"] = "median") -> NaiveBaseline` — `agg` selects the per-segment statistic; default preserves every existing call site and test exactly. `NaiveBaseline`'s fields generalize from `medians`/`overall_median` to `values`/`overall_value` (nothing outside `baseline.py` touches the field names — checked directly, not assumed).
- Classification baseline call site: `fit_naive_baseline(train, label_col="breach", segment_col="is_bot_author", agg="mean")` — the segment's breach *rate*, used as `NaiveBaseline.predict`'s output (a probability, unchanged `predict` logic).

**Tests to write:** `agg="mean"` on a small breach/no-breach fixture,
segment rate and the unseen-segment fallback (mirrors the two existing
median tests exactly, `agg="mean"`); a regression test that `agg`
defaulting to `"median"` reproduces the two existing tests' results
unchanged.

## Task 12: `train_classifier` — the comparison sweep and the PR-AUC gate

**Files:**
- Modify: `src/almanac/model/train.py`
- Test: `tests/unit/test_model_train.py`, `tests/unit/test_model_train_logging.py` (extend both)

**Interfaces:**
- `CLASSIFIER_CANDIDATES: dict[str, dict[str, Any]]` — named `LGBMClassifier` config sweep, at least `"default"` (`{}`) and `"is_unbalance"` (`{"is_unbalance": True}`), per §5.3.
- `ClassificationResult` (frozen dataclass): `candidates: dict[str, ClassifierCandidateResult]` (model, `roc_auc`, `average_precision`, `log_loss`), `baseline: NaiveBaseline`, `baseline_average_precision: float`, `best_candidate: str`, `beats_baseline: bool` — `best_candidate` is whichever scores highest `average_precision`; `beats_baseline` compares it against the baseline's, higher-is-better (the inverse direction of `TrainResult`'s `beats_baseline`, stated explicitly in the docstring so the sign doesn't get read backwards).
- `train_classifier(frame, *, random_state=42, test_size=0.3) -> ClassificationResult` — splits once, fits the baseline and every candidate on the same split (so the comparison is apples-to-apples), scores each with `roc_auc_score`/`average_precision_score`/`log_loss` (scikit-learn, already a pinned dependency).
- `log_classification_run(result, *, experiment_name, tracking_uri) -> dict[str, str]` — one MLflow run per candidate **plus** one for the baseline, all in the same experiment, params/metrics for each; returns `{candidate_name: model_uri}` for the runner to register the winner from.

**Tests to write:** a synthetic frame with a real signal proving at least
one candidate beats the baseline (mirrors Task 4's own no-signal-passes-
first-try note — small enough to need a fixture, not a network call);
`best_candidate` picks the higher-`average_precision` of two candidates
with deliberately different scores; the `agg="mean"` baseline call site
proven end to end.

## Task 13: Runner/CLI wiring and Terraform

**Files:**
- Modify: `src/almanac/model/runner.py`, `infra/terraform/databricks.tf`, `infra/terraform/variables.tf`
- Test: `tests/unit/test_model_runner_cli.py`, `tests/integration/test_model_runner.py` (extend both)

**Interfaces:**
- `run_training(..., objective: Literal["regression", "classification"] = "regression", threshold_seconds: int | None = None)` — `objective="regression"` is the exact existing path (default, so every current call site and test is untouched); `"classification"` calls `build_classification_frame` (needs `threshold_seconds`, required together — enforced with a guard clause, not silently defaulted) → `train_classifier` → `log_classification_run` → registers `best_candidate`'s model under the **same** `pr_review_sla_risk` name if `beats_baseline`.
- CLI: `--objective {regression,classification}` (default `regression`), `--threshold-seconds` (required when `--objective classification`, enforced by `_build_parser`).
- Terraform: `train_model`'s `parameters` gain `--objective classification --threshold-seconds 1487` — **1487 is the §5.3-measured constant, passed explicitly, never recomputed by the job** (recomputing it live would silently change "the SLA" run to run). Existing `--catalog`/`--schema`/`--register` unchanged. No new job, no new cluster — same `databricks_job.train_model`, since this supersedes what runs there, not adds a parallel pipeline.

**Tests to write:** `_build_parser` enforces `--threshold-seconds` only
required with `--objective classification` (`SystemExit` otherwise);
`run_training(objective="classification", threshold_seconds=None)` raises
before touching Spark; a real integration run through `main()` with
`--objective classification` against a small fixture, real MLflow, no
mock — matching every other runner test in this plan.

## Task 14 (was Task 10): Wrap-up — exit gate, README, STATUS.md

Unchanged in shape from the original plan's Task 10, extended to cover
both results: the exit gate table gets one new row (classification's
PR-AUC gate, mirroring the existing MAE-gate row), the README status
block states both the regression null result and the classification
outcome — whichever it measures to be — and STATUS.md's verification log
gets Task 10–13's rows plus this closing one. Runs after Task 13's real
cloud step, not before, so it reports the actual measured outcome rather
than a plan.

## Exit gate additions (this plan's own, on top of the original)

| Gate | How it's verified |
|---|---|
| The classification population and derivation match §5.3 exactly | Task 10's fixture, covering every `label_exclusion` branch |
| The SLA threshold is passed explicitly, never recomputed by the job | Task 13's CLI test + the Terraform parameter itself |
| No classifier ships without beating a measured, segment-rate baseline | Task 12's `beats_baseline` gate + its directional test |
| The comparison is a real sweep, not one config | `ClassificationResult.candidates` has ≥2 entries, each independently scored and logged |
| Registration still requires `register AND beats_baseline` | Task 13, same code shape as the regression path |

## Deferred, carried from §5.3

`right_censored` inclusion via a survival-analysis-shaped label;
per-segment or per-repo thresholds; probability calibration; the
`@challenger` retraining workflow. None of this plan's tasks touch them.
