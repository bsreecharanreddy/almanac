"""Trains the SLA-risk regressor, measures it against the naive baseline
-- 'no model ships without a measured comparison' (design doc §5.1),
enforced here in code -- and logs the run to MLflow.
"""

from dataclasses import dataclass
from typing import Any

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from mlflow.models import ModelSignature, infer_signature
from sklearn.metrics import average_precision_score, log_loss, mean_absolute_error, roc_auc_score
from sklearn.model_selection import train_test_split

from almanac.model.baseline import NaiveBaseline, fit_naive_baseline

LABEL_COLUMN = "time_to_first_response_seconds"
SEGMENT_COLUMN = "is_bot_author"
BREACH_LABEL_COLUMN = "breach"

# Named LGBMClassifier configs tried in one run (design doc §5.3): plain
# defaults, and LightGBM's own answer to the measured 25.55%/74.45% class
# split. A comparison table, not one shot.
CLASSIFIER_CANDIDATES: dict[str, dict[str, Any]] = {
    "default": {},
    "is_unbalance": {"is_unbalance": True},
}

# The columns assemble_training_set() + join_label() produce, minus
# identifiers (repo_id, pr_number, author_login, as_of_timestamp) and the
# label itself. Stated once: runner.py (Task 6) logs this same list as an
# MLflow param rather than re-deriving or re-typing it.
FEATURE_COLUMNS: list[str] = [
    "prior_pr_count",
    "prior_merge_rate",
    "events_total_to_date",
    "bot_events_to_date",
    "prs_opened_to_date",
    "bot_share_to_date",
    "is_draft",
    "is_bot_author",
    "opened_day_of_week",
    "opened_hour",
]

# compute_pr_similarity's own output columns (design doc §8.3a). Not folded
# into FEATURE_COLUMNS itself: whether they earn a place is Task 6's own
# question, decided by comparing a with/without run against the same
# registered champion, not assumed by shipping them unconditionally.
SIMILARITY_FEATURE_COLUMNS: list[str] = [
    "similar_neighbor_count",
    "similar_prior_breach_rate",
]


@dataclass(frozen=True)
class TrainResult:
    model: LGBMRegressor
    baseline: NaiveBaseline
    model_mae: float
    baseline_mae: float
    beats_baseline: bool
    signature: ModelSignature


def train_model(
    frame: pd.DataFrame,
    *,
    random_state: int = 42,
    test_size: float = 0.3,
    **lgbm_params: Any,
) -> TrainResult:
    """Fit LightGBM, fit the baseline on the same split, and compare their MAE."""
    train, test = train_test_split(frame, test_size=test_size, random_state=random_state)

    baseline = fit_naive_baseline(train, label_col=LABEL_COLUMN, segment_col=SEGMENT_COLUMN)
    baseline_predictions = baseline.predict(test[[SEGMENT_COLUMN]])
    baseline_mae = float(mean_absolute_error(test[LABEL_COLUMN], baseline_predictions))

    model = LGBMRegressor(random_state=random_state, verbosity=-1, **lgbm_params)
    train_features = train[FEATURE_COLUMNS].astype("float64")
    model.fit(train_features, train[LABEL_COLUMN])
    model_predictions = model.predict(test[FEATURE_COLUMNS].astype("float64"))
    model_mae = float(mean_absolute_error(test[LABEL_COLUMN], model_predictions))

    return TrainResult(
        model=model,
        baseline=baseline,
        model_mae=model_mae,
        baseline_mae=baseline_mae,
        beats_baseline=model_mae < baseline_mae,
        # Unity Catalog refuses to register a model with no signature --
        # measured for real against the live workspace (Task 13's cloud
        # run); §5.1's regression path never registered a model before
        # now, so this never fired until the classification path did.
        signature=infer_signature(train_features, np.asarray(model.predict(train_features))),
    )


@dataclass(frozen=True)
class ClassifierCandidateResult:
    model: LGBMClassifier
    roc_auc: float
    average_precision: float
    log_loss: float
    signature: ModelSignature


@dataclass(frozen=True)
class ClassificationResult:
    candidates: dict[str, ClassifierCandidateResult]
    baseline: NaiveBaseline
    baseline_average_precision: float
    best_candidate: str
    beats_baseline: bool
    feature_columns: list[str]


def _best_candidate(candidates: dict[str, ClassifierCandidateResult]) -> str:
    """Highest average precision (PR-AUC) wins -- higher is better, the
    inverse direction of train_model's lower-is-better MAE gate.
    """
    return max(candidates, key=lambda name: candidates[name].average_precision)


def train_classifier(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str] | None = None,
    random_state: int = 42,
    test_size: float = 0.3,
) -> ClassificationResult:
    """Fit the breach-rate baseline and every `CLASSIFIER_CANDIDATES`
    config on the same split; compare by average precision (design doc
    §5.3 -- PR-AUC over ROC-AUC on the measured 25.55%-positive target).

    `feature_columns` defaults to `FEATURE_COLUMNS`; Task 6 (§8.3a) passes
    `FEATURE_COLUMNS + SIMILARITY_FEATURE_COLUMNS` for the with-similarity
    arm of the same comparison, same CLASSIFIER_CANDIDATES sweep, same split.
    """
    columns = feature_columns if feature_columns is not None else FEATURE_COLUMNS
    train, test = train_test_split(frame, test_size=test_size, random_state=random_state)

    baseline = fit_naive_baseline(
        train, label_col=BREACH_LABEL_COLUMN, segment_col=SEGMENT_COLUMN, agg="mean"
    )
    baseline_predictions = baseline.predict(test[[SEGMENT_COLUMN]])
    baseline_average_precision = float(
        average_precision_score(test[BREACH_LABEL_COLUMN], baseline_predictions)
    )

    candidates: dict[str, ClassifierCandidateResult] = {}
    for name, params in CLASSIFIER_CANDIDATES.items():
        model = LGBMClassifier(random_state=random_state, verbosity=-1, **params)
        train_features = train[columns].astype("float64")
        model.fit(train_features, train[BREACH_LABEL_COLUMN])
        probabilities = np.asarray(model.predict_proba(test[columns].astype("float64")))
        predicted = probabilities[:, 1]
        candidates[name] = ClassifierCandidateResult(
            model=model,
            roc_auc=float(roc_auc_score(test[BREACH_LABEL_COLUMN], predicted)),
            average_precision=float(average_precision_score(test[BREACH_LABEL_COLUMN], predicted)),
            log_loss=float(log_loss(test[BREACH_LABEL_COLUMN], predicted)),
            signature=infer_signature(train_features, np.asarray(model.predict(train_features))),
        )

    best_candidate = _best_candidate(candidates)
    return ClassificationResult(
        candidates=candidates,
        baseline=baseline,
        baseline_average_precision=baseline_average_precision,
        best_candidate=best_candidate,
        beats_baseline=candidates[best_candidate].average_precision > baseline_average_precision,
        feature_columns=columns,
    )


def log_classification_run(
    result: ClassificationResult, *, experiment_name: str, tracking_uri: str
) -> dict[str, str]:
    """Log every candidate, plus the baseline, as its own MLflow run in the
    same experiment -- a real comparison table, not one number standing in
    for "the model". Returns {candidate_name: model_uri} so the runner can
    register whichever won.
    """
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="baseline"):
        mlflow.log_param("feature_columns", result.feature_columns)
        mlflow.log_metric("average_precision", result.baseline_average_precision)

    model_uris: dict[str, str] = {}
    for name, candidate in result.candidates.items():
        with mlflow.start_run(run_name=name) as run:
            mlflow.log_param("feature_columns", result.feature_columns)
            mlflow.log_param("candidate", name)
            mlflow.log_param("random_state", candidate.model.get_params()["random_state"])
            mlflow.log_metric("roc_auc", candidate.roc_auc)
            mlflow.log_metric("average_precision", candidate.average_precision)
            mlflow.log_metric("log_loss", candidate.log_loss)
            mlflow.log_metric("baseline_average_precision", result.baseline_average_precision)
            mlflow.log_metric(
                "beats_baseline",
                float(candidate.average_precision > result.baseline_average_precision),
            )
            mlflow.log_metric("is_best_candidate", float(name == result.best_candidate))
            mlflow.lightgbm.log_model(candidate.model, name="model", signature=candidate.signature)
            model_uris[name] = f"runs:/{run.info.run_id}/model"

    return model_uris


def log_training_run(result: TrainResult, *, experiment_name: str, tracking_uri: str) -> str:
    """Log params, metrics, and the model artifact; return the run's model URI."""
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run() as run:
        mlflow.log_param("feature_columns", FEATURE_COLUMNS)
        mlflow.log_param("random_state", result.model.get_params()["random_state"])
        mlflow.log_metric("model_mae", result.model_mae)
        mlflow.log_metric("baseline_mae", result.baseline_mae)
        mlflow.log_metric("beats_baseline", float(result.beats_baseline))
        mlflow.lightgbm.log_model(result.model, name="model", signature=result.signature)
        return f"runs:/{run.info.run_id}/model"
