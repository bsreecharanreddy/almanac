"""Score the quarter with the registered champion, so risk can be ranked.

The served endpoint returns a **class**, not a score: `mlflow.lightgbm.log_model`
is given a signature inferred from `model.predict()`, so every row in Task 1's
inference table is a boolean. A boolean cannot rank an intervention queue, which
is what §5 promises and what §7's page 1 draws. This produces the score offline
instead, beside the true outcome, from the same registered champion.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from almanac.contracts import Contract
from almanac.model.dataset import build_classification_frame
from almanac.model.train import BREACH_LABEL_COLUMN, FEATURE_COLUMNS

SCORE_COLUMN = "breach_risk"

# Keyed on the PR, like every other governed surface here. `scored_at` is
# processing time and is deliberately not conflated with anything in the frame.
PREDICTIONS_CONTRACT = Contract(
    surface="pr_breach_predictions",
    columns={
        "repo_id": "bigint",
        "pr_number": "bigint",
        SCORE_COLUMN: "double",
        BREACH_LABEL_COLUMN: "boolean",
        "is_bot_author": "boolean",
    },
    keys=("repo_id", "pr_number"),
    checks={
        # A probability, not a logit and not a class. This is the whole point
        # of the table, so it is the constraint the table refuses to violate.
        "risk_is_a_probability": f"{SCORE_COLUMN} BETWEEN 0 AND 1",
    },
)


class ProbabilityModel(Protocol):
    """Anything exposing scikit-learn's `predict_proba`."""

    def predict_proba(self, features: pd.DataFrame) -> Any: ...


def positive_class_probability(model: ProbabilityModel, features: pd.DataFrame) -> pd.Series:
    """Column 1 of `predict_proba`, which is P(breach).

    Taken by position because that is scikit-learn's contract for a binary
    classifier whose classes are ordered [False, True] -- the same indexing
    `train.py` uses to compute the metrics the champion was chosen on.
    """
    probabilities = model.predict_proba(features[FEATURE_COLUMNS].astype("float64"))
    return pd.Series(probabilities[:, 1], index=features.index, dtype="float64")


def attach_scores(frame: pd.DataFrame, model: ProbabilityModel) -> pd.DataFrame:
    """The scored surface: identity, score, outcome. Pure -- no reads, no writes."""
    scored = frame.loc[:, ["repo_id", "pr_number", BREACH_LABEL_COLUMN, "is_bot_author"]].copy()
    scored[SCORE_COLUMN] = positive_class_probability(model, frame)
    scored[BREACH_LABEL_COLUMN] = scored[BREACH_LABEL_COLUMN].astype(bool)
    scored["is_bot_author"] = scored["is_bot_author"].astype(bool)
    return scored.loc[:, list(PREDICTIONS_CONTRACT.columns)]


def score_quarter(
    spark: SparkSession,
    model: ProbabilityModel,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    threshold_seconds: int,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
    gold_version: int | None = None,
) -> DataFrame:
    """Build the classification frame at pinned versions and score it.

    Same builder the champion was trained through, so the feature computation
    cannot drift between training and scoring -- the skew this project measures
    rather than assumes away.
    """
    frame = build_classification_frame(
        spark,
        silver_path=silver_path,
        features_path=features_path,
        gold_table=gold_table,
        threshold_seconds=threshold_seconds,
        silver_version=silver_version,
        features_versions=features_versions,
        gold_version=gold_version,
    )
    return spark.createDataFrame(attach_scores(frame, model))
