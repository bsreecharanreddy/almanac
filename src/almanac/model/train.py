"""Trains the SLA-risk regressor and measures it against the naive
baseline -- 'no model ships without a measured comparison' (design doc
§5.1), enforced here in code rather than left to a checklist step.
"""

from dataclasses import dataclass
from typing import Any

import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

from almanac.model.baseline import NaiveBaseline, fit_naive_baseline

LABEL_COLUMN = "time_to_first_response_seconds"
SEGMENT_COLUMN = "is_bot_author"

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


@dataclass(frozen=True)
class TrainResult:
    model: LGBMRegressor
    baseline: NaiveBaseline
    model_mae: float
    baseline_mae: float
    beats_baseline: bool


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
    model.fit(train[FEATURE_COLUMNS].astype("float64"), train[LABEL_COLUMN])
    model_predictions = model.predict(test[FEATURE_COLUMNS].astype("float64"))
    model_mae = float(mean_absolute_error(test[LABEL_COLUMN], model_predictions))

    return TrainResult(
        model=model,
        baseline=baseline,
        model_mae=model_mae,
        baseline_mae=baseline_mae,
        beats_baseline=model_mae < baseline_mae,
    )
