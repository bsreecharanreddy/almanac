"""Per-feature contributions behind a prediction, so an explanation can cite a computed number."""

from __future__ import annotations

from typing import Any, Protocol

import pandas as pd

from almanac.model.train import FEATURE_COLUMNS

BASELINE_COLUMN = "expected_value"


class ContributionModel(Protocol):
    """Anything exposing LightGBM's `predict(..., pred_contrib=True)`."""

    def predict(self, features: pd.DataFrame, **kwargs: Any) -> Any: ...


def contribution_frame(model: ContributionModel, features: pd.DataFrame) -> pd.DataFrame:
    """Per-feature contributions beside the baseline they move from. Pure -- no I/O."""
    contributed = model.predict(features[FEATURE_COLUMNS].astype("float64"), pred_contrib=True)
    # LightGBM returns n_features + 1 columns and the last is the expected value,
    # NOT a feature -- its documented departure from the shap package. Naming the
    # columns here is what stops that becoming a silent off-by-one downstream.
    width = len(FEATURE_COLUMNS) + 1
    if contributed.shape[1] != width:
        raise ValueError(
            f"expected {width} contribution columns ({len(FEATURE_COLUMNS)} features "
            f"plus the expected value), got {contributed.shape[1]} -- a multiclass "
            "booster returns one such block per class and must not be sliced blindly"
        )
    return pd.DataFrame(
        contributed, columns=[*FEATURE_COLUMNS, BASELINE_COLUMN], index=features.index
    )
