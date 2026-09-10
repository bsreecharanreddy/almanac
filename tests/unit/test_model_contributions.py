"""Per-feature contributions: the computed numbers an explanation is allowed to cite.

Deliberately tested against a **real** `LGBMClassifier` rather than a stub. The
thing under test is LightGBM's own column convention -- `pred_contrib=True`
returns `n_features + 1` columns whose last one is the expected value, not a
feature -- and a stub would only encode the belief this test exists to falsify.
"""

from typing import Any

import numpy as np
import pandas as pd
import pytest
from lightgbm import LGBMClassifier

from almanac.model.contributions import BASELINE_COLUMN, contribution_frame
from almanac.model.train import FEATURE_COLUMNS

ROWS = 200


def features(rows: int = ROWS) -> pd.DataFrame:
    """`prior_pr_count` alone decides the label; every other feature is noise."""
    rng = np.random.default_rng(0)
    data = {column: rng.normal(size=rows) for column in FEATURE_COLUMNS}
    data["prior_pr_count"] = np.arange(rows, dtype="float64")
    return pd.DataFrame(data)[FEATURE_COLUMNS]


def trained() -> tuple[LGBMClassifier, pd.DataFrame]:
    frame = features()
    labels = frame["prior_pr_count"] > ROWS / 2
    # Small-data settings, not tuning: LightGBM's defaults refuse to split at
    # this size and every contribution would be zero, which passes vacuously.
    model = LGBMClassifier(
        n_estimators=20, min_child_samples=1, min_data_in_bin=1, verbosity=-1, random_state=0
    )
    model.fit(frame, labels)
    return model, frame


class StubContributor:
    """Returns a fixed-width contribution matrix, like LightGBM's raw output."""

    def __init__(self, width: int) -> None:
        self._width = width
        self.seen_columns: list[str] | None = None

    def predict(self, features: pd.DataFrame, **kwargs: Any) -> np.ndarray:
        self.seen_columns = list(features.columns)
        return np.zeros((len(features), self._width))


def test_contributions_and_baseline_sum_to_the_raw_score() -> None:
    """The property that catches an off-by-one without hard-coding a position.

    Treating the expected value as a feature, or dropping it, breaks this sum.
    """
    model, frame = trained()

    contributions = contribution_frame(model, frame)

    total = contributions[FEATURE_COLUMNS].sum(axis=1) + contributions[BASELINE_COLUMN]
    assert np.allclose(total, model.predict(frame, raw_score=True))


def test_the_dominant_feature_carries_the_largest_contribution() -> None:
    """Sums are order-invariant, so the test above survives a column shuffle.

    This one does not: the label is a function of `prior_pr_count` alone, so a
    mislabelled column moves the largest contribution off it.
    """
    model, frame = trained()

    contributions = contribution_frame(model, frame)

    largest = contributions[FEATURE_COLUMNS].abs().mean().idxmax()
    assert largest == "prior_pr_count"


def test_the_baseline_is_one_column_and_no_feature_is_lost() -> None:
    """`n_features + 1` in, `n_features + 1` out, named rather than positional."""
    model, frame = trained()

    contributions = contribution_frame(model, frame)

    assert list(contributions.columns) == [*FEATURE_COLUMNS, BASELINE_COLUMN]
    assert contributions[BASELINE_COLUMN].nunique() == 1


def test_an_unexpected_width_is_refused_rather_than_silently_realigned() -> None:
    """A multiclass booster returns `(n_features + 1) * n_classes` columns.

    Slicing that to the first `n_features` yields plausible numbers for the
    wrong class, which is the failure this guard exists to make loud.
    """
    model = StubContributor(width=len(FEATURE_COLUMNS) * 2 + 2)

    with pytest.raises(ValueError, match="contribution columns"):
        contribution_frame(model, features(rows=3))


def test_only_the_contracted_feature_columns_reach_the_model() -> None:
    """Training used FEATURE_COLUMNS; explaining on a different set is skew."""
    model = StubContributor(width=len(FEATURE_COLUMNS) + 1)
    frame = features(rows=3)
    frame["not_a_feature"] = 1.0

    contribution_frame(model, frame)

    assert model.seen_columns == FEATURE_COLUMNS


def test_an_empty_frame_keeps_its_shape() -> None:
    """No rows is not an error; it is a window with nothing in it."""
    model = StubContributor(width=len(FEATURE_COLUMNS) + 1)

    contributions = contribution_frame(model, features(rows=0))

    assert list(contributions.columns) == [*FEATURE_COLUMNS, BASELINE_COLUMN]
    assert len(contributions) == 0
