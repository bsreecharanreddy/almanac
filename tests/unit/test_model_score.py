"""Batch scoring: a real probability beside the true outcome, under a contract.

No SparkSession -- `attach_scores` is pure pandas, which is what makes the
scoring surface testable without a cluster.
"""

import numpy as np
import pandas as pd

from almanac.contracts import Contract, check_expressions
from almanac.model.score import (
    PREDICTIONS_CONTRACT,
    SCORE_COLUMN,
    attach_scores,
    positive_class_probability,
)
from almanac.model.train import BREACH_LABEL_COLUMN, FEATURE_COLUMNS


class StubClassifier:
    """Returns a fixed [P(no breach), P(breach)] per row, like scikit-learn."""

    def __init__(self, breach_probabilities: list[float]) -> None:
        self._p = breach_probabilities
        self.seen_columns: list[str] | None = None

    def predict_proba(self, features: pd.DataFrame) -> np.ndarray:
        self.seen_columns = list(features.columns)
        return np.array([[1 - p, p] for p in self._p])


def frame(rows: int = 2) -> pd.DataFrame:
    data = {column: [0.5] * rows for column in FEATURE_COLUMNS}
    data.update(
        {
            "repo_id": list(range(1, rows + 1)),
            "pr_number": [10] * rows,
            BREACH_LABEL_COLUMN: [i % 2 == 0 for i in range(rows)],
            "is_bot_author": [False] * rows,
        }
    )
    # `is_bot_author` is already a FEATURE_COLUMN as well as an output column;
    # it is the segment the model splits on and the segment the page filters by.
    return pd.DataFrame(data)[[*FEATURE_COLUMNS, "repo_id", "pr_number", BREACH_LABEL_COLUMN]]


def test_the_positive_class_column_is_taken_by_position() -> None:
    """Column 1 is P(breach). Taking column 0 would invert every ranking on the
    page and still produce plausible-looking probabilities.
    """
    scores = positive_class_probability(StubClassifier([0.9, 0.1]), frame())

    assert list(scores) == [0.9, 0.1]


def test_only_the_contracted_feature_columns_reach_the_model() -> None:
    """Training used FEATURE_COLUMNS; scoring on a different set is skew."""
    model = StubClassifier([0.5, 0.5])

    positive_class_probability(model, frame())

    assert model.seen_columns == FEATURE_COLUMNS


def test_the_scored_surface_carries_score_beside_outcome() -> None:
    scored = attach_scores(frame(), StubClassifier([0.8, 0.2]))

    assert list(scored.columns) == list(PREDICTIONS_CONTRACT.columns)
    assert list(scored[SCORE_COLUMN]) == [0.8, 0.2]
    assert list(scored[BREACH_LABEL_COLUMN]) == [True, False]


def test_the_scored_surface_matches_its_own_contract() -> None:
    """The contract is the published promise; a drift here is a silent break."""
    scored = attach_scores(frame(), StubClassifier([0.8, 0.2]))

    assert set(scored.columns) == set(PREDICTIONS_CONTRACT.columns)
    assert PREDICTIONS_CONTRACT.keys == ("repo_id", "pr_number")


def test_the_contract_refuses_a_score_that_is_not_a_probability() -> None:
    """A logit or a raw margin would rank fine and calibrate meaninglessly, so
    the table itself refuses it rather than trusting the caller.
    """
    rules = check_expressions(PREDICTIONS_CONTRACT)

    assert rules["risk_is_a_probability"] == f"{SCORE_COLUMN} BETWEEN 0 AND 1"
    assert "repo_id_not_null" in rules and "pr_number_not_null" in rules


def test_a_contract_is_what_the_predictions_table_declares() -> None:
    assert isinstance(PREDICTIONS_CONTRACT, Contract)
    assert PREDICTIONS_CONTRACT.columns[SCORE_COLUMN] == "double"
