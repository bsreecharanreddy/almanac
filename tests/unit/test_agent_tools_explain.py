"""explain: the champion's own per-feature contributions, or nothing at all.

Two things are under test that `test_model_contributions.py` cannot cover,
because they live at the tool boundary rather than in the pure function:
that the numbers returned are Task 1's numbers rather than a parallel
computation, and that a window `predict` refuses is refused here too --
LightGBM turns a null into NaN and still emits a contribution for it, so
without the same gate this tool would publish a number for a feature the
window does not contain.
"""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from lightgbm import LGBMClassifier
from pyspark.sql import SparkSession

from almanac.agent.schemas import ExplainInput, ExplainResult, GetFeaturesInput, Refusal
from almanac.agent.tools import explain, get_features
from almanac.model.contributions import BASELINE_COLUMN, contribution_frame
from almanac.model.train import FEATURE_COLUMNS
from tests.agent_fixtures import (
    REDUCED_AS_OF,
    REDUCED_ENTITY,
    RICH_AS_OF,
    RICH_ENTITY,
    TWO_ERA_EVENTS,
    land_silver_and_features,
    silver_frame,
)

pytestmark = pytest.mark.spark

# Distinct magnitudes and mixed signs, so an ordering assertion names one
# unambiguous answer. Read in FEATURE_COLUMNS order; the last value is the
# baseline, per LightGBM's convention.
_STUB_ROW = [0.9, -0.7, 0.5, -0.3, 0.2, -0.15, 0.1, -0.08, 0.05, -0.01, 0.25]
_STRONGEST_THREE = ["prior_pr_count", "prior_merge_rate", "events_total_to_date"]


class _StubContributor:
    """A fixed contribution row for every input -- `contributions.py`'s
    `ContributionModel` Protocol, so the ordering under test is the tool's
    and not a booster's.
    """

    def __init__(self, row: list[float] | None = None) -> None:
        self._row = row if row is not None else _STUB_ROW

    def predict(self, features: pd.DataFrame, **kwargs: Any) -> np.ndarray:
        return np.tile(self._row, (len(features), 1))


def _trained_booster() -> LGBMClassifier:
    """A real booster, so the column convention Task 1 guards is exercised
    end to end. Small-data settings, not tuning: LightGBM's defaults refuse
    to split at this size and every contribution would be zero.
    """
    rng = np.random.default_rng(0)
    rows = 200
    frame = pd.DataFrame({column: rng.normal(size=rows) for column in FEATURE_COLUMNS})
    frame["prior_pr_count"] = np.arange(rows, dtype="float64")
    model = LGBMClassifier(
        n_estimators=20, min_child_samples=1, min_data_in_bin=1, verbosity=-1, random_state=0
    )
    model.fit(frame[FEATURE_COLUMNS], frame["prior_pr_count"] > rows / 2)
    return model


def _land(spark: SparkSession, tmp_path: Path) -> tuple[str, str]:
    return land_silver_and_features(spark, silver_frame(spark, TWO_ERA_EVENTS), tmp_path)


def _explain(
    spark: SparkSession, model: Any, paths: tuple[str, str], *, top_k: int = 5
) -> ExplainResult | Refusal:
    silver_path, features_path = paths
    return explain(
        spark,
        model,
        ExplainInput(entity=RICH_ENTITY, as_of=RICH_AS_OF, top_k=top_k),
        model_version="test-1",
        silver_path=silver_path,
        features_path=features_path,
    )


def test_every_contribution_returned_is_task_1s_own_number(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The tool must not compute contributions by any path but Task 1's.

    Asserted against `contribution_frame` called directly on the vector
    `get_features` returns, so an off-by-one in the tool's column handling
    moves every number and shows here.
    """
    paths = _land(spark, tmp_path)
    model = _trained_booster()
    fetched = get_features(
        spark,
        GetFeaturesInput(entity=RICH_ENTITY, as_of=RICH_AS_OF),
        silver_path=paths[0],
        features_path=paths[1],
    )
    expected = contribution_frame(model, pd.DataFrame([fetched.features])).iloc[0]

    result = _explain(spark, model, paths, top_k=len(FEATURE_COLUMNS))

    assert isinstance(result, ExplainResult)
    assert result.baseline == pytest.approx(expected[BASELINE_COLUMN])
    assert {c.feature for c in result.contributions} == set(FEATURE_COLUMNS)
    for contribution in result.contributions:
        assert contribution.contribution == pytest.approx(expected[contribution.feature])


def test_contributions_are_sorted_by_magnitude_and_truncated_to_an_explicit_k(
    spark: SparkSession, tmp_path: Path
) -> None:
    """ "Top" means largest pull in either direction, and `top_k` is reported
    rather than left for a caller to infer from the row count.
    """
    paths = _land(spark, tmp_path)

    result = _explain(spark, _StubContributor(), paths, top_k=3)

    assert isinstance(result, ExplainResult)
    assert [c.feature for c in result.contributions] == _STRONGEST_THREE
    assert result.top_k == 3


def test_direction_follows_the_sign_of_the_contribution(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The stub row mixes signs, so both directions are exercised. Inverting
    the mapping raises in `Contribution` itself rather than returning a
    contradiction, which is what Phase 10's directional check inherits.
    """
    paths = _land(spark, tmp_path)

    result = _explain(spark, _StubContributor(), paths, top_k=len(FEATURE_COLUMNS))

    assert isinstance(result, ExplainResult)
    directions = {c.feature: c.direction for c in result.contributions}
    assert directions["prior_pr_count"] == "increases_risk"
    assert directions["prior_merge_rate"] == "decreases_risk"


def test_equal_magnitudes_break_ties_by_feature_name(spark: SparkSession, tmp_path: Path) -> None:
    """Phase 10 replays recorded transcripts, so a tie must not order itself
    by whatever `sorted` last saw.
    """
    paths = _land(spark, tmp_path)
    tied = _StubContributor([0.5] * len(FEATURE_COLUMNS) + [0.0])

    result = _explain(spark, tied, paths, top_k=3)

    assert isinstance(result, ExplainResult)
    assert [c.feature for c in result.contributions] == sorted(FEATURE_COLUMNS)[:3]


def test_a_top_k_larger_than_the_feature_set_returns_every_feature(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Asking for more than exists is not an error; it is every feature."""
    paths = _land(spark, tmp_path)

    result = _explain(spark, _StubContributor(), paths, top_k=len(FEATURE_COLUMNS) + 5)

    assert isinstance(result, ExplainResult)
    assert len(result.contributions) == len(FEATURE_COLUMNS)


def test_a_reduced_era_window_is_refused_rather_than_explained(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The same gate `predict` sits behind. Without it LightGBM would emit a
    contribution for `is_draft` from a NaN, and Phase 10's verifier would
    read that fabricated number as grounded.
    """
    silver_path, features_path = _land(spark, tmp_path)

    result = explain(
        spark,
        _StubContributor(),
        ExplainInput(entity=REDUCED_ENTITY, as_of=REDUCED_AS_OF),
        model_version="test-1",
        silver_path=silver_path,
        features_path=features_path,
    )

    assert isinstance(result, Refusal)
    assert result.missing_feature == "is_draft"
    assert "cannot score" in result.reason
    assert "contributions" not in type(result).model_fields


def test_provenance_names_the_model_version_and_the_read_delta_versions(
    spark: SparkSession, tmp_path: Path
) -> None:
    """An explanation is traceable to the byte-level state that produced it."""
    paths = _land(spark, tmp_path)

    result = _explain(spark, _StubContributor(), paths)

    assert isinstance(result, ExplainResult)
    assert result.provenance.model_version == "test-1"
    assert result.provenance.read_delta_versions == {
        "events": 0,
        "author_activity": 0,
        "repo_activity": 0,
        "pr_static": 0,
    }
