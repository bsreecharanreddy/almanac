"""predict: the champion's score, or a structured refusal -- never a number
in place of one it cannot honestly produce (design doc S4.2).

Runs against the real Delta tables `tests/agent_fixtures.py` lands rather
than a stub feature frame: the drift check runs on what `get_features`
actually returns, and a stub would only encode the belief this test exists
to check.
"""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from almanac.agent.schemas import PredictInput, PredictResult, Refusal
from almanac.agent.tools import predict
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


class _StubModel:
    """A fixed P(breach) = 0.7 for every row -- `model/score.py`'s
    `ProbabilityModel` Protocol, injected so no endpoint is called (Task 11
    is the first billable step, per the plan).
    """

    def predict_proba(self, features: pd.DataFrame) -> Any:
        return np.tile([0.3, 0.7], (len(features), 1))


def _land(spark: SparkSession, tmp_path: Path) -> tuple[str, str]:
    return land_silver_and_features(spark, silver_frame(spark, TWO_ERA_EVENTS), tmp_path)


def test_a_rich_era_window_returns_a_score(spark: SparkSession, tmp_path: Path) -> None:
    silver_path, features_path = _land(spark, tmp_path)

    result = predict(
        spark,
        _StubModel(),
        PredictInput(entity=RICH_ENTITY, as_of=RICH_AS_OF),
        model_version="test-1",
        silver_path=silver_path,
        features_path=features_path,
    )

    assert isinstance(result, PredictResult)
    assert result.breach_risk == 0.7
    assert result.provenance.model_version == "test-1"


def test_a_reduced_era_window_returns_a_structured_refusal_naming_the_feature(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path = _land(spark, tmp_path)

    result = predict(
        spark,
        _StubModel(),
        PredictInput(entity=REDUCED_ENTITY, as_of=REDUCED_AS_OF),
        model_version="test-1",
        silver_path=silver_path,
        features_path=features_path,
    )

    assert isinstance(result, Refusal)
    assert result.missing_feature == "is_draft"
    assert "cannot score" in result.reason
    assert "breach_risk" not in type(result).model_fields
