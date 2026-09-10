"""predict: the champion's score, or a structured refusal -- never a number
in place of one it cannot honestly produce (design doc S4.2).

Reuses `test_agent_tools_get_features.py`'s fixture shape (real Delta
tables under `tmp_path`, a Silver-event builder) rather than a stub feature
frame: the drift check runs on what `get_features` actually returns, and a
stub would only encode the belief this test exists to check.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from almanac.agent.schemas import EntityKey, PredictInput, PredictRefusal, PredictResult
from almanac.agent.tools import predict
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)

pytestmark = pytest.mark.spark

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)

_INGESTED = datetime(2025, 8, 20, tzinfo=UTC)

type _Row = tuple[
    int, int | None, datetime, str, str | None, str, bool | None, bool | None, None, datetime
]


def _event(
    repo_id: int,
    pr_number: int | None,
    created_at: datetime,
    event_type: str,
    *,
    event_action: str | None,
    actor_login: str,
    pr_merged: bool | None = None,
    pr_draft: bool | None = None,
) -> _Row:
    return (
        repo_id,
        pr_number,
        created_at,
        event_type,
        event_action,
        actor_login,
        pr_merged,
        pr_draft,
        None,
        _INGESTED,
    )


def _pr_opened(
    repo_id: int, pr_number: int, created_at: datetime, actor: str, *, draft: bool | None
) -> _Row:
    return _event(
        repo_id,
        pr_number,
        created_at,
        "PullRequestEvent",
        event_action="opened",
        actor_login=actor,
        pr_draft=draft,
    )


def _pr_closed(
    repo_id: int, pr_number: int, created_at: datetime, actor: str, *, merged: bool
) -> _Row:
    return _event(
        repo_id,
        pr_number,
        created_at,
        "PullRequestEvent",
        event_action="closed",
        actor_login=actor,
        pr_merged=merged,
    )


# PR 8: carol's first PR, opened then closed *merged* -- gives PR 10 and
# PR 11 below a settled prior_pr_count=1, prior_merge_rate=1.0, so the only
# difference between the two query entities is is_draft.
# PR 10: rich-era query entity, pr_draft recorded False.
# PR 11: reduced-era query entity -- pr_draft omitted entirely, the exact
# shape of the 2026-09-05 window (design doc S4.2): the column exists, this
# row's value is null.
_EVENTS = [
    _pr_opened(1, 8, datetime(2025, 8, 10, 9, tzinfo=UTC), "carol", draft=False),
    _pr_closed(1, 8, datetime(2025, 8, 11, 9, tzinfo=UTC), "carol", merged=True),
    _pr_opened(1, 10, datetime(2025, 8, 12, 9, tzinfo=UTC), "carol", draft=False),
    _pr_opened(1, 11, datetime(2025, 8, 13, 9, tzinfo=UTC), "carol", draft=None),
]

_RICH_ENTITY = EntityKey(repo_id=1, pr_number=10)
_RICH_AS_OF = datetime(2025, 8, 12, 9, 5, tzinfo=UTC)
_REDUCED_ENTITY = EntityKey(repo_id=1, pr_number=11)
_REDUCED_AS_OF = datetime(2025, 8, 13, 9, 5, tzinfo=UTC)


class _StubModel:
    """A fixed P(breach) = 0.7 for every row -- `model/score.py`'s
    `ProbabilityModel` Protocol, injected so no endpoint is called (Task 11
    is the first billable step, per the plan).
    """

    def predict_proba(self, features: pd.DataFrame) -> Any:
        return np.tile([0.3, 0.7], (len(features), 1))


def _land(spark: SparkSession, tmp_path: Path) -> tuple[str, str]:
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path = str(tmp_path / "events")
    features_path = str(tmp_path / "features")

    events.write.format("delta").save(f"{silver_path}/clean")
    compute_author_activity(events).write.format("delta").save(f"{features_path}/author_activity")
    compute_repo_activity(events).write.format("delta").save(f"{features_path}/repo_activity")
    compute_pr_static(events).write.format("delta").save(f"{features_path}/pr_static")
    return silver_path, features_path


def test_a_rich_era_window_returns_a_score(spark: SparkSession, tmp_path: Path) -> None:
    silver_path, features_path = _land(spark, tmp_path)

    result = predict(
        spark,
        _StubModel(),
        PredictInput(entity=_RICH_ENTITY, as_of=_RICH_AS_OF),
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
        PredictInput(entity=_REDUCED_ENTITY, as_of=_REDUCED_AS_OF),
        model_version="test-1",
        silver_path=silver_path,
        features_path=features_path,
    )

    assert isinstance(result, PredictRefusal)
    assert result.missing_feature == "is_draft"
    assert "cannot score" in result.reason
    assert "breach_risk" not in type(result).model_fields
