"""run_similarity_comparison against real Delta I/O and a real local
MLflow file-store -- same offline/hermetic shape as test_model_runner.py.

The Silver+Gold fixture below is deliberately not shared with that file's
own `_write_silver_and_gold_for_classification`: this one also needs a
similarity table, and duplicating the fixture builder here keeps each
test file's setup readable on its own rather than reaching across files
for a helper only test_model_runner.py otherwise owns.
"""

from pathlib import Path

import mlflow
import pytest
from pyspark.sql import SparkSession

from almanac.features.runner import run_features
from almanac.model.similarity_comparison import (
    CHAMPION_AVERAGE_PRECISION,
    run_similarity_comparison,
)
from tests.helpers import opened_at

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)
_GOLD_SCHEMA = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, "
    "label_exclusion string, opened_at timestamp, closed_at timestamp"
)
_SIMILARITY_SCHEMA = (
    "repo_id long, pr_number long, similar_neighbor_count long, similar_prior_breach_rate double"
)


def _write_fixtures(spark: SparkSession, tmp_path: Path) -> tuple[Path, Path, str, Path]:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_dir = tmp_path / "gold_fact"
    gold_table = f"gold_fact_sim_{tmp_path.name}".replace("-", "_")
    similarity_path = tmp_path / "similarity"

    rows = [
        (
            1,
            n,
            opened_at(n),
            "PullRequestEvent",
            "opened",
            "alice",
            None,
            False,
            None,
            opened_at(n, minutes=5),
        )
        for n in range(1, 60)
    ]
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").save(
        str(silver_path / "clean")
    )
    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )

    # n=1..30 under the threshold, n=31..59 over it -- both classes present.
    gold_rows = [(1, n, 1000 + n * 10, None, None, None) for n in range(1, 60)]
    spark.createDataFrame(gold_rows, _GOLD_SCHEMA).write.format("delta").save(str(gold_dir))
    spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
    spark.sql(f"CREATE TABLE {gold_table} USING DELTA LOCATION '{gold_dir}'")

    # The similarity column carries real signal (mirrors
    # test_model_train.py's own synthetic-signal fixture): a PR is scored
    # only if n is even, and among those, similar_prior_breach_rate
    # perfectly predicts the label -- enough for train_classifier's
    # is_unbalance/default sweep to pick it up on a small fixture.
    similarity_rows = [(1, n, 4, 1.0 if n > 30 else 0.0) for n in range(1, 60) if n % 2 == 0]
    spark.createDataFrame(similarity_rows, _SIMILARITY_SCHEMA).write.format("delta").save(
        str(similarity_path)
    )
    return silver_path, features_path, gold_table, similarity_path


def test_run_similarity_comparison_logs_both_arms_and_checks_the_champion_gate(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path, gold_table, similarity_path = _write_fixtures(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"
    experiment_name = "pr-similarity-comparison-test"

    result = run_similarity_comparison(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_table=gold_table,
        threshold_seconds=1200,
        similarity_path=str(similarity_path),
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        register=False,
    )

    assert result.with_similarity.feature_columns[-2:] == [
        "similar_neighbor_count",
        "similar_prior_breach_rate",
    ]
    assert "similar_neighbor_count" not in result.without_similarity.feature_columns
    assert result.beats_champion == (
        result.with_similarity.candidates[result.with_similarity.best_candidate].average_precision
        > CHAMPION_AVERAGE_PRECISION
    )

    mlflow.set_tracking_uri(tracking_uri)
    with_runs = mlflow.search_runs(experiment_names=[experiment_name])
    without_runs = mlflow.search_runs(experiment_names=[f"{experiment_name}-without-similarity"])
    assert len(with_runs) == len(result.with_similarity.candidates) + 1
    assert len(without_runs) == len(result.without_similarity.candidates) + 1
