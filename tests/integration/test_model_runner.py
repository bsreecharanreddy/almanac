"""run_training / main against real Delta I/O and a real local MLflow
file-store -- offline and hermetic, exactly like every other integration
test in this repo. The live Databricks/UC path is exercised for real only
during Phase 4's cloud verification step (this plan's own next task).
"""

from pathlib import Path

import mlflow
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from almanac.features.runner import run_features
from almanac.model.dataset import build_classification_frame
from almanac.model.runner import main, run_training
from almanac.model.train import ClassificationResult, TrainResult
from tests.helpers import opened_at

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)
_GOLD_SCHEMA = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string"
)
# join_breach_label's `closed_no_response` branch references opened_at/
# closed_at even when every row's label_exclusion is NULL (Spark compiles
# both `when` branches against the schema) -- so the classification path's
# fixture needs these columns present, unlike the regression path's.
_GOLD_SCHEMA_WITH_TIMES = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, "
    "label_exclusion string, opened_at timestamp, closed_at timestamp"
)


def _write_silver_and_gold(spark: SparkSession, tmp_path: Path) -> tuple[Path, Path, str]:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_dir = tmp_path / "gold_fact"
    gold_table = f"gold_fact_{tmp_path.name}".replace("-", "_")

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
        for n in range(1, 40)
    ]
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").save(
        str(silver_path / "clean")
    )

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )

    gold_rows = [(1, n, 1000 + n * 10, None) for n in range(1, 40)]
    spark.createDataFrame(gold_rows, _GOLD_SCHEMA).write.format("delta").save(str(gold_dir))
    # Gold's fact is a metastore table on the real platform, not a path
    # (Task 9); an external table over the tmp Delta dir mirrors that here.
    spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
    spark.sql(f"CREATE TABLE {gold_table} USING DELTA LOCATION '{gold_dir}'")
    return silver_path, features_path, gold_table


def test_run_training_logs_a_real_mlflow_run(spark: SparkSession, tmp_path: Path) -> None:
    silver_path, features_path, gold_table = _write_silver_and_gold(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"

    result = run_training(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_table=gold_table,
        tracking_uri=tracking_uri,
        experiment_name="pr-review-sla-risk-test",
        register=False,
    )

    assert isinstance(result, TrainResult)  # objective="regression" (the default)
    assert result.model_mae >= 0
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-risk-test"])
    assert len(runs) == 1


def test_main_wires_the_parsed_arguments_through_to_a_real_run(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path, gold_table = _write_silver_and_gold(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"

    code = main(
        [
            "--silver-path",
            str(silver_path),
            "--features-path",
            str(features_path),
            "--gold-table",
            gold_table,
            "--tracking-uri",
            tracking_uri,
            "--experiment-name",
            "pr-review-sla-risk-cli-test",
        ]
    )

    assert code == 0
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-risk-cli-test"])
    assert len(runs) == 1


def _write_silver_and_gold_for_classification(
    spark: SparkSession, tmp_path: Path
) -> tuple[Path, Path, str]:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_dir = tmp_path / "gold_fact"
    gold_table = f"gold_fact_cls_{tmp_path.name}".replace("-", "_")

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
        for n in range(1, 40)
    ]
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").save(
        str(silver_path / "clean")
    )

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )

    # n=1..20 land under the 1200s threshold, n=21..39 over it -- both
    # classes present in both the train and test split (§5.3's population,
    # every row label_exclusion=None).
    gold_rows = [(1, n, 1000 + n * 10, None, None, None) for n in range(1, 40)]
    spark.createDataFrame(gold_rows, _GOLD_SCHEMA_WITH_TIMES).write.format("delta").save(
        str(gold_dir)
    )
    spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
    spark.sql(f"CREATE TABLE {gold_table} USING DELTA LOCATION '{gold_dir}'")
    return silver_path, features_path, gold_table


def test_run_training_classification_objective_logs_a_real_mlflow_comparison(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path, gold_table = _write_silver_and_gold_for_classification(
        spark, tmp_path
    )
    tracking_uri = f"file://{tmp_path}/mlruns"

    result = run_training(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_table=gold_table,
        tracking_uri=tracking_uri,
        experiment_name="pr-review-sla-breach-test",
        register=False,
        objective="classification",
        threshold_seconds=1200,
    )

    assert isinstance(result, ClassificationResult)
    assert len(result.candidates) >= 2
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-breach-test"])
    assert len(runs) == len(result.candidates) + 1  # one per candidate, plus the baseline


def test_build_classification_frame_similarity_frame_is_absent_unless_given(
    spark: SparkSession, tmp_path: Path
) -> None:
    """similarity_frame's None path (design doc §8.3a) stays exactly the
    frame build_classification_frame has always produced -- no similar_*
    columns, same row count -- and passing one adds them, left-joined, with
    a genuinely-absent (not zero-filled) row for a PR it never scored.
    """
    silver_path, features_path, gold_table = _write_silver_and_gold_for_classification(
        spark, tmp_path
    )

    without = build_classification_frame(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_table=gold_table,
        threshold_seconds=1200,
    )
    assert "similar_prior_breach_rate" not in without.columns

    similarity_frame = spark.createDataFrame(
        [(1, 1, 4, 0.75)],  # only pr_number=1 was ever scored
        "repo_id long, pr_number long, similar_neighbor_count long, "
        "similar_prior_breach_rate double",
    )
    with_similarity = build_classification_frame(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_table=gold_table,
        threshold_seconds=1200,
        similarity_frame=similarity_frame,
    )

    assert len(with_similarity) == len(without)  # a left join drops nothing
    by_pr = with_similarity.set_index("pr_number")["similar_prior_breach_rate"]
    assert by_pr.loc[1] == 0.75
    assert by_pr.loc[2] is None or pd.isna(by_pr.loc[2])  # never scored -- absent, not 0.0


def test_main_wires_the_classification_objective_through_to_a_real_run(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path, gold_table = _write_silver_and_gold_for_classification(
        spark, tmp_path
    )
    tracking_uri = f"file://{tmp_path}/mlruns"

    code = main(
        [
            "--silver-path",
            str(silver_path),
            "--features-path",
            str(features_path),
            "--gold-table",
            gold_table,
            "--tracking-uri",
            tracking_uri,
            "--experiment-name",
            "pr-review-sla-breach-cli-test",
            "--objective",
            "classification",
            "--threshold-seconds",
            "1200",
        ]
    )

    assert code == 0
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-breach-cli-test"])
    assert len(runs) >= 2  # at least one candidate plus the baseline
