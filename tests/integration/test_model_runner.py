"""run_training / main against real Delta I/O and a real local MLflow
file-store -- offline and hermetic, exactly like every other integration
test in this repo. The live Databricks/UC path is exercised for real only
during Phase 4's cloud verification step (this plan's own next task).
"""

from datetime import UTC, datetime
from pathlib import Path

import mlflow
import pytest
from pyspark.sql import SparkSession

from almanac.features.runner import run_features
from almanac.model.runner import main, run_training

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)
_GOLD_SCHEMA = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string"
)


def _write_silver_and_gold(spark: SparkSession, tmp_path: Path) -> tuple[Path, Path, Path]:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_warehouse = tmp_path / "warehouse"

    rows = [
        (
            1,
            n,
            datetime(2025, 8, 13, 9, tzinfo=UTC),
            "PullRequestEvent",
            "opened",
            "alice",
            None,
            False,
            None,
            datetime(2025, 8, 13, 9, 5, tzinfo=UTC),
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
    spark.createDataFrame(gold_rows, _GOLD_SCHEMA).write.format("delta").save(
        str(gold_warehouse / "gold.db" / "fact_pull_request")
    )
    return silver_path, features_path, gold_warehouse


def test_run_training_logs_a_real_mlflow_run(spark: SparkSession, tmp_path: Path) -> None:
    silver_path, features_path, gold_warehouse = _write_silver_and_gold(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"

    result = run_training(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_warehouse=str(gold_warehouse),
        tracking_uri=tracking_uri,
        experiment_name="pr-review-sla-risk-test",
        register=False,
    )

    assert result.model_mae >= 0
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-risk-test"])
    assert len(runs) == 1


def test_main_wires_the_parsed_arguments_through_to_a_real_run(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path, gold_warehouse = _write_silver_and_gold(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"

    code = main(
        [
            "--silver-path",
            str(silver_path),
            "--features-path",
            str(features_path),
            "--gold-warehouse",
            str(gold_warehouse),
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
