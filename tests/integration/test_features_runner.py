"""run_features against real Delta I/O: all three tables land, and a
second run overwrites rather than duplicates (full recompute is
idempotent by construction -- this is what proves it).
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.features.runner import main, run_features

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


def _write_silver(spark: SparkSession, path: Path) -> None:
    rows = [
        (
            1,
            5,
            datetime(2025, 8, 13, 9, tzinfo=UTC),
            "PullRequestEvent",
            "opened",
            "alice",
            None,
            False,
            None,
            datetime(2025, 8, 13, 9, 5, tzinfo=UTC),
        ),
    ]
    spark.createDataFrame(rows, _SCHEMA).write.format("delta").save(str(path / "clean"))


def test_run_features_writes_all_three_tables_and_is_idempotent(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    _write_silver(spark, silver_path)

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )

    for name in ("author_activity", "repo_activity", "pr_static"):
        assert spark.read.format("delta").load(str(features_path / name)).count() >= 1

    first_pr_static = spark.read.format("delta").load(str(features_path / "pr_static")).collect()

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )
    second_pr_static = spark.read.format("delta").load(str(features_path / "pr_static")).collect()

    assert sorted(map(str, first_pr_static)) == sorted(map(str, second_pr_static))


def test_main_wires_the_parsed_arguments_through_to_a_real_run(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The CLI path, exercised for real -- `SparkSession.getActiveSession()`
    picks up this test's own session, so `main()` never falls back to
    building a new one, and no mocking is needed to prove the wiring works.
    """
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    _write_silver(spark, silver_path)

    code = main(["--silver-path", str(silver_path), "--features-path", str(features_path)])

    assert code == 0
    assert spark.read.format("delta").load(str(features_path / "author_activity")).count() >= 0
