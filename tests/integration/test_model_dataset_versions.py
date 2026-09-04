"""build_training_frame: every Delta read is independently version-
pinnable, so a specific training frame -- features AND label -- stays
reproducible even after later Gold/Silver activity, extending Task 7's
leakage-suite invariant to the label join (design doc §5.2).
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from almanac.features.runner import run_features
from almanac.model.dataset import build_classification_frame, build_training_frame

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)
_GOLD_SCHEMA = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string"
)
_GOLD_SCHEMA_WITH_TIMES = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, "
    "label_exclusion string, opened_at timestamp, closed_at timestamp"
)


def _latest_version(spark: SparkSession, path: Path) -> int:
    row = DeltaTable.forPath(spark, str(path)).history(1).select("version").collect()[0]
    return int(row["version"])


def _register(spark: SparkSession, name: str, location: Path) -> None:
    """Gold's fact is a metastore table on the real platform, not a path
    (Task 9); an external table over the tmp Delta dir mirrors that here.
    """
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"CREATE TABLE {name} USING DELTA LOCATION '{location}'")


def test_pinning_every_version_reproduces_the_frame_after_a_later_label_update(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_dir = tmp_path / "gold_fact"
    gold_table = f"gold_fact_{tmp_path.name}".replace("-", "_")

    spark.createDataFrame(
        [
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
            )
        ],
        _SILVER_SCHEMA,
    ).write.format("delta").save(str(silver_path / "clean"))

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )
    features_v1 = _latest_version(spark, features_path / "author_activity")

    spark.createDataFrame([(1, 5, 3600, None)], _GOLD_SCHEMA).write.format("delta").save(
        str(gold_dir)
    )
    _register(spark, gold_table, gold_dir)
    gold_v1 = _latest_version(spark, gold_dir)

    def build(features_version: int, gold_version: int) -> pd.DataFrame:
        return build_training_frame(
            spark,
            silver_path=str(silver_path),
            features_path=str(features_path),
            gold_table=gold_table,
            features_version=features_version,
            gold_version=gold_version,
        )

    original = build(features_v1, gold_v1)
    assert original.iloc[0]["time_to_first_response_seconds"] == 3600

    # A later Gold correction: a different PR's label lands as a second
    # Delta version, well after the original build -- not backdated in place.
    spark.createDataFrame([(1, 6, 7200, None)], _GOLD_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(gold_dir))
    gold_v2 = _latest_version(spark, gold_dir)

    # assert_frame_equal, not `==`: the frame carries NaN feature columns
    # (this PR's own open event is its as-of cutoff, so every temporal
    # lookup is a strict-`<` miss), and NaN != NaN makes a dict `==` compare
    # unequal even when the frames are identical.
    pd.testing.assert_frame_equal(build(features_v1, gold_v1), original)

    live = build(features_v1, gold_v2)
    assert len(live) == 1  # PR 6 has no feature-side spine row; still one training row
    pd.testing.assert_frame_equal(live, original)  # this PR's label untouched by the other's append


def test_classification_frame_shares_the_same_version_pinning(
    spark: SparkSession, tmp_path: Path
) -> None:
    """build_classification_frame wires the same shared feature frame and
    version-pinned Gold read as build_training_frame (§5.3) -- only the
    label join differs.
    """
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_dir = tmp_path / "gold_fact"
    gold_table = f"gold_fact_cls_{tmp_path.name}".replace("-", "_")

    spark.createDataFrame(
        [
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
            )
        ],
        _SILVER_SCHEMA,
    ).write.format("delta").save(str(silver_path / "clean"))

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )
    features_v1 = _latest_version(spark, features_path / "author_activity")

    spark.createDataFrame([(1, 5, 3600, None, None, None)], _GOLD_SCHEMA_WITH_TIMES).write.format(
        "delta"
    ).save(str(gold_dir))
    _register(spark, gold_table, gold_dir)
    gold_v1 = _latest_version(spark, gold_dir)

    frame = build_classification_frame(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_table=gold_table,
        threshold_seconds=100,
        features_version=features_v1,
        gold_version=gold_v1,
    )

    assert len(frame) == 1
    assert bool(frame.iloc[0]["breach"]) is True  # 3600s > 100s threshold
