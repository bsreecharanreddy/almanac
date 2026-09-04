"""as_of_join: the point-in-time-correct engine every feature group's
lookup runs through. Three cases, each forcing the next refinement:
ordinary (latest prior row wins), boundary (a row timestamped exactly at
the as-of time must not count as prior), cold start (no qualifying row
leaves nulls, never drops the spine row).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.join import as_of_join

pytestmark = pytest.mark.spark


def test_the_latest_row_strictly_before_the_as_of_time_wins(spark: SparkSession) -> None:
    spine = spark.createDataFrame(
        [(1, datetime(2025, 8, 13, tzinfo=UTC))],
        "repo_id long, as_of_timestamp timestamp",
    )
    feature_table = spark.createDataFrame(
        [
            (1, datetime(2025, 8, 10, tzinfo=UTC), 1),
            (1, datetime(2025, 8, 12, tzinfo=UTC), 2),  # latest prior row
            (1, datetime(2025, 8, 14, tzinfo=UTC), 99),  # after the as-of time
        ],
        "repo_id long, event_time timestamp, value long",
    )

    result = as_of_join(spine, feature_table, on=["repo_id"]).collect()

    assert len(result) == 1
    assert result[0]["value"] == 2


def test_a_row_timestamped_exactly_at_the_as_of_time_is_not_prior(spark: SparkSession) -> None:
    """The strict `<` this project's governing invariant depends on
    (CLAUDE.md): a feature row at exactly T has not happened yet from a
    spine row asking "as of T", and must not leak in.
    """
    t = datetime(2025, 8, 13, tzinfo=UTC)
    spine = spark.createDataFrame([(1, t)], "repo_id long, as_of_timestamp timestamp")
    feature_table = spark.createDataFrame(
        [(1, t, 999)],  # exactly at the as-of time -- must be excluded
        "repo_id long, event_time timestamp, value long",
    )

    result = as_of_join(spine, feature_table, on=["repo_id"]).collect()

    assert len(result) == 1
    assert result[0]["value"] is None


def test_no_qualifying_row_leaves_features_null_rather_than_dropping_the_spine_row(
    spark: SparkSession,
) -> None:
    """Cold start is a fact about an entity's history, not an error --
    the same discipline `label_exclusion` already applies to the label
    (design doc §5.1). A first-time author, or a repo with only future
    rows relative to this spine row, must still appear in the assembled
    training set.
    """
    spine = spark.createDataFrame(
        [(1, datetime(2025, 8, 13, tzinfo=UTC)), (2, datetime(2025, 8, 13, tzinfo=UTC))],
        "repo_id long, as_of_timestamp timestamp",
    )
    feature_table = spark.createDataFrame(
        # repo 1's only row is future; repo 2 has none at all
        [(1, datetime(2025, 8, 14, tzinfo=UTC), 999)],
        "repo_id long, event_time timestamp, value long",
    )

    result = {
        r["repo_id"]: r["value"] for r in as_of_join(spine, feature_table, on=["repo_id"]).collect()
    }

    assert result == {1: None, 2: None}


def test_one_key_with_many_spine_rows_and_many_feature_rows(spark: SparkSession) -> None:
    """The bot-author shape: a single `on` key carrying a large share of
    both sides. The old `spine join feature_table` form made this key an
    O(n^2) blow-up (docs/findings/2026-09-04-author-activity-self-join.md);
    each spine row must still get the latest feature value strictly before
    its own as-of time.
    """
    key = "dependabot[bot]"
    spine = spark.createDataFrame(
        [(key, datetime(2025, 8, day, tzinfo=UTC)) for day in (2, 4, 6, 8)],
        "author_login string, as_of_timestamp timestamp",
    )
    feature_table = spark.createDataFrame(
        [(key, datetime(2025, 8, day, tzinfo=UTC), day * 10) for day in (1, 3, 5, 7)],
        "author_login string, event_time timestamp, value long",
    )

    rows = (
        as_of_join(spine, feature_table, on=["author_login"]).orderBy("as_of_timestamp").collect()
    )

    # as-of day 2 -> day-1 value; day 4 -> day-3; day 6 -> day-5; day 8 -> day-7
    assert [r["value"] for r in rows] == [10, 30, 50, 70]
