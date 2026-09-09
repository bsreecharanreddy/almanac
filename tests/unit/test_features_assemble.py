"""assemble_training_set: chains as_of_join across both temporal groups
and joins pr_static directly, producing one wide, point-in-time-correct
frame -- the "as_of demo" §9's Phase 3 gate names.
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.assemble import assemble_training_set
from almanac.features.groups import compute_pr_static
from almanac.features.spine import build_pr_opened_spine

pytestmark = pytest.mark.spark


def test_assembles_one_row_per_spine_entry_with_every_group_joined(spark: SparkSession) -> None:
    spine = spark.createDataFrame(
        [(1, 5, "alice", datetime(2025, 8, 13, tzinfo=UTC))],
        "repo_id long, pr_number long, author_login string, as_of_timestamp timestamp",
    )
    author_activity = spark.createDataFrame(
        [("alice", datetime(2025, 8, 10, tzinfo=UTC), 3, 0.5)],
        "author_login string, event_time timestamp, prior_pr_count long, prior_merge_rate double",
    )
    repo_activity = spark.createDataFrame(
        [(1, datetime(2025, 8, 12, tzinfo=UTC), 10, 1, 2, 0.1)],
        "repo_id long, event_time timestamp, events_total_to_date long, bot_events_to_date long, "
        "prs_opened_to_date long, bot_share_to_date double",
    )
    pr_static = spark.createDataFrame(
        [(1, 5, False, False, 4, 9)],
        "repo_id long, pr_number long, is_draft boolean, is_bot_author boolean, "
        "opened_day_of_week int, opened_hour int",
    )

    result = assemble_training_set(
        spine, author_activity=author_activity, repo_activity=repo_activity, pr_static=pr_static
    ).collect()

    assert len(result) == 1
    row = result[0]
    assert (row["prior_pr_count"], row["prior_merge_rate"]) == (3, 0.5)
    assert row["events_total_to_date"] == 10
    assert row["is_draft"] is False


def test_a_pr_with_two_opened_events_does_not_fan_the_join_out(spark: SparkSession) -> None:
    """The measured bug, end to end.

    `build_pr_opened_spine` and `compute_pr_static` are joined on
    (repo_id, pr_number). Emitting one row per opened *event* made that
    equi-join multiply 2x2 into 4 rows for the 25 PRs GH Archive carries two
    distinct `opened` events for -- 7,320,196 rows against a distinct key
    count of 7,320,121 (docs/findings/2026-09-08-pr-opened-spine-fanout.md).

    Built from raw events on purpose: deduping either side alone still fans
    out, so a test on one function cannot catch this.
    """
    schema = (
        "repo_id long, pr_number long, created_at timestamp, event_type string, "
        "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
        "is_pr_comment boolean, ingested_at timestamp"
    )
    earlier = datetime(2025, 8, 13, 9, tzinfo=UTC)
    later = datetime(2025, 8, 13, 11, tzinfo=UTC)
    events = spark.createDataFrame(
        [
            (1, 5, later, "PullRequestEvent", "opened", "alice", None, False, None, later),
            (1, 5, earlier, "PullRequestEvent", "opened", "alice", None, False, None, earlier),
        ],
        schema,
    )
    empty_author = spark.createDataFrame(
        [],
        "author_login string, event_time timestamp, prior_pr_count long, prior_merge_rate double",
    )
    empty_repo = spark.createDataFrame(
        [],
        "repo_id long, event_time timestamp, events_total_to_date long, bot_events_to_date long, "
        "prs_opened_to_date long, bot_share_to_date double",
    )

    result = assemble_training_set(
        build_pr_opened_spine(events),
        author_activity=empty_author,
        repo_activity=empty_repo,
        pr_static=compute_pr_static(events),
    )

    assert result.count() == 1
    assert result.select("repo_id", "pr_number").distinct().count() == 1
