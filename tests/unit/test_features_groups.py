"""compute_repo_activity / compute_pr_static: cumulative-to-date repo
signals and PR-open-time-known attributes, both Silver-native (§3.1 --
never a read of agg_repo_daily or fact_pull_request).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)
from tests.helpers import one

pytestmark = pytest.mark.spark

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


_Row = tuple[
    int, int | None, datetime, str, str | None, str | None, bool | None, bool | None, None, datetime
]


def _row(
    repo_id: int,
    pr_number: int | None,
    created_at: datetime,
    event_type: str,
    *,
    action: str | None = None,
    actor: str | None = None,
    merged: bool | None = None,
    draft: bool | None = None,
) -> _Row:
    return (
        repo_id,
        pr_number,
        created_at,
        event_type,
        action,
        actor,
        merged,
        draft,
        None,
        created_at,
    )


def test_repo_activity_is_cumulative_and_keyed_on_the_events_own_timestamp(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            _row(1, None, datetime(2025, 8, 10, tzinfo=UTC), "WatchEvent", actor="alice"),
            _row(1, None, datetime(2025, 8, 11, tzinfo=UTC), "WatchEvent", actor="dependabot[bot]"),
            _row(
                1,
                5,
                datetime(2025, 8, 12, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="bob",
            ),
        ],
        _SCHEMA,
    )

    result = compute_repo_activity(events)

    # Filtered server-side on a Python datetime literal, never compared
    # against a *collected* one: PySpark returns a collected Timestamp
    # naive in the driver's local timezone (tests/helpers.py's epoch_of
    # docstring), so indexing a dict keyed on a collected event_time with
    # a UTC literal is exactly the pitfall Task 1's spine test hit.
    day1 = one(result.where(F.col("event_time") == F.lit(datetime(2025, 8, 10, tzinfo=UTC))))
    day1_counts = (
        day1["events_total_to_date"],
        day1["bot_events_to_date"],
        day1["prs_opened_to_date"],
    )
    assert day1_counts == (1, 0, 0)

    day3 = one(result.where(F.col("event_time") == F.lit(datetime(2025, 8, 12, tzinfo=UTC))))
    day3_counts = (
        day3["events_total_to_date"],
        day3["bot_events_to_date"],
        day3["prs_opened_to_date"],
    )
    assert day3_counts == (3, 1, 1)
    assert day3["bot_share_to_date"] == pytest.approx(1 / 3)


def test_pr_static_carries_open_time_attributes_with_no_temporal_join(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            _row(
                1,
                5,
                datetime(2025, 8, 12, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="dependabot[bot]",
                draft=True,
            )
        ],
        _SCHEMA,
    )

    result = compute_pr_static(events).collect()

    assert len(result) == 1
    assert result[0]["is_draft"] is True
    assert result[0]["is_bot_author"] is True
    assert result[0]["opened_hour"] == 0


def test_author_activity_prior_pr_count_and_merge_rate(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            # PR1: alice opens day 1, merges day 3.
            _row(
                1,
                1,
                datetime(2025, 8, 1, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="alice",
            ),
            _row(
                1,
                1,
                datetime(2025, 8, 3, tzinfo=UTC),
                "PullRequestEvent",
                action="closed",
                actor="alice",
                merged=True,
            ),
            # PR2: alice opens day 5, still open.
            _row(
                1,
                2,
                datetime(2025, 8, 5, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="alice",
            ),
            # PR3: alice opens day 6 -- PR2 has not closed yet.
            _row(
                1,
                3,
                datetime(2025, 8, 6, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="alice",
            ),
        ],
        _SCHEMA,
    )

    result = compute_author_activity(events)

    pr2 = one(result.where(F.col("event_time") == F.lit(datetime(2025, 8, 5, tzinfo=UTC))))
    assert (pr2["prior_pr_count"], pr2["prior_merge_rate"]) == (1, 1.0)

    pr3 = one(result.where(F.col("event_time") == F.lit(datetime(2025, 8, 6, tzinfo=UTC))))
    # PR2 opened before PR3 but has not closed -- its outcome is unknown,
    # not a non-merge, so it must not appear in either the count or the rate.
    assert (pr3["prior_pr_count"], pr3["prior_merge_rate"]) == (1, 1.0)


def test_author_activity_excludes_a_prior_closing_at_the_exact_open_instant(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            _row(
                1,
                1,
                datetime(2025, 8, 1, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="alice",
            ),
            # PR1 closes at the same instant PR2 opens. "Known strictly
            # before" means PR1's outcome is not yet visible to PR2.
            _row(
                1,
                1,
                datetime(2025, 8, 5, tzinfo=UTC),
                "PullRequestEvent",
                action="closed",
                actor="alice",
                merged=True,
            ),
            _row(
                1,
                2,
                datetime(2025, 8, 5, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="alice",
            ),
        ],
        _SCHEMA,
    )

    pr2 = one(
        compute_author_activity(events).where(
            F.col("event_time") == F.lit(datetime(2025, 8, 5, tzinfo=UTC))
        )
    )
    assert (pr2["prior_pr_count"], pr2["prior_merge_rate"]) == (0, None)


def test_author_activity_is_scoped_per_author(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            _row(
                1,
                1,
                datetime(2025, 8, 1, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="alice",
            ),
            _row(
                1,
                1,
                datetime(2025, 8, 2, tzinfo=UTC),
                "PullRequestEvent",
                action="closed",
                actor="alice",
                merged=False,
            ),
            _row(
                1,
                9,
                datetime(2025, 8, 6, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="bob",
            ),
        ],
        _SCHEMA,
    )

    bob = one(
        compute_author_activity(events).where(
            F.col("event_time") == F.lit(datetime(2025, 8, 6, tzinfo=UTC))
        )
    )
    assert (bob["prior_pr_count"], bob["prior_merge_rate"]) == (0, None)


def test_author_activity_counts_each_prior_once_for_a_same_instant_burst(
    spark: SparkSession,
) -> None:
    # A bot opening many PRs in one second is the shape that made the old
    # self-join a multi-terabyte cartesian blow-up on the real quarter
    # (docs/findings/2026-09-04-author-activity-self-join.md); the old GROUP
    # BY also counted each prior once per burst PR. Each prior counts once.
    burst = datetime(2025, 8, 10, tzinfo=UTC)
    events = spark.createDataFrame(
        [
            _row(
                1,
                1,
                datetime(2025, 8, 1, tzinfo=UTC),
                "PullRequestEvent",
                action="opened",
                actor="botty[bot]",
            ),
            _row(
                1,
                1,
                datetime(2025, 8, 2, tzinfo=UTC),
                "PullRequestEvent",
                action="closed",
                actor="botty[bot]",
                merged=True,
            ),
            _row(1, 5, burst, "PullRequestEvent", action="opened", actor="botty[bot]"),
            _row(1, 6, burst, "PullRequestEvent", action="opened", actor="botty[bot]"),
            _row(1, 7, burst, "PullRequestEvent", action="opened", actor="botty[bot]"),
        ],
        _SCHEMA,
    )

    row = one(compute_author_activity(events).where(F.col("event_time") == F.lit(burst)))
    assert (row["prior_pr_count"], row["prior_merge_rate"]) == (1, 1.0)
