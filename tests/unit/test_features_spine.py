"""build_pr_opened_spine: the PR-opened population, re-derived from Silver
-- not read from fact_pull_request, per §3.1's peer-of-Gold boundary
(design doc §4.4a).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.spine import build_pr_opened_spine
from tests.helpers import epoch_of

pytestmark = pytest.mark.spark

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


def test_only_opened_pull_request_events_become_spine_rows(spark: SparkSession) -> None:
    rows = [
        (
            1,
            10,
            datetime(2025, 8, 13, 9, tzinfo=UTC),
            "PullRequestEvent",
            "opened",
            "alice",
            None,
            False,
            None,
            datetime(2025, 8, 13, 9, 5, tzinfo=UTC),
        ),
        # A closed event for the same PR must not also become a spine row.
        (
            1,
            10,
            datetime(2025, 8, 14, 9, tzinfo=UTC),
            "PullRequestEvent",
            "closed",
            "bob",
            True,
            False,
            None,
            datetime(2025, 8, 14, 9, 5, tzinfo=UTC),
        ),
        # A review event is not an "opened" event at all.
        (
            1,
            10,
            datetime(2025, 8, 13, 12, tzinfo=UTC),
            "PullRequestReviewEvent",
            None,
            "carol",
            None,
            None,
            None,
            datetime(2025, 8, 13, 12, 5, tzinfo=UTC),
        ),
    ]
    events = spark.createDataFrame(rows, _SCHEMA)

    spine = build_pr_opened_spine(events)

    assert spine.columns == ["repo_id", "pr_number", "author_login", "as_of_timestamp"]
    result = spine.collect()
    assert len(result) == 1
    assert result[0]["author_login"] == "alice"
    # No test asserts on a collected datetime (tests/helpers.py's epoch_of
    # docstring): PySpark returns it naive in the driver's local timezone,
    # so a direct comparison passes or fails by machine.
    expected = int(datetime(2025, 8, 13, 9, tzinfo=UTC).timestamp())
    assert epoch_of(spine, "as_of_timestamp") == expected


def _opened_row(pr: int, when: datetime, who: str, draft: bool = False) -> tuple[object, ...]:
    return (
        1,
        pr,
        when,
        "PullRequestEvent",
        "opened",
        who,
        None,
        draft,
        None,
        when,
    )


def test_two_opened_events_for_one_pr_produce_one_spine_row(spark: SparkSession) -> None:
    """GH Archive carries two distinct `opened` events -- different
    `event_id`s, so Silver is right to keep both -- for 25 PRs in the
    measured quarter. The spine's grain is the PR, not the event
    (docs/findings/2026-09-08-pr-opened-spine-fanout.md).
    """
    later = datetime(2025, 8, 13, 11, tzinfo=UTC)
    earlier = datetime(2025, 8, 13, 9, tzinfo=UTC)
    # Later row first, so a fix that merely takes the first row seen fails.
    events = spark.createDataFrame(
        [_opened_row(10, later, "alice"), _opened_row(10, earlier, "alice")], _SCHEMA
    )

    spine = build_pr_opened_spine(events)

    assert spine.count() == 1
    assert epoch_of(spine, "as_of_timestamp") == int(earlier.timestamp())


def test_distinct_prs_are_not_collapsed(spark: SparkSession) -> None:
    """The dedup must key on the PR, never on the repo."""
    when = datetime(2025, 8, 13, 9, tzinfo=UTC)
    events = spark.createDataFrame(
        [_opened_row(10, when, "alice"), _opened_row(11, when, "bob")], _SCHEMA
    )

    assert build_pr_opened_spine(events).count() == 2


def test_the_spine_is_deterministic_when_duplicates_tie_on_time(spark: SparkSession) -> None:
    """Byte-for-byte reproducibility is the governing invariant, so a tie
    on `created_at` must not leave the winner to Spark's row order.
    """
    when = datetime(2025, 8, 13, 9, tzinfo=UTC)
    rows = [_opened_row(10, when, "bob"), _opened_row(10, when, "alice")]
    first = build_pr_opened_spine(spark.createDataFrame(rows, _SCHEMA)).collect()
    second = build_pr_opened_spine(spark.createDataFrame(rows[::-1], _SCHEMA)).collect()

    assert len(first) == 1
    assert first == second
