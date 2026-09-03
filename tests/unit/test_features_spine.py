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
