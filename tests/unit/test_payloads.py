"""Parsing Bronze's raw JSON into the Silver contract.

Every assertion here is a measured property of the committed fixtures, not
an expectation of what GH Archive ought to contain.
"""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from almanac.pipeline.payloads import parse_events

pytestmark = pytest.mark.spark


def _parsed(spark: SparkSession, path: Path) -> DataFrame:
    raw = spark.read.text(str(path)).withColumnRenamed("value", "raw_json")
    return parse_events(raw)


def test_modern_fields_are_extracted(spark: SparkSession, modern_events_path: Path) -> None:
    df = _parsed(spark, modern_events_path)
    assert df.count() == 2000
    assert df.filter("event_type IS NULL").count() == 0
    assert df.filter("created_at_raw IS NULL").count() == 0
    assert df.filter("actor_raw IS NULL").count() == 0
    assert df.filter("repo_id IS NULL").count() == 0


def test_legacy_fields_are_extracted(spark: SparkSession, legacy_events_path: Path) -> None:
    """Legacy carries `actor` as a bare string and `repository`, not `repo`.

    Three of 2,000 legacy records carry no `repository` object at all -- the
    quarantine rows Phase 1 already measured -- so `repo_id` is asserted at
    1,997, not 2,000. Asserting 2,000 here would be asserting the fixture is
    something other than what it is.
    """
    df = _parsed(spark, legacy_events_path)
    assert df.count() == 2000
    assert df.filter("event_type IS NULL").count() == 0
    assert df.filter("actor_raw IS NULL").count() == 0
    assert df.filter("repo_id IS NOT NULL").count() == 1997


def test_legacy_repo_name_is_qualified_like_modern(
    spark: SparkSession, legacy_events_path: Path
) -> None:
    named = _parsed(spark, legacy_events_path).filter("repo_name IS NOT NULL")
    assert named.count() == 1997
    assert named.filter("repo_name NOT LIKE '%/%'").count() == 0


def test_pr_number_is_present_in_both_eras(
    spark: SparkSession, modern_events_path: Path, legacy_events_path: Path
) -> None:
    """`(repo_id, pr_number)` is the fact key, so it must survive every era.

    Measured: 149 of 149 modern and 106 of 106 legacy `PullRequestEvent`
    carry a number, and `number` is one of the five fields that survived the
    October 2025 payload reduction -- so the key holds in all three eras.
    """
    for path, expected in ((modern_events_path, 149), (legacy_events_path, 106)):
        prs = _parsed(spark, path).filter("event_type = 'PullRequestEvent'")
        assert prs.count() == expected
        assert prs.filter("pr_number IS NULL").count() == 0


def test_draft_is_null_in_legacy_rather_than_false(
    spark: SparkSession, modern_events_path: Path, legacy_events_path: Path
) -> None:
    """The draft flag did not exist in 2014. Absent is not the same as false.

    Measured: non-null on 149 of 149 modern PRs and 0 of 106 legacy ones.
    Collapsing that to `false` would make the legacy slice look like 106
    confirmed non-draft PRs, and §5.1's draft-exclusion rule would then be
    silently applied to a population it cannot be evaluated on.
    """
    modern = _parsed(spark, modern_events_path).filter("event_type = 'PullRequestEvent'")
    legacy = _parsed(spark, legacy_events_path).filter("event_type = 'PullRequestEvent'")
    assert modern.filter("pr_draft IS NOT NULL").count() == 149
    assert legacy.filter("pr_draft IS NOT NULL").count() == 0


def test_issue_comment_pr_discriminator(spark: SparkSession, modern_events_path: Path) -> None:
    """`issue.pull_request` separates a PR comment from a genuine issue comment.

    Measured: present on 55 of 92 modern `IssueCommentEvent`. The label
    counts a comment only when the issue is actually a PR.
    """
    comments = _parsed(spark, modern_events_path).filter("event_type = 'IssueCommentEvent'")
    assert comments.count() == 92
    assert comments.filter("issue_is_pr").count() == 55


def test_unknown_event_type_does_not_break_parsing(spark: SparkSession) -> None:
    """A new event type must never be able to break ingestion (CLAUDE.md).

    An explicit schema means an unrecognised payload shape yields nulls in
    the columns it does not have, and the row still lands.
    """
    raw = spark.createDataFrame(
        [
            (
                '{"id":"1","type":"BrandNewEvent","created_at":"2025-08-13T14:00:00Z",'
                '"actor":{"login":"a"},"repo":{"id":7,"name":"o/r"},"payload":{"wat":true}}',
            )
        ],
        "raw_json string",
    )
    row = parse_events(raw).first()
    assert row is not None
    assert row["event_type"] == "BrandNewEvent"
    assert row["repo_id"] == 7
    assert row["pr_number"] is None


def test_malformed_json_is_not_silently_dropped(spark: SparkSession) -> None:
    """A record that will not parse must still reach the quality rules.

    Dropping it here would put it in neither Silver nor quarantine, which is
    the exact vanishing act `coalesce(cond, False)` exists to prevent one
    layer down.
    """
    raw = spark.createDataFrame([("{not json at all",)], "raw_json string")
    parsed = parse_events(raw)
    assert parsed.count() == 1
    row = parsed.first()
    assert row is not None
    assert row["event_type"] is None
