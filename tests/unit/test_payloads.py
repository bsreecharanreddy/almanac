"""Parsing Bronze's raw JSON into the Silver contract."""

import json
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
    """Legacy carries `actor` as a bare string and `repository`, not `repo`."""
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
    """`(repo_id, pr_number)` is the fact key, so it must survive every era."""
    for path, expected in ((modern_events_path, 149), (legacy_events_path, 106)):
        prs = _parsed(spark, path).filter("event_type = 'PullRequestEvent'")
        assert prs.count() == expected
        assert prs.filter("pr_number IS NULL").count() == 0


def test_draft_is_null_in_legacy_rather_than_false(
    spark: SparkSession, modern_events_path: Path, legacy_events_path: Path
) -> None:
    """The draft flag did not exist in 2014. Absent is not the same as false."""
    modern = _parsed(spark, modern_events_path).filter("event_type = 'PullRequestEvent'")
    legacy = _parsed(spark, legacy_events_path).filter("event_type = 'PullRequestEvent'")
    assert modern.filter("pr_draft IS NOT NULL").count() == 149
    assert legacy.filter("pr_draft IS NOT NULL").count() == 0


def test_issue_comment_pr_discriminator(spark: SparkSession, modern_events_path: Path) -> None:
    """`issue.pull_request` separates a PR comment from a genuine issue comment."""
    comments = _parsed(spark, modern_events_path).filter("event_type = 'IssueCommentEvent'")
    assert comments.count() == 92
    assert comments.filter("issue_is_pr").count() == 55


def test_push_size_is_extracted_from_the_payload_in_both_eras(
    spark: SparkSession, modern_events_path: Path, legacy_events_path: Path
) -> None:
    """Commit volume comes from `payload.size`, never the 20-capped `commits` array."""
    modern = _parsed(spark, modern_events_path).filter("event_type = 'PushEvent'")
    legacy = _parsed(spark, legacy_events_path).filter("event_type = 'PushEvent'")

    assert modern.filter("push_size IS NULL").count() == 0
    assert modern.filter("push_distinct_size IS NULL").count() == 0
    assert modern.filter("push_size > 20").count() > 0, "trap 3: real pushes exceed the array cap"

    assert legacy.filter("push_size IS NULL").count() == 0
    assert legacy.filter("push_distinct_size IS NOT NULL").count() == 0


def test_unknown_event_type_does_not_break_parsing(spark: SparkSession) -> None:
    """A new event type must never be able to break ingestion (CLAUDE.md)."""
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
    """A record that will not parse must still reach the quality rules."""
    raw = spark.createDataFrame([("{not json at all",)], "raw_json string")
    parsed = parse_events(raw)
    assert parsed.count() == 1
    row = parsed.first()
    assert row is not None
    assert row["event_type"] is None


def _events_file(tmp_path: Path, *events: dict[str, object]) -> Path:
    path = tmp_path / "events.json"
    path.write_text("\n".join(json.dumps(e) for e in events))
    return path


def _pr_event(action: str, *, pull_request: dict[str, object]) -> dict[str, object]:
    return {
        "id": f"evt-{action}",
        "type": "PullRequestEvent",
        "actor": {"login": "alice"},
        "repo": {"id": 1, "name": "o/r"},
        "created_at": "2026-09-08T14:00:00Z",
        "payload": {"action": action, "number": 7, "pull_request": pull_request},
    }


# The reduced era's `pull_request` object, measured 2026-09-08 across 5 hours:
# these five keys and nothing else -- no `merged`, no `draft`, no `title`.
_REDUCED_PR: dict[str, object] = {"base": {}, "head": {}, "id": 9, "number": 7, "url": "u"}


def test_reduced_era_merge_is_read_from_the_action(spark: SparkSession, tmp_path: Path) -> None:
    """The October 2025 reduction dropped `pull_request.merged` and replaced it
    with a distinct `action='merged'` -- present in every 2026 hour sampled
    (285/457/283), absent from the 2025 hour, whose actions are only
    {opened, closed, reopened}. Reading only the field leaves `pr_merged` NULL
    across the whole reduced era (design doc 4.8 decision 3).
    """
    path = _events_file(
        tmp_path,
        _pr_event("merged", pull_request=_REDUCED_PR),
        _pr_event("closed", pull_request=_REDUCED_PR),
    )
    by_action = {r["event_action"]: r["pr_merged"] for r in _parsed(spark, path).collect()}

    assert by_action["merged"] is True
    assert by_action["closed"] is False


def test_reduced_era_open_leaves_merge_unknown(spark: SparkSession, tmp_path: Path) -> None:
    """An `opened` event says nothing about whether the PR ever merged.

    Era-bound nulls stay unknown rather than being folded to False -- the same
    rule `pr_draft` already follows. Folding here would fabricate a negative
    label for every open PR in the reduced era.
    """
    path = _events_file(tmp_path, _pr_event("opened", pull_request=_REDUCED_PR))

    assert _parsed(spark, path).collect()[0]["pr_merged"] is None


def test_rich_era_still_reads_the_field_not_the_action(spark: SparkSession, tmp_path: Path) -> None:
    """The rich era carries `merged` on a `closed` action, and it must win.

    Deriving from the action first would read this true merge as False.
    """
    path = _events_file(
        tmp_path, _pr_event("closed", pull_request={**_REDUCED_PR, "merged": True, "draft": False})
    )

    assert _parsed(spark, path).collect()[0]["pr_merged"] is True
