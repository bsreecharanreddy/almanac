from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.explore.schema import era_for
from almanac.pipeline.eras import normalize_events
from tests.helpers import RawRow, epoch_of, one, raw

pytestmark = pytest.mark.spark

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)

LEGACY: RawRow = ("2014-06-12T03:00:00-07:00", "abc", 1, "o/r", "PushEvent", None, None, INGESTED)
MODERN: RawRow = ("2025-08-13T14:00:00Z", "abc", 1, "o/r", "PushEvent", "999", None, INGESTED)


def instant(year: int, month: int, day: int, hour: int, minute: int) -> int:
    """The epoch second of a UTC wall-clock time."""
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp())


def test_legacy_timestamp_offset_is_converted_not_truncated(spark: SparkSession) -> None:
    """Measured: pre-2015 `created_at` carries -07:00."""
    assert epoch_of(normalize_events(raw(spark, LEGACY)), "created_at") == instant(
        2014, 6, 12, 10, 0
    )


def test_modern_timestamp_is_utc_unchanged(spark: SparkSession) -> None:
    assert epoch_of(normalize_events(raw(spark, MODERN)), "created_at") == instant(
        2025, 8, 13, 14, 0
    )


def test_legacy_event_id_is_a_content_hash(spark: SparkSession) -> None:
    """Measured: 0 of 2,000 legacy events carry an `id`."""
    out = one(normalize_events(raw(spark, LEGACY)))
    assert out["event_id"], "legacy events must still get an id"
    assert out["event_id_source"] == "content_hash"


def test_modern_event_id_is_the_native_id(spark: SparkSession) -> None:
    out = one(normalize_events(raw(spark, MODERN)))
    assert out["event_id"] == "999"
    assert out["event_id_source"] == "native"


def test_content_hash_is_stable_across_runs(spark: SparkSession) -> None:
    # A non-deterministic hash would duplicate legacy history on every re-run.
    first = one(normalize_events(raw(spark, LEGACY)))["event_id"]
    second = one(normalize_events(raw(spark, LEGACY)))["event_id"]
    assert first == second


def test_distinct_legacy_events_hash_differently(spark: SparkSession) -> None:
    other: RawRow = (
        "2014-06-12T03:00:00-07:00",
        "abc",
        2,
        "o/s",
        "PushEvent",
        None,
        None,
        INGESTED,
    )
    ids = [r["event_id"] for r in normalize_events(raw(spark, LEGACY, other)).collect()]
    assert len(set(ids)) == 2


def test_era_is_labelled_on_every_row(spark: SparkSession) -> None:
    rows: list[RawRow] = [
        ("2014-06-12T03:00:00-07:00", "a", 1, "o/r", "PushEvent", None, None, INGESTED),
        ("2025-08-13T14:00:00Z", "b", 2, "o/s", "PushEvent", "1", None, INGESTED),
        ("2025-11-01T00:00:00Z", "c", 3, "o/t", "PushEvent", "2", None, INGESTED),
    ]
    eras = {r["schema_era"] for r in normalize_events(raw(spark, *rows)).collect()}
    assert eras == {"legacy_v1", "modern_v2", "reduced_v3"}


def test_repo_name_case_is_preserved(spark: SparkSession) -> None:
    """Measured: case-only renames exist (GLB -> glb)."""
    row: RawRow = (
        "2025-08-13T14:00:00Z",
        "a",
        1,
        "Lumacaonta/GLB",
        "PushEvent",
        "1",
        None,
        INGESTED,
    )
    assert one(normalize_events(raw(spark, row)))["repo_name"] == "Lumacaonta/GLB"


def test_output_is_exactly_the_canonical_silver_shape(spark: SparkSession) -> None:
    """The declared contract, asserted rather than assumed."""
    assert set(normalize_events(raw(spark, MODERN)).columns) == {
        "event_id",
        "event_id_source",
        "actor_login",
        "created_at",
        "repo_id",
        "repo_name",
        "event_type",
        "event_action",
        "schema_era",
        "ingested_at",
        "event_date",
        "event_hour",
        "pr_number",
        "pr_merged",
        "pr_draft",
        "is_pr_comment",
        "push_size",
        "push_distinct_size",
    }


def test_spark_era_labels_agree_with_the_python_implementation(spark: SparkSession) -> None:
    """One rule, two implementations; they must not drift apart."""
    boundaries = [
        datetime(2014, 12, 31, 23, 59, tzinfo=UTC),
        datetime(2015, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 10, 14, 23, 59, tzinfo=UTC),
        datetime(2025, 10, 15, 0, 0, tzinfo=UTC),
    ]
    rows: list[RawRow] = [
        (d.strftime("%Y-%m-%dT%H:%M:%SZ"), "a", 1, "o/r", "PushEvent", str(n), None, INGESTED)
        for n, d in enumerate(boundaries)
    ]
    out = normalize_events(raw(spark, *rows)).collect()
    labelled = {r["event_id"]: r["schema_era"] for r in out}
    assert labelled == {str(n): era_for(d).value for n, d in enumerate(boundaries)}


def test_content_hash_does_not_depend_on_the_session_timezone(spark: SparkSession) -> None:
    """The dedup key must be a property of the event, not of the cluster."""
    original = spark.conf.get("spark.sql.session.timeZone", "UTC")
    assert original is not None
    try:
        ids = set()
        for tz in ("UTC", "America/New_York", "Asia/Kolkata"):
            spark.conf.set("spark.sql.session.timeZone", tz)
            ids.add(one(normalize_events(raw(spark, LEGACY)))["event_id"])
        assert len(ids) == 1, "the content hash moved with the session timezone"
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)


def test_nulls_hold_their_position_in_the_content_hash(spark: SparkSession) -> None:
    """Two different events, each null in a different field, are not one event."""
    no_actor: RawRow = (
        "2014-06-12T03:00:00-07:00",
        None,
        5,
        "o/r",
        "PushEvent",
        None,
        None,
        INGESTED,
    )
    no_repo: RawRow = (
        "2014-06-12T03:00:00-07:00",
        "5",
        None,
        "o/r",
        "PushEvent",
        None,
        None,
        INGESTED,
    )
    ids = [r["event_id"] for r in normalize_events(raw(spark, no_actor, no_repo)).collect()]
    assert len(set(ids)) == 2


def test_same_actor_repo_second_and_type_are_still_two_events(spark: SparkSession) -> None:
    """The collision measured in a real legacy hour, pinned."""
    push_a: RawRow = (
        "2014-06-12T14:07:42-07:00",
        "oschettler",
        18377459,
        "oschettler/allesuns",
        "PushEvent",
        None,
        "https://github.com/oschettler/allesuns/compare/19d1cb231c...c59c2cc6f1",
        INGESTED,
    )
    push_b: RawRow = (
        "2014-06-12T14:07:42-07:00",
        "oschettler",
        18377459,
        "oschettler/allesuns",
        "PushEvent",
        None,
        "https://github.com/oschettler/allesuns/compare/f8ccc599d0...19d1cb231c",
        INGESTED,
    )
    ids = {r["event_id"] for r in normalize_events(raw(spark, push_a, push_b)).collect()}
    assert len(ids) == 2


def _issue_comment(spark: SparkSession, created_at: str, *, on_a_pr: bool) -> DataFrame:
    row: RawRow = (created_at, "abc", 1, "o/r", "IssueCommentEvent", "1", None, INGESTED)
    return raw(spark, row).withColumn("issue_is_pr", F.lit(on_a_pr))


def test_legacy_issue_comments_report_unknown_not_false(spark: SparkSession) -> None:
    """`issue.pull_request` does not exist before 2015, so neither does the answer."""
    legacy = one(
        normalize_events(_issue_comment(spark, "2014-06-12T03:00:00-07:00", on_a_pr=False))
    )
    assert legacy["is_pr_comment"] is None


def test_modern_issue_comments_report_the_measured_answer(spark: SparkSession) -> None:
    on_pr = one(normalize_events(_issue_comment(spark, "2025-08-13T14:00:00Z", on_a_pr=True)))
    on_issue = one(normalize_events(_issue_comment(spark, "2025-08-13T14:00:00Z", on_a_pr=False)))
    assert on_pr["is_pr_comment"] is True
    assert on_issue["is_pr_comment"] is False


def test_non_comment_events_have_no_pr_comment_answer(spark: SparkSession) -> None:
    """The question does not apply to a push, so the column must not answer it."""
    pushed = raw(spark, MODERN).withColumn("issue_is_pr", F.lit(False))
    assert one(normalize_events(pushed))["is_pr_comment"] is None
