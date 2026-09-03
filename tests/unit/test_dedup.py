from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from almanac.pipeline.dedup import deduplicate, duplicate_stats
from tests.helpers import epoch_of, one

pytestmark = pytest.mark.spark

SCHEMA = (
    "event_id string, created_at timestamp, event_hour int, repo_id long, ingested_at timestamp"
)

type DedupRow = tuple[str | None, datetime, int, int, datetime]

T14 = datetime(2025, 8, 13, 14, 59, tzinfo=UTC)
T15 = datetime(2025, 8, 13, 15, 0, tzinfo=UTC)
T = datetime(2025, 8, 13, 14, 0, tzinfo=UTC)


def rows(spark: SparkSession, *data: DedupRow) -> DataFrame:
    return spark.createDataFrame(list(data), SCHEMA)


def test_exact_duplicate_within_one_hour_removed(spark: SparkSession) -> None:
    df = rows(spark, ("e1", T, 14, 1, T), ("e1", T, 14, 1, T))
    assert deduplicate(df).count() == 1


def test_duplicate_across_adjacent_hours_removed(spark: SparkSession) -> None:
    """Design doc §12 trap 4 -- the case Phase 0 measured but never tested."""
    df = rows(spark, ("e1", T14, 14, 1, T14), ("e1", T15, 15, 1, T15))
    assert deduplicate(df).count() == 1


def test_earliest_occurrence_is_the_one_kept(spark: SparkSession) -> None:
    # Event time is the source of truth. Keeping the later copy would
    # inflate every response-latency measurement built on it.
    df = rows(spark, ("e1", T15, 15, 1, T15), ("e1", T14, 14, 1, T14))
    assert epoch_of(deduplicate(df), "created_at") == int(T14.timestamp())


def test_ingestion_time_breaks_a_tie_on_event_time(spark: SparkSession) -> None:
    """Two copies stamped with the same event time must still resolve."""
    early = datetime(2025, 8, 13, 16, 0, tzinfo=UTC)
    late = datetime(2025, 8, 13, 18, 0, tzinfo=UTC)
    df = rows(spark, ("e1", T, 14, 1, late), ("e1", T, 14, 2, early))
    assert one(deduplicate(df))["repo_id"] == 2


def test_distinct_events_are_untouched(spark: SparkSession) -> None:
    df = rows(spark, ("e1", T, 14, 1, T), ("e2", T, 14, 1, T))
    assert deduplicate(df).count() == 2


def test_every_column_of_the_kept_row_survives(spark: SparkSession) -> None:
    """Dedup must not quietly reshape the rows it keeps."""
    df = rows(spark, ("e1", T14, 14, 7, T14), ("e1", T15, 15, 7, T15))
    out = deduplicate(df)
    assert out.columns == df.columns
    kept = one(out)
    assert (kept["event_id"], kept["event_hour"], kept["repo_id"]) == ("e1", 14, 7)


def test_stats_report_what_was_removed(spark: SparkSession) -> None:
    df = rows(spark, ("e1", T, 14, 1, T), ("e1", T, 14, 1, T), ("e2", T, 14, 1, T))
    stats = duplicate_stats(df)
    assert stats["total"] == 3
    assert stats["distinct"] == 2
    assert stats["duplicates"] == 1


def test_events_without_an_id_are_never_collapsed(spark: SparkSession) -> None:
    """Four distinct id-less events are four events, not one."""
    idless: list[DedupRow] = [(None, T, 14, r, T) for r in (1, 2, 3, 4)]
    out = deduplicate(rows(spark, *idless))
    assert out.count() == 4
    assert sorted(r["repo_id"] for r in out.collect()) == [1, 2, 3, 4]
