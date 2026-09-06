"""Streaming ingest: watermark, dedup, and the era assertion. No network."""

from datetime import timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from almanac.explore.schema import SchemaEra
from almanac.stream.ingest import (
    _reject_non_reduced_era,
    late_event_count,
    stream_events,
    write_stream_silver,
)
from tests.helpers import land_poll, one, run_streaming_query

pytestmark = pytest.mark.spark

_EVENT = {
    "id": "1",
    "type": "PushEvent",
    "created_at": "2026-09-06T12:00:00Z",
    "repo": {"id": 42, "name": "acme/almanac"},
}


def _event(event_id: str, created_at: str) -> dict[str, object]:
    return {**_EVENT, "id": event_id, "created_at": created_at}


def _ingest(
    spark: SparkSession, landing: Path, dest: Path, checkpoint: Path, *, watermark: timedelta
) -> StreamingQuery:
    """One bounded pass over whatever the landing dir currently holds."""
    stream = stream_events(spark, str(landing), watermark=watermark)
    query = write_stream_silver(stream, str(dest), str(checkpoint), available_now=True)
    run_streaming_query(query)
    return query


# --- dedup: the same key Silver's batch dedup uses, within the watermark ---


def test_dedups_on_event_id_within_watermark(spark: SparkSession, tmp_path: Path) -> None:
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    land_poll(
        landing,
        [_event("1", "2026-09-06T12:00:00Z"), _event("1", "2026-09-06T12:00:00Z")],
        polled_at="2026-09-06T12:00:01Z",
        name="poll_0",
    )

    _ingest(spark, landing, dest, checkpoint, watermark=timedelta(minutes=10))

    assert spark.read.format("delta").load(str(dest)).count() == 1


def test_distinct_events_are_both_kept(spark: SparkSession, tmp_path: Path) -> None:
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    land_poll(
        landing,
        [_event("1", "2026-09-06T12:00:00Z"), _event("2", "2026-09-06T12:00:01Z")],
        polled_at="2026-09-06T12:00:02Z",
        name="poll_0",
    )

    _ingest(spark, landing, dest, checkpoint, watermark=timedelta(minutes=10))

    assert spark.read.format("delta").load(str(dest)).count() == 2


def test_event_id_and_schema_era_match_the_batch_paths_own_computation(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The dedup key is not a second copy of the fact -- it's the same function."""
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    land_poll(
        landing,
        [_event("abc123", "2026-09-06T12:00:00Z")],
        polled_at="2026-09-06T12:00:01Z",
        name="poll_0",
    )

    _ingest(spark, landing, dest, checkpoint, watermark=timedelta(minutes=10))

    row = one(spark.read.format("delta").load(str(dest)))
    assert row["event_id"] == "abc123"
    assert row["schema_era"] == SchemaEra.REDUCED_V3.value


# --- lateness: reported via observe(), because a row this old is dropped
# by dropDuplicatesWithinWatermark before it ever reaches a foreachBatch
# write -- verified directly, 2026-09-06 ---


def test_a_prompt_event_is_not_flagged_late(spark: SparkSession, tmp_path: Path) -> None:
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    land_poll(
        landing,
        [_event("1", "2026-09-06T12:00:00Z")],
        polled_at="2026-09-06T12:00:05Z",
        name="poll_0",
    )  # 5s delay, well under the watermark

    query = _ingest(spark, landing, dest, checkpoint, watermark=timedelta(seconds=60))

    assert late_event_count(query) == 0
    assert one(spark.read.format("delta").load(str(dest)))["is_late"] is False


def test_a_row_older_than_the_watermark_is_counted_even_though_it_is_dropped(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The failure mode this exists to catch: a duplicate (or any row)
    arriving after the watermark has moved past it is not written -- and
    without `observe()`, not reported anywhere either."""
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    watermark = timedelta(seconds=60)

    # Poll 1: event id=1 at T0, on time -- establishes state.
    land_poll(
        landing,
        [_event("1", "2026-01-01T00:00:00Z")],
        polled_at="2026-01-01T00:00:00Z",
        name="poll_0",
    )
    _ingest(spark, landing, dest, checkpoint, watermark=watermark)

    # Poll 2: a NEW event 2 minutes later in event time -- advances the
    # watermark well past T0 + 60s, evicting id=1's dedup state.
    land_poll(
        landing,
        [_event("2", "2026-01-01T00:02:00Z")],
        polled_at="2026-01-01T00:02:00Z",
        name="poll_1",
    )
    _ingest(spark, landing, dest, checkpoint, watermark=watermark)

    # Poll 3: id=1 again, same created_at as before (now older than the
    # watermark) -- dropDuplicatesWithinWatermark can no longer help.
    land_poll(
        landing,
        [_event("1", "2026-01-01T00:00:00Z")],
        polled_at="2026-01-01T00:03:20Z",
        name="poll_2",
    )
    query = _ingest(spark, landing, dest, checkpoint, watermark=watermark)

    assert spark.read.format("delta").load(str(dest)).count() == 2, (
        "poll 3's row is too late to land -- correct here, since it is a "
        "genuine duplicate of poll 1's row, not data loss"
    )
    assert late_event_count(query) >= 1, "the dropped row must be counted, not silently absorbed"


# --- the era assertion: the live feed cannot produce a non-REDUCED_V3 row ---


def test_reject_non_reduced_era_raises_on_a_legacy_row(spark: SparkSession) -> None:
    df = spark.createDataFrame([("legacy_v1",)], "schema_era string")
    with pytest.raises(ValueError, match="REDUCED_V3"):
        _reject_non_reduced_era(df)


def test_reject_non_reduced_era_passes_reduced_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame([("reduced_v3",), ("reduced_v3",)], "schema_era string")
    _reject_non_reduced_era(df)  # must not raise
