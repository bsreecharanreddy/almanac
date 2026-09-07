"""Streaming ingest: watermark, dedup, and the era assertion. No network."""

from datetime import timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from almanac.explore.schema import SchemaEra
from almanac.stream.ingest import (
    _reject_id_less_event,
    late_event_count,
    non_reduced_count,
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
    # Present on purpose: `actor` is exactly the field `payloads.EVENT_SCHEMA`
    # omits (its type varies pre/post 2015), so it only survives the landing
    # zone's round trip if `event` is carried there as a raw string rather
    # than a struct typed by that schema -- a real bug Task 4's
    # batch-equality gate caught 2026-09-06, invisible to every test below
    # until this field was added.
    "actor": {"login": "alice"},
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


def test_actor_login_survives_the_landing_zone_round_trip(
    spark: SparkSession, tmp_path: Path
) -> None:
    """`actor` is exactly the field `payloads.EVENT_SCHEMA` omits (its type
    varies pre/post 2015); it only comes through if the landing zone carries
    `event` as a raw string rather than a struct typed by that schema."""
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    land_poll(
        landing,
        [_event("1", "2026-09-06T12:00:00Z")],
        polled_at="2026-09-06T12:00:01Z",
        name="poll_0",
    )

    _ingest(spark, landing, dest, checkpoint, watermark=timedelta(minutes=10))

    assert one(spark.read.format("delta").load(str(dest)))["actor_login"] == "alice"


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


# --- the id assertion: an event dedup cannot deduplicate is a real anomaly ---


def test_reject_id_less_event_raises_when_event_id_is_null(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], "event_id string")
    with pytest.raises(ValueError, match="no event_id"):
        _reject_id_less_event(df)


def test_reject_id_less_event_passes_rows_that_carry_an_id(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a",), ("b",)], "event_id string")
    _reject_id_less_event(df)  # must not raise


def test_an_old_era_row_is_ingested_rather_than_rejected(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The live feed really does deliver them: 3 of 6801 events in a 30-poll
    window on 2026-09-07 were `modern_v2`, one created 2021-07-30. They carry
    valid ids, and `pipeline.eras` already handles every era, so rejecting
    them lost real data for no invariant."""
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    land_poll(
        landing,
        [_event("1", "2021-07-30T18:12:32Z"), _event("2", "2026-09-06T12:00:00Z")],
        polled_at="2026-09-06T12:00:05Z",
        name="poll_0",
    )

    query = _ingest(spark, landing, dest, checkpoint, watermark=timedelta(minutes=10))

    written = spark.read.format("delta").load(str(dest)).collect()
    eras = {r["event_id"]: r["schema_era"] for r in written}
    assert eras == {"1": SchemaEra.MODERN_V2.value, "2": SchemaEra.REDUCED_V3.value}
    assert non_reduced_count(query) == 1, "the rate must be reported, not assumed"


def test_a_distinct_late_event_is_dropped_not_just_deduplicated(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The neighbouring test covers a late *duplicate*, where dropping costs
    nothing. This is the case that does cost: a never-before-seen event whose
    event time is already behind the watermark is silently absent from Silver.
    On the live feed that is not exotic -- 16.4% of a 30-poll window carried an
    event time over a day old (2026-09-07)."""
    landing, dest, checkpoint = tmp_path / "landing", tmp_path / "dest", tmp_path / "checkpoint"
    landing.mkdir()
    watermark = timedelta(seconds=60)

    land_poll(
        landing, [_event("1", "2026-01-01T00:00:00Z")], polled_at="2026-01-01T00:00:00Z", name="p0"
    )
    _ingest(spark, landing, dest, checkpoint, watermark=watermark)

    # Advances the watermark well past T0 + 60s.
    land_poll(
        landing, [_event("2", "2026-01-01T00:02:00Z")], polled_at="2026-01-01T00:02:00Z", name="p1"
    )
    _ingest(spark, landing, dest, checkpoint, watermark=watermark)

    land_poll(
        landing,
        [_event("999", "2025-06-09T15:12:47Z")],
        polled_at="2026-01-01T00:03:20Z",
        name="p2",
    )
    query = _ingest(spark, landing, dest, checkpoint, watermark=watermark)

    ids = {r["event_id"] for r in spark.read.format("delta").load(str(dest)).collect()}
    assert ids == {"1", "2"}, "event 999 is new, not a duplicate -- its absence is data loss"
    assert late_event_count(query) >= 1, "at least it is counted"
