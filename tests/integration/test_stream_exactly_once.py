"""Exactly-once across a restart from checkpoint -- the streaming analogue of
Phase 1's "rerun any hour twice, byte-identical" gate. No network."""

from datetime import timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.stream.ingest import stream_events, write_stream_silver
from tests.helpers import land_poll, run_streaming_query

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_EVENT = {
    "id": "1",
    "type": "PushEvent",
    "created_at": "2026-09-06T12:00:00Z",
    "repo": {"id": 42, "name": "acme/almanac"},
}


def _event(event_id: str, created_at: str) -> dict[str, object]:
    return {**_EVENT, "id": event_id, "created_at": created_at}


def _run(spark: SparkSession, landing: Path, dest: Path, checkpoint: Path) -> None:
    stream = stream_events(spark, str(landing), watermark=timedelta(minutes=10))
    query = write_stream_silver(stream, str(dest), str(checkpoint), available_now=True)
    run_streaming_query(query)


def _rows(spark: SparkSession, dest: Path) -> set[tuple[object, ...]]:
    """Every column but `is_late`, as a comparable set -- row order is not
    part of the contract, and `is_late` depends on wall-clock ingest time,
    not on the data itself."""
    df = spark.read.format("delta").load(str(dest)).drop("is_late")
    return {tuple(row) for row in df.collect()}


def test_exactly_once_across_a_restart_from_checkpoint(spark: SparkSession, tmp_path: Path) -> None:
    """A query stopped after processing part of the landing zone, then
    restarted as a brand-new StreamingQuery object against the SAME
    checkpoint, must reproduce exactly what one uninterrupted run over the
    same data would have -- no row missing, none duplicated.
    """
    # Two poll files, same events, same polled_at either way -- the ONLY
    # difference between the two runs below is whether the query is stopped
    # and restarted between them. Giving the reference run a different poll
    # boundary (e.g. landing everything in one file) would make `ingested_at`
    # itself differ between the two tables for a reason that has nothing to
    # do with exactly-once -- caught for real the first time this test ran.
    poll_0 = [_event("1", "2026-09-06T12:00:00Z"), _event("2", "2026-09-06T12:00:05Z")]
    poll_1 = [_event("3", "2026-09-06T12:00:10Z"), _event("4", "2026-09-06T12:00:15Z")]

    # The interrupted path: two separate runs, two separate StreamingQuery
    # objects, the same checkpoint -- a real "restart", not a fresh start.
    interrupted_landing = tmp_path / "interrupted" / "landing"
    interrupted_dest = tmp_path / "interrupted" / "dest"
    interrupted_checkpoint = tmp_path / "interrupted" / "checkpoint"
    interrupted_landing.mkdir(parents=True)

    land_poll(interrupted_landing, poll_0, polled_at="2026-09-06T12:00:20Z", name="poll_0")
    _run(spark, interrupted_landing, interrupted_dest, interrupted_checkpoint)  # "crashes" here

    land_poll(interrupted_landing, poll_1, polled_at="2026-09-06T12:00:25Z", name="poll_1")
    _run(spark, interrupted_landing, interrupted_dest, interrupted_checkpoint)  # the "restart"

    # The reference: both files present from the start, one uninterrupted run.
    reference_landing = tmp_path / "reference" / "landing"
    reference_dest = tmp_path / "reference" / "dest"
    reference_checkpoint = tmp_path / "reference" / "checkpoint"
    reference_landing.mkdir(parents=True)

    land_poll(reference_landing, poll_0, polled_at="2026-09-06T12:00:20Z", name="poll_0")
    land_poll(reference_landing, poll_1, polled_at="2026-09-06T12:00:25Z", name="poll_1")
    _run(spark, reference_landing, reference_dest, reference_checkpoint)

    interrupted_rows = _rows(spark, interrupted_dest)
    reference_rows = _rows(spark, reference_dest)

    assert len(interrupted_rows) == 4, "no row lost and none duplicated across the restart"
    assert interrupted_rows == reference_rows, (
        "a restart must reproduce an uninterrupted run exactly"
    )


def test_a_restarted_query_does_not_reprocess_an_already_committed_batch(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Directly exercises the idempotent-write path: re-running against a
    checkpoint with NO new files must add nothing, proving the second
    `.start()` genuinely resumed rather than reprocessing batch 0."""
    landing = tmp_path / "landing"
    dest = tmp_path / "dest"
    checkpoint = tmp_path / "checkpoint"
    landing.mkdir()

    land_poll(
        landing,
        [_event("1", "2026-09-06T12:00:00Z")],
        polled_at="2026-09-06T12:00:01Z",
        name="poll_0",
    )
    _run(spark, landing, dest, checkpoint)
    assert spark.read.format("delta").load(str(dest)).count() == 1

    # A second "restart" against the same checkpoint and landing contents --
    # a real crash-and-restart would see this exact state (no new files yet).
    _run(spark, landing, dest, checkpoint)
    assert spark.read.format("delta").load(str(dest)).count() == 1, (
        "a restart with nothing new to process must not re-append the prior batch"
    )
