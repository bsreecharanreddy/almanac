"""Replay forces exactly the cases the live feed's sampled tail cannot: late
arrival, cross-batch duplication, out-of-order delivery, and a poll-cadence
gap -- and proves the replayed stream agrees with batch Silver over the same
real fixture. No network."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.eras import SILVER_COLUMNS
from almanac.pipeline.silver import run_silver
from almanac.pipeline.source import SourceConfig
from almanac.stream.replay import replay_hours
from tests.helpers import build_bronze

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_GHARCHIVE_CONFIG = Path("conf/sources/gharchive.yml")
_COMPARABLE = [c for c in SILVER_COLUMNS if c != "ingested_at"]


def _rows(spark: SparkSession, dest: Path) -> set[tuple[object, ...]]:
    return {
        tuple(row)
        for row in spark.read.format("delta").load(str(dest)).select(*_COMPARABLE).collect()
    }


def test_forces_event_after_watermark_passed(
    spark: SparkSession, tmp_path: Path, reduced_events_path: Path
) -> None:
    """After two natural cycles deliver every event on time, a trailing
    cycle redelivers all of them again, an hour late -- comfortably past the
    default 10-minute watermark. Every redelivered row is counted, and none
    of them duplicate what natural delivery already wrote."""
    dest = tmp_path / "dest"
    stats = replay_hours(
        spark, ["a", "b"], str(dest), source=reduced_events_path, lateness=timedelta(hours=1)
    )

    assert stats.late_events == stats.events_replayed
    assert spark.read.format("delta").load(str(dest)).count() == stats.events_replayed


def test_forces_duplicate_across_micro_batches(
    spark: SparkSession, tmp_path: Path, reduced_events_path: Path
) -> None:
    """Half of cycle 0's events reappear in cycle 1's landing file -- a
    duplicate spanning two separate micro-batches, the case
    `dropDuplicatesWithinWatermark`'s cross-batch state exists for."""
    dest = tmp_path / "dest"
    stats = replay_hours(
        spark, ["a", "b"], str(dest), source=reduced_events_path, duplicate_rate=0.5, seed=1
    )

    assert stats.events_landed > stats.events_replayed, "some events were landed twice"
    assert spark.read.format("delta").load(str(dest)).count() == stats.events_replayed, (
        "the duplicate copies must not inflate the written table"
    )


def test_forces_out_of_order_within_batch(
    spark: SparkSession, tmp_path: Path, reduced_events_path: Path
) -> None:
    """Shuffling the order events are written to one poll file must not
    change the result -- the watermark is computed over a batch's max event
    time, not its arrival order."""
    ordered = tmp_path / "ordered"
    shuffled = tmp_path / "shuffled"
    replay_hours(spark, ["only"], str(ordered), source=reduced_events_path, shuffle=False)
    replay_hours(spark, ["only"], str(shuffled), source=reduced_events_path, shuffle=True, seed=7)

    assert _rows(spark, ordered) == _rows(spark, shuffled)


def test_forces_gap_and_observes_recovery(
    spark: SparkSession, tmp_path: Path, reduced_events_path: Path
) -> None:
    """Three poll cycles against the same checkpoint -- each transition is a
    real restart, standing in for the poller having gone quiet between them.
    Recovery means no loss and nothing double-counted once it resumes."""
    dest = tmp_path / "dest"
    stats = replay_hours(spark, ["a", "b", "c"], str(dest), source=reduced_events_path)

    assert stats.late_events == 0
    assert spark.read.format("delta").load(str(dest)).count() == stats.events_replayed


def test_replayed_stream_equals_batch_silver(
    spark: SparkSession, tmp_path: Path, reduced_events_path: Path
) -> None:
    """The gate: a faithful, undisturbed replay of the real fixture must
    reproduce exactly what one batch Silver run over the same fixture
    produces -- the two paths share `parse_events`/`normalize_events`, so
    disagreement here means one of them is wrong."""
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    build_bronze(
        spark,
        reduced_events_path,
        bronze_path,
        event_date="2025-11-03",
        event_hour=14,
        ingested_at=datetime(2026, 9, 2, tzinfo=UTC),
    )
    config = SourceConfig.load(_GHARCHIVE_CONFIG)
    clean, quarantined = run_silver(
        spark, str(bronze_path), str(silver_path), event_date="2025-11-03", config=config
    )
    assert quarantined.count() == 0, "a quarantined row would need its own handling in the gate"

    replay_dest = tmp_path / "replayed"
    stats = replay_hours(spark, ["only"], str(replay_dest), source=reduced_events_path)

    batch_rows = {tuple(row) for row in clean.select(*_COMPARABLE).collect()}
    assert stats.late_events == 0
    assert batch_rows == _rows(spark, replay_dest)
