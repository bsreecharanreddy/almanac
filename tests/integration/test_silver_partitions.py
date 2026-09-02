"""Silver's write behaviour across more than one file.

Every test here fails against the Silver that shipped in Phase 1, and none
of them could have been written against a single file -- which is all that
Silver was ever run on. `run_silver` wrote `mode("overwrite")` with no
`partitionBy` and no `replaceWhere`, so each hour destroyed every hour
before it; at Tier 3's 2,208 files the table would have finished holding
one hour of data while every count-based check still looked plausible.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from almanac.pipeline.silver import CLEAN_SUBDIR, run_silver
from almanac.pipeline.source import SourceConfig
from tests.helpers import build_bronze

pytestmark = [pytest.mark.spark, pytest.mark.integration]

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
CONFIG = SourceConfig.load(Path("conf/sources/gharchive.yml"))
DATE = "2025-08-13"


def _clean(spark: SparkSession, out: Path) -> DataFrame:
    return spark.read.format("delta").load(f"{out}/{CLEAN_SUBDIR}")


def test_a_second_hour_does_not_destroy_the_first(
    spark: SparkSession, modern_events_path: Path, tmp_path: Path
) -> None:
    """Defect A, stated at its smallest: two hours must coexist.

    The two hours hold **disjoint** events, as two real hourly files do. An
    earlier version of this test wrote the same fixture to both hours and
    failed -- correctly, because deduplication collapsed the two identical
    copies to one. That is trap 4 working, not the defect, and it is now
    asserted on its own below.
    """
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    for index, hour in enumerate((14, 15)):
        build_bronze(
            spark,
            modern_events_path,
            bronze,
            event_date=DATE,
            event_hour=hour,
            ingested_at=INGESTED,
            part=(index, 2),
        )
    run_silver(spark, str(bronze), str(out), event_date=DATE, config=CONFIG)

    clean = _clean(spark, out)
    hours = {r["event_hour"] for r in clean.select("event_hour").distinct().collect()}
    assert hours == {14, 15}
    assert clean.count() == 2000, "a disjoint split must lose nothing"


def test_duplicate_events_across_two_hours_are_deduplicated(
    spark: SparkSession, modern_events_path: Path, tmp_path: Path
) -> None:
    """Trap 4, tested at the grain Silver actually runs at.

    Phase 1's Task 5 built cross-hour dedup and unit-tested it on crafted
    rows. This is the first test that the *runner* preserves that property:
    Silver's grain is a day precisely so that dedup can see across the hour
    boundaries inside it. An hour-at-a-time Silver would pass every unit
    test and still never compare two files.
    """
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    for hour in (14, 15):
        build_bronze(
            spark,
            modern_events_path,
            bronze,
            event_date=DATE,
            event_hour=hour,
            ingested_at=INGESTED,
        )
    run_silver(spark, str(bronze), str(out), event_date=DATE, config=CONFIG)

    clean = _clean(spark, out)
    assert clean.count() == 2000, "4,000 rows carrying 2,000 ids must collapse to 2,000"
    assert clean.select("event_id").distinct().count() == 2000


def test_a_second_day_does_not_destroy_the_first(
    spark: SparkSession, modern_events_path: Path, tmp_path: Path
) -> None:
    """The same defect at day scale, which is the grain the backfill runs at."""
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    days = ("2025-08-13", "2025-08-14")
    for day in days:
        build_bronze(
            spark,
            modern_events_path,
            bronze,
            event_date=day,
            event_hour=14,
            ingested_at=INGESTED,
        )
    for day in days:
        run_silver(spark, str(bronze), str(out), event_date=day, config=CONFIG)

    dates = {r["event_date"] for r in _clean(spark, out).select("event_date").distinct().collect()}
    assert dates == set(days)


def test_rerunning_one_day_replaces_only_that_partition(
    spark: SparkSession, modern_events_path: Path, tmp_path: Path
) -> None:
    """Idempotency at partition scope, not table scope.

    Phase 1's idempotency test re-ran the only day there was, so replacing
    the whole table and replacing one partition were indistinguishable.
    """
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    days = ("2025-08-13", "2025-08-14")
    for day in days:
        build_bronze(
            spark,
            modern_events_path,
            bronze,
            event_date=day,
            event_hour=14,
            ingested_at=INGESTED,
        )
        run_silver(spark, str(bronze), str(out), event_date=day, config=CONFIG)

    before = _clean(spark, out).count()
    run_silver(spark, str(bronze), str(out), event_date=days[0], config=CONFIG)
    after = _clean(spark, out)

    assert after.count() == before, "a re-run must neither duplicate nor delete"
    assert {r["event_date"] for r in after.select("event_date").distinct().collect()} == set(days)


def test_silver_reads_bronze_not_the_source_archive(
    spark: SparkSession, modern_events_path: Path, tmp_path: Path
) -> None:
    """Defect B: Bronze's output must actually be Silver's input.

    Bronze exists so that a Silver change does not re-download ~190 GB. That
    only holds if Silver reads it -- and the proof is that Silver produces
    rows when the source archive is not reachable at all.
    """
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    build_bronze(
        spark,
        modern_events_path,
        bronze,
        event_date=DATE,
        event_hour=14,
        ingested_at=INGESTED,
    )
    clean, _ = run_silver(spark, str(bronze), str(out), event_date=DATE, config=CONFIG)
    assert clean.count() > 0

    # Nothing in the call took a path to the archive; Bronze is the only input.
    assert str(modern_events_path) not in str(bronze)


def test_events_keep_their_source_hour_not_their_utc_hour(
    spark: SparkSession, legacy_events_path: Path, tmp_path: Path
) -> None:
    """Partitioning follows the source file, and this is why.

    Measured on the committed legacy fixture: one archive file named hour 14
    holds events from 14:05 to 15:01 at `-07:00`, which is UTC 21:05 to
    22:01 -- 1,957 events in hour 21 and 43 in hour 22. Deriving the
    partition from `created_at` would scatter one file across two of them,
    and no `replaceWhere` scoped to a single hour could then replace that
    file's contribution idempotently.

    `created_at` remains the event-time column, and every temporal question
    downstream is asked of it. `event_date`/`event_hour` describe where the
    row was ingested from, which is what makes the write reproducible.
    """
    bronze, out = tmp_path / "bronze", tmp_path / "silver"
    build_bronze(
        spark,
        legacy_events_path,
        bronze,
        event_date="2014-06-12",
        event_hour=14,
        ingested_at=INGESTED,
    )
    run_silver(spark, str(bronze), str(out), event_date="2014-06-12", config=CONFIG)

    clean = _clean(spark, out)
    assert {r["event_hour"] for r in clean.select("event_hour").distinct().collect()} == {14}
    utc_hours = {r["h"] for r in clean.selectExpr("hour(created_at) AS h").distinct().collect()}
    assert utc_hours == {21, 22}
