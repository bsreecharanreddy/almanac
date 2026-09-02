"""Bronze -> Silver against the committed fixtures. No network.

The unit suites test each stage in isolation and every one of them was
green while `normalize_events` and `deduplicate` could not actually be
composed -- the declared Silver order was unbuildable and nothing noticed
until these tests existed. That is what this file is for.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.bronze import add_ingestion_metadata
from almanac.pipeline.dedup import duplicate_stats
from almanac.pipeline.eras import normalize_events
from almanac.pipeline.silver import read_events, run_silver
from almanac.pipeline.source import SourceConfig
from tests.helpers import one

pytestmark = [pytest.mark.spark, pytest.mark.integration]

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
CONFIG = SourceConfig.load(Path("conf/sources/gharchive.yml"))


@pytest.fixture(params=["modern", "legacy"])
def fixture_path(
    request: pytest.FixtureRequest, modern_events_path: Path, legacy_events_path: Path
) -> Path:
    """Both eras, through the same pipeline, from the same tests."""
    return modern_events_path if request.param == "modern" else legacy_events_path


def test_fixture_flows_bronze_to_silver(
    spark: SparkSession, fixture_path: Path, tmp_path: Path
) -> None:
    clean, _ = run_silver(
        spark, str(fixture_path), str(tmp_path), ingested_at=INGESTED, config=CONFIG
    )
    assert clean.count() > 0, "the fixture must produce usable rows"
    assert "_failed_rules" in clean.columns
    assert "_reject_rules" not in clean.columns


def test_every_record_is_accounted_for(
    spark: SparkSession, fixture_path: Path, tmp_path: Path
) -> None:
    """Conservation, stated as an equation rather than an inequality.

    `clean + quarantined <= raw` is not conservation -- dropping every row
    satisfies it. The only rows the pipeline may remove are duplicates, so
    the count it removes has to be named and added back.
    """
    raw = spark.read.json(str(fixture_path)).count()
    clean, quarantined = run_silver(
        spark, str(fixture_path), str(tmp_path), ingested_at=INGESTED, config=CONFIG
    )
    stamped = add_ingestion_metadata(
        read_events(spark, str(fixture_path)),
        ingested_at=INGESTED,
        source_file=str(fixture_path),
    )
    removed = duplicate_stats(normalize_events(stamped))["duplicates"]
    assert clean.count() + quarantined.count() + removed == raw


def test_silver_is_idempotent(spark: SparkSession, fixture_path: Path, tmp_path: Path) -> None:
    first, _ = run_silver(
        spark, str(fixture_path), str(tmp_path), ingested_at=INGESTED, config=CONFIG
    )
    n = first.count()
    second, _ = run_silver(
        spark, str(fixture_path), str(tmp_path), ingested_at=INGESTED, config=CONFIG
    )
    assert second.count() == n
    assert spark.read.format("delta").load(f"{tmp_path}/clean").count() == n


def test_both_eras_are_labelled_and_get_an_id(
    spark: SparkSession, fixture_path: Path, tmp_path: Path
) -> None:
    """The era-specific handling survives a real file, not just a crafted row."""
    clean, _ = run_silver(
        spark, str(fixture_path), str(tmp_path), ingested_at=INGESTED, config=CONFIG
    )
    row = one(clean)
    assert row["schema_era"] in {"legacy_v1", "modern_v2", "reduced_v3"}
    assert row["event_id"]
    assert clean.filter("event_id IS NULL").count() == 0


def test_legacy_repo_names_are_qualified_like_modern_ones(
    spark: SparkSession, legacy_events_path: Path
) -> None:
    """Legacy splits owner and name; modern ships `owner/repo` already.

    Leaving legacy unreconstructed would make `repo_name` mean two different
    things either side of 2015, and Phase 2's SCD2 would read the era
    boundary as a mass rename of every repo that survived it.
    """
    names = read_events(spark, str(legacy_events_path)).filter("repo_name IS NOT NULL")
    assert names.count() > 0
    assert names.filter("repo_name NOT LIKE '%/%'").count() == 0
