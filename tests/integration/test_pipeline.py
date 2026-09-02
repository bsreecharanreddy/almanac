"""Bronze -> Silver against the committed fixtures. No network."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.dedup import duplicate_stats
from almanac.pipeline.eras import normalize_events
from almanac.pipeline.payloads import parse_events
from almanac.pipeline.silver import read_bronze, run_silver
from almanac.pipeline.source import SourceConfig
from tests.helpers import build_bronze, one

pytestmark = [pytest.mark.spark, pytest.mark.integration]

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
CONFIG = SourceConfig.load(Path("conf/sources/gharchive.yml"))

# The date each fixture is landed under -- the archive file's own hour, not
# the events' UTC hour (see SILVER_COLUMNS).
ERAS = {"modern": "2025-08-13", "legacy": "2014-06-12"}


@pytest.fixture(params=sorted(ERAS))
def era(request: pytest.FixtureRequest) -> str:
    """Both eras, through the same pipeline, from the same tests."""
    return str(request.param)


@pytest.fixture
def landed(
    spark: SparkSession,
    era: str,
    modern_events_path: Path,
    legacy_events_path: Path,
    tmp_path: Path,
) -> tuple[Path, Path, str, Path]:
    """One era's fixture, landed in a real Bronze table ready for Silver."""
    source = modern_events_path if era == "modern" else legacy_events_path
    bronze, out, date = tmp_path / "bronze", tmp_path / "silver", ERAS[era]
    build_bronze(spark, source, bronze, event_date=date, event_hour=14, ingested_at=INGESTED)
    return bronze, out, date, source


def test_fixture_flows_bronze_to_silver(
    spark: SparkSession, landed: tuple[Path, Path, str, Path]
) -> None:
    bronze, out, date, _ = landed
    clean, _ = run_silver(spark, str(bronze), str(out), event_date=date, config=CONFIG)
    assert clean.count() > 0, "the fixture must produce usable rows"
    assert "_failed_rules" in clean.columns
    assert "_reject_rules" not in clean.columns


def test_every_record_is_accounted_for(
    spark: SparkSession, landed: tuple[Path, Path, str, Path]
) -> None:
    """Conservation, stated as an equation rather than an inequality."""
    bronze, out, date, source = landed
    raw = spark.read.text(str(source)).count()
    clean, quarantined = run_silver(spark, str(bronze), str(out), event_date=date, config=CONFIG)
    landed_rows = read_bronze(spark, str(bronze), event_date=date)
    removed = duplicate_stats(normalize_events(parse_events(landed_rows)))["duplicates"]
    assert clean.count() + quarantined.count() + removed == raw


def test_silver_is_idempotent(spark: SparkSession, landed: tuple[Path, Path, str, Path]) -> None:
    bronze, out, date, _ = landed
    first, _ = run_silver(spark, str(bronze), str(out), event_date=date, config=CONFIG)
    n = first.count()
    second, _ = run_silver(spark, str(bronze), str(out), event_date=date, config=CONFIG)
    assert second.count() == n
    assert spark.read.format("delta").load(f"{out}/clean").count() == n


def test_both_eras_are_labelled_and_get_an_id(
    spark: SparkSession, landed: tuple[Path, Path, str, Path]
) -> None:
    """The era-specific handling survives a real file, not just a crafted row."""
    bronze, out, date, _ = landed
    clean, _ = run_silver(spark, str(bronze), str(out), event_date=date, config=CONFIG)
    row = one(clean)
    assert row["schema_era"] in {"legacy_v1", "modern_v2", "reduced_v3"}
    assert row["event_id"]
    assert clean.filter("event_id IS NULL").count() == 0


def test_pr_columns_reach_silver(spark: SparkSession, landed: tuple[Path, Path, str, Path]) -> None:
    """Gold's inputs must actually arrive. Phase 1's Silver carried no payload."""
    bronze, out, date, _ = landed
    clean, _ = run_silver(spark, str(bronze), str(out), event_date=date, config=CONFIG)
    for column in ("pr_number", "pr_merged", "pr_draft", "is_pr_comment", "event_action"):
        assert column in clean.columns
    prs = clean.filter("event_type = 'PullRequestEvent'")
    assert prs.count() > 0
    assert prs.filter("pr_number IS NULL").count() == 0
