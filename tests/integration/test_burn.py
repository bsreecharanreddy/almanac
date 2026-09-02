"""``process_day`` and ``backfill`` against the committed fixtures. No network."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
from pyspark.sql import SparkSession

from almanac.burn.backfill import backfill
from almanac.burn.checkpoint import BackfillCheckpoint
from almanac.burn.context import BurnContext, LakePaths
from almanac.burn.day import process_day
from almanac.config import Settings
from almanac.pipeline.source import SourceConfig

pytestmark = [pytest.mark.spark, pytest.mark.integration]

CONFIG = SourceConfig.load(Path("conf/sources/gharchive.yml"))
DAY = date(2025, 8, 13)
PINNED = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)


@pytest.fixture
def archive_client(modern_events_path: Path) -> Iterator[httpx.Client]:
    """Serves the modern fixture for 2025-08-13 hour 14; 404 for every other hour."""
    body = modern_events_path.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/2025-08-13-14.json.gz":
            return httpx.Response(200, content=body)
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        yield client


def _context(client: httpx.Client, root: Path) -> BurnContext:
    return BurnContext(
        paths=LakePaths.under(root),
        config=CONFIG,
        client=client,
        settings=Settings(),
        ingested_at=PINNED,
    )


def test_process_day_lands_bronze_and_silver_and_reports_the_gap(
    spark: SparkSession, archive_client: httpx.Client, tmp_path: Path
) -> None:
    result = process_day(spark, DAY, _context(archive_client, tmp_path))

    assert result.rows_bronze > 0
    assert result.rows_clean > 0
    assert result.compressed_gb > 0
    assert result.timings.total_seconds >= 0

    assert result.gaps.expected == 24
    assert result.gaps.present == 1
    assert len(result.gaps.missing) == 23

    landed = spark.read.format("delta").load(str(tmp_path / "silver" / "clean"))
    assert landed.count() == result.rows_clean


def test_backfill_checkpoints_the_day_skips_it_on_a_rerun_and_reports_per_layer(
    spark: SparkSession, archive_client: httpx.Client, tmp_path: Path
) -> None:
    ctx = _context(archive_client, tmp_path)
    checkpoint = BackfillCheckpoint(tmp_path / "cp")

    first = backfill(spark, DAY, DAY, ctx, checkpoint)
    assert first.days_processed == 1
    assert first.days_skipped == 0
    assert checkpoint.completed() == {DAY}
    assert list((tmp_path / "staging").glob("*.json.gz")) == []  # cleared once Bronze holds it
    assert len(first.hours_missing) == 23

    summary = first.summary()
    assert summary["rows_clean"] == first.results[0].rows_clean
    assert "gb_per_bronze_hour" in summary
    assert "gb_per_silver_hour" in summary

    again = backfill(spark, DAY, DAY, ctx, checkpoint)
    assert again.days_processed == 0
    assert again.days_skipped == 1
