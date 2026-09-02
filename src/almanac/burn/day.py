"""One day of the archive through fetch + Bronze + Silver, timed per stage."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from almanac.burn.context import BurnContext
from almanac.extract.archive import fetch_hours
from almanac.extract.outcome import FetchResult, FetchStatus
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.gaps import GapReport, expected_hours
from almanac.pipeline.silver import run_silver

_BYTES_PER_GB = 1024**3


def gb_per_hour(gb: float, seconds: float) -> float | None:
    """GB-gz per hour at the observed rate; None when nothing ran."""
    return round(gb / (seconds / 3600), 2) if seconds > 0 else None


@dataclass(frozen=True)
class LayerTimings:
    fetch_seconds: float
    bronze_seconds: float
    silver_seconds: float

    @property
    def total_seconds(self) -> float:
        return self.fetch_seconds + self.bronze_seconds + self.silver_seconds


@dataclass(frozen=True)
class DayResult:
    day: date
    gaps: GapReport
    rows_bronze: int
    rows_clean: int
    rows_quarantined: int
    compressed_gb: float
    timings: LayerTimings

    def summary(self) -> dict[str, object]:
        """JSON-ready per-day record for the checkpoint and the log line."""
        return {
            "hours_expected": self.gaps.expected,
            "hours_present": self.gaps.present,
            "hours_missing": [h.isoformat() for h in self.gaps.missing],
            "rows_bronze": self.rows_bronze,
            "rows_clean": self.rows_clean,
            "rows_quarantined": self.rows_quarantined,
            "compressed_gb": round(self.compressed_gb, 4),
            "fetch_seconds": round(self.timings.fetch_seconds, 1),
            "bronze_seconds": round(self.timings.bronze_seconds, 1),
            "silver_seconds": round(self.timings.silver_seconds, 1),
        }


def process_day(spark: SparkSession, day: date, ctx: BurnContext) -> DayResult:
    """Fetch, land Bronze, run Silver for one day; idempotent per replaceWhere."""
    # One clock read for the whole day: a per-hour read makes a replay produce
    # different rows (design doc §4.1b).
    ingested_at = ctx.ingested_at or datetime.now(UTC)
    hours = expected_hours(day, day)

    started = time.monotonic()
    fetched = fetch_hours(
        ((h.date(), h.hour) for h in hours),
        client=ctx.client,
        dest_dir=ctx.paths.staging,
        settings=ctx.settings,
    )
    fetch_seconds = time.monotonic() - started

    paired = list(zip(hours, fetched, strict=True))
    present = {hour for hour, result in paired if result.status is FetchStatus.OK}
    gaps = GapReport.build(hours, present)
    compressed_gb = sum(r.bytes_downloaded for r in fetched) / _BYTES_PER_GB

    started = time.monotonic()
    _land_bronze(spark, paired, ctx.paths.bronze, ingested_at=ingested_at)
    rows_bronze = (
        spark.read.format("delta")
        .load(ctx.paths.bronze)
        .filter(F.col("event_date") == day.isoformat())
        .count()
    )
    bronze_seconds = time.monotonic() - started

    started = time.monotonic()
    clean, quarantined = run_silver(
        spark, ctx.paths.bronze, ctx.paths.silver, event_date=day.isoformat(), config=ctx.config
    )
    rows_clean, rows_quarantined = clean.count(), quarantined.count()
    silver_seconds = time.monotonic() - started

    return DayResult(
        day=day,
        gaps=gaps,
        rows_bronze=rows_bronze,
        rows_clean=rows_clean,
        rows_quarantined=rows_quarantined,
        compressed_gb=compressed_gb,
        timings=LayerTimings(fetch_seconds, bronze_seconds, silver_seconds),
    )


def _land_bronze(
    spark: SparkSession,
    paired: list[tuple[datetime, FetchResult]],
    bronze_path: str,
    *,
    ingested_at: datetime,
) -> None:
    for hour, result in paired:
        if result.status is not FetchStatus.OK or result.path is None:
            continue
        date_str = hour.date().isoformat()
        # read.text, not read.json: per-file inference disagrees between hours
        # of one day (Task 7), and parsing is a transform Bronze may not do.
        raw = spark.read.text(str(result.path)).withColumnRenamed("value", "raw_json")
        stamped = add_ingestion_metadata(raw, ingested_at=ingested_at, source_file=result.url)
        partitioned = stamped.withColumn("event_date", F.lit(date_str)).withColumn(
            "event_hour", F.lit(hour.hour)
        )
        write_bronze(partitioned, bronze_path, event_date=date_str, event_hour=hour.hour)
