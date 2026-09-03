"""The checkpointed loop over Tier 3's derived span (the full Q3 2025 quarter)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import SparkSession

from almanac.burn.checkpoint import BackfillCheckpoint, pending_days
from almanac.burn.context import BurnContext
from almanac.burn.day import DayResult, gb_per_hour, process_day


@dataclass(frozen=True)
class BackfillReport:
    days_requested: int
    days_skipped: int
    results: list[DayResult] = field(default_factory=list)

    @property
    def days_processed(self) -> int:
        return len(self.results)

    @property
    def compressed_gb(self) -> float:
        return sum(r.compressed_gb for r in self.results)

    @property
    def hours_missing(self) -> list[str]:
        return [h.isoformat() for r in self.results for h in r.gaps.missing]

    def summary(self) -> dict[str, object]:
        fetch = sum(r.timings.fetch_seconds for r in self.results)
        bronze = sum(r.timings.bronze_seconds for r in self.results)
        silver = sum(r.timings.silver_seconds for r in self.results)
        gb = self.compressed_gb
        return {
            "days_requested": self.days_requested,
            "days_processed": self.days_processed,
            "days_skipped": self.days_skipped,
            "rows_bronze": sum(r.rows_bronze for r in self.results),
            "rows_clean": sum(r.rows_clean for r in self.results),
            "rows_quarantined": sum(r.rows_quarantined for r in self.results),
            "compressed_gb": round(gb, 3),
            "hours_missing": self.hours_missing,
            "fetch_seconds": round(fetch, 1),
            "bronze_seconds": round(bronze, 1),
            "silver_seconds": round(silver, 1),
            "gb_per_bronze_hour": gb_per_hour(gb, bronze),
            "gb_per_silver_hour": gb_per_hour(gb, silver),
        }


def backfill(
    spark: SparkSession,
    start: date,
    end: date,
    ctx: BurnContext,
    checkpoint: BackfillCheckpoint,
) -> BackfillReport:
    """Process every not-yet-done day in [start, end], checkpointing each."""
    done = checkpoint.completed()
    todo = pending_days(start, end, done)
    requested = (end - start).days + 1

    results: list[DayResult] = []
    for day in todo:
        result = process_day(spark, day, ctx)
        checkpoint.record(day, result.summary())
        _clear_downloads(ctx, day)
        results.append(result)

    return BackfillReport(
        days_requested=requested, days_skipped=requested - len(todo), results=results
    )


def _clear_downloads(ctx: BurnContext, day: date) -> None:
    # At ~2 GB/day the staging area would otherwise reach the quarter's ~185 GB.
    for path in ctx.paths.staging.glob(f"{day.isoformat()}-*.json.gz"):
        path.unlink()
