"""Measure real cluster throughput on one day of data.

Deliberately minimal: one day (24 files) through Bronze only. The number
being bought is GB-gz per cluster-hour, and Bronze is enough to get it
because ingest is the volume-bound stage. Running Silver too would conflate
two rates and cost more credit for a less interpretable answer.

**Fetch and Spark time are measured separately.** A single wall-clock figure
cannot distinguish a slow cluster from a slow network, and the cost model
downstream is priced per *cluster*-hour -- so a number that silently
includes download time overstates what the cluster costs to run. Both are
reported, along with the rate implied by each.
"""

import json
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from almanac.config import Settings
from almanac.extract.archive import fetch_hour
from almanac.extract.outcome import FetchStatus
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.gaps import GapReport, expected_hours
from almanac.spark import local_session

_BYTES_PER_GB = 1024**3


def session() -> SparkSession:
    """The cluster's session on Databricks, a local one anywhere else.

    ``local_session`` would build a second, local-mode session on a real
    cluster and quietly measure the driver instead of the cluster -- which
    is the one measurement this whole task exists to avoid getting wrong.
    """
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-calibration")


def _rate(gb: float, seconds: float) -> float | None:
    return round(gb / (seconds / 3600), 2) if seconds > 0 else None


def main(day: str, bronze_path: str, staging_dir: str) -> dict[str, object]:
    d = date.fromisoformat(day)
    settings = Settings()
    spark = session()
    # Task 4's finding: a timestamp's rendering, and anything hashed from it,
    # follows this setting. Pinned rather than inherited from the cluster.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # One clock read, stamped on every row. Per-hour reads would make a
    # replay of this run produce different rows (design doc §4.1b).
    ingested_at = datetime.now(UTC)
    hours = expected_hours(d, d)
    present: set[datetime] = set()

    fetch_seconds = 0.0
    spark_seconds = 0.0
    total_bytes = 0

    with httpx.Client() as client:
        for hour in hours:
            started = time.monotonic()
            result = fetch_hour(
                hour.date(), hour.hour, client=client, dest_dir=Path(staging_dir), settings=settings
            )
            fetch_seconds += time.monotonic() - started

            # A missing hour is reported, never silently filled (§12 trap 5).
            if result.status is not FetchStatus.OK or result.path is None:
                continue
            present.add(hour)
            total_bytes += result.bytes_downloaded

            started = time.monotonic()
            # `read.text`, not `read.json`. Parsing is a transformation, and
            # Bronze never transforms -- Task 2's Bronze tests are written
            # against a `raw_json string` schema for exactly this reason.
            # It is also the only thing that works: `read.json` infers a
            # schema per file, and real hours of one day do not agree on it,
            # so hour N's write rejects hour N+1 with a schema mismatch
            # (measured 2026-09-01, run 653007078353887). Raw text has one
            # schema for every hour and every era.
            raw = spark.read.text(str(result.path)).withColumnRenamed("value", "raw_json")
            stamped = add_ingestion_metadata(raw, ingested_at=ingested_at, source_file=result.url)
            partitioned = stamped.withColumn(
                "event_date", F.lit(hour.date().isoformat())
            ).withColumn("event_hour", F.lit(hour.hour))
            write_bronze(
                partitioned, bronze_path, event_date=hour.date().isoformat(), event_hour=hour.hour
            )
            spark_seconds += time.monotonic() - started

    started = time.monotonic()
    rows = spark.read.format("delta").load(bronze_path).count()
    spark_seconds += time.monotonic() - started

    gaps = GapReport.build(hours, present)
    gb = total_bytes / _BYTES_PER_GB
    report: dict[str, object] = {
        "day": day,
        "hours_expected": gaps.expected,
        "hours_present": gaps.present,
        "hours_missing": [h.isoformat() for h in gaps.missing],
        "rows": rows,
        "compressed_gb": round(gb, 3),
        "fetch_seconds": round(fetch_seconds, 1),
        "spark_seconds": round(spark_seconds, 1),
        "wall_clock_seconds": round(fetch_seconds + spark_seconds, 1),
        "rows_per_spark_second": round(rows / spark_seconds, 1) if spark_seconds else None,
        # The number this task exists to produce. The second is what a naive
        # single-timer measurement would have reported instead.
        "gb_per_cluster_hour": _rate(gb, spark_seconds),
        "gb_per_wall_clock_hour": _rate(gb, fetch_seconds + spark_seconds),
    }
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
