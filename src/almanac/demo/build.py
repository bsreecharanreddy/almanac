"""Fixtures to committed artifacts. The only demo module that imports Spark.

Runs at build time, never at view time: the app reads what this writes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.config import Settings
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.silver import run_silver
from almanac.pipeline.source import SourceConfig

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEMO_DATA_DIR = _REPO_ROOT / "demo" / "data"

# (fixture glob, event_date, event_hour). All three eras, unlike
# scripts/build_silver_fixture.py's two: the schema break is the point.
ERAS: tuple[tuple[str, str, int], ...] = (
    ("modern-*.jsonl.gz", "2025-08-13", 14),
    ("legacy-*.jsonl.gz", "2014-06-12", 14),
    ("reduced-*.jsonl.gz", "2025-11-03", 14),
)

# Fixed, so a rerun is byte-for-byte reproducible -- the same constant
# scripts/build_silver_fixture.py uses, and for the same reason.
_INGESTED_AT = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


def _land_era(
    spark: SparkSession, *, glob: str, event_date: str, event_hour: int, root: Path
) -> dict[str, Any]:
    """One era through Bronze and Silver. Returns its counts."""
    settings = Settings()
    config = SourceConfig.load(_REPO_ROOT / "conf" / "sources" / "gharchive.yml")
    matches = sorted(settings.fixture_dir.glob(glob))
    if not matches:
        raise SystemExit(f"no fixture matching {glob}; run `make fixtures`")

    bronze, silver = root / "bronze", root / "silver"
    raw = spark.read.text(str(matches[0])).withColumnRenamed("value", "raw_json")
    stamped = add_ingestion_metadata(raw, ingested_at=_INGESTED_AT, source_file=str(matches[0]))
    partitioned = stamped.withColumn("event_date", F.lit(event_date)).withColumn(
        "event_hour", F.lit(event_hour)
    )
    write_bronze(partitioned, str(bronze), event_date=event_date, event_hour=event_hour)
    run_silver(spark, str(bronze), str(silver), event_date=event_date, config=config)

    def _count(path: Path) -> int:
        frame: DataFrame = spark.read.format("delta").load(str(path))
        return frame.where(F.col("event_date") == event_date).count()

    bronze_rows = (
        spark.read.format("delta")
        .load(str(bronze))
        .where(F.col("event_date") == event_date)
        .count()
    )
    silver_rows = _count(silver / "clean")
    quarantine_rows = _count(silver / "quarantine")
    return {
        "event_date": event_date,
        "event_hour": event_hour,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        # Asserted by the caller, never assumed -- a NULL rule condition
        # would otherwise drop a record from both sides silently.
        "scored_rows": silver_rows + quarantine_rows,
    }


def build_medallion(spark: SparkSession, *, out_dir: Path) -> dict[str, Any]:
    """Land every era, write `medallion.json`, return what it wrote."""
    out_dir.mkdir(parents=True, exist_ok=True)
    eras = [
        _land_era(spark, glob=g, event_date=d, event_hour=h, root=out_dir / "lake")
        for g, d, h in ERAS
    ]
    summary = {
        "scale": {
            "hours_per_era": 1,
            "note": (
                "One archived hour per schema era. The platform's measured "
                "backfill is 341,060,851 rows over Q3 2025."
            ),
        },
        "eras": eras,
    }
    (out_dir / "medallion.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary
