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
from almanac.demo.artifacts import DEMO_DATA_DIR
from almanac.demo.champion import load_champion
from almanac.features.assemble import assemble_training_set
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)
from almanac.features.spine import build_pr_opened_spine
from almanac.model.train import FEATURE_COLUMNS
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.silver import run_silver
from almanac.pipeline.source import SourceConfig
from almanac.spark import local_session

_REPO_ROOT = Path(__file__).resolve().parents[3]

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


# Surrogate keys only. docs/pseudonymization.md: a login identifies a real
# person who never opted into this, and the owner half of `owner/repo` usually
# does too. The queue ranks by rank, not by name.
_PUBLISHED_COLUMNS = ["repo_id", "pr_number", "as_of_timestamp", "is_bot_author"]


def build_queue(spark: SparkSession, *, out_dir: Path) -> dict[str, Any]:
    """Score the fixture population, and measure how much of it has features at all."""
    out_dir.mkdir(parents=True, exist_ok=True)
    events = spark.read.format("delta").load(str(out_dir / "lake" / "silver" / "clean"))
    frame = assemble_training_set(
        build_pr_opened_spine(events),
        author_activity=compute_author_activity(events),
        repo_activity=compute_repo_activity(events),
        pr_static=compute_pr_static(events),
    )
    # is_bot_author is both a published (surrogate-safe) column and a model
    # feature -- keep it once, or the select below carries it twice and
    # `pdf[FEATURE_COLUMNS]` returns 11 columns for 10 names.
    published_only = [c for c in _PUBLISHED_COLUMNS if c not in FEATURE_COLUMNS]
    keep = [c for c in published_only if c in frame.columns] + FEATURE_COLUMNS
    pdf = frame.select(*keep).toPandas()

    model = load_champion()
    pdf["breach_risk"] = model.booster_.predict(pdf[FEATURE_COLUMNS].astype("float64").to_numpy())
    # toPandas() does not guarantee row order across environments (partition
    # count tracks core count, which differs machine to machine), and most
    # rows tie on breach_risk here -- an hour of fixture data carries almost
    # no prior history, so most features are null and score identically.
    # Without a deterministic tiebreak, rank among tied rows silently follows
    # whatever order Spark happened to collect them in.
    pdf = pdf.sort_values(
        ["breach_risk", "repo_id", "pr_number"], ascending=[False, True, True]
    ).reset_index(drop=True)
    pdf.insert(0, "rank", pdf.index + 1)
    pdf.to_parquet(out_dir / "queue.parquet", index=False)

    coverage = {
        "total_rows": len(pdf),
        "features": {
            column: {"non_null": int(pdf[column].notna().sum())} for column in FEATURE_COLUMNS
        },
        "why": (
            "A feature named 'to date' may read only events strictly before its "
            "own as_of. One archived hour contains almost no prior history, so "
            "most of these are null. That is point-in-time correctness working, "
            "not a defect."
        ),
    }
    (out_dir / "coverage.json").write_text(json.dumps(coverage, indent=2, sort_keys=True) + "\n")
    return coverage


def main(argv: list[str] | None = None) -> int:
    """Build every artifact the demo reads."""
    spark = local_session("almanac-demo-build")
    build_medallion(spark, out_dir=DEMO_DATA_DIR)
    build_queue(spark, out_dir=DEMO_DATA_DIR)
    return 0


if __name__ == "__main__":
    from almanac.cli import run_cli

    run_cli(main)
