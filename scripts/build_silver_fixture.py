"""Land the committed fixtures through Bronze -> Silver, so Gold has real
Delta tables to select from locally and in CI (no backfill to read there).

Ephemeral output under gitignored ``data/``. Idempotent: a rerun replaces
the two eras it wrote, not accumulates them.
"""

from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from almanac.config import Settings
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.silver import run_silver
from almanac.pipeline.source import SourceConfig
from almanac.spark import local_session

# (fixture glob, event_date, event_hour) -- the test_pipeline.py slice.
_FIXTURES = (
    ("modern-*.jsonl.gz", "2025-08-13", 14),
    ("legacy-*.jsonl.gz", "2014-06-12", 14),
)

# Fixed, so a rerun is byte-for-byte reproducible.
_INGESTED_AT = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


def main() -> None:
    settings = Settings()
    config = SourceConfig.load(Path("conf/sources/gharchive.yml"))
    bronze_path = settings.data_dir / "gold_fixture" / "bronze"
    silver_path = settings.data_dir / "gold_fixture" / "silver"

    spark = local_session("almanac-gold-fixture")
    for pattern, event_date, event_hour in _FIXTURES:
        matches = sorted(settings.fixture_dir.glob(pattern))
        if not matches:
            raise SystemExit(f"no fixture matching {pattern}; run `make fixtures`")

        raw = spark.read.text(str(matches[0])).withColumnRenamed("value", "raw_json")
        stamped = add_ingestion_metadata(raw, ingested_at=_INGESTED_AT, source_file=str(matches[0]))
        partitioned = stamped.withColumn("event_date", F.lit(event_date)).withColumn(
            "event_hour", F.lit(event_hour)
        )
        write_bronze(partitioned, str(bronze_path), event_date=event_date, event_hour=event_hour)

        run_silver(spark, str(bronze_path), str(silver_path), event_date=event_date, config=config)
        print(f"landed {pattern} -> silver ({event_date})")


if __name__ == "__main__":
    main()
