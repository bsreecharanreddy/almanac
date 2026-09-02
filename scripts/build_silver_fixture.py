"""Materialize the committed fixtures through Bronze -> Silver, for Gold.

Gold reads Silver, never a source archive or Bronze directly (design doc
§3.3). Locally and in CI there is no backfill to read from, so this script
exists purely to give Gold real Delta tables to select from -- the same
committed fixtures every other test in this repo already uses (`modern-*`,
`legacy-*`), landed through the real `almanac.pipeline` functions rather
than a shortcut.

Ephemeral output under `data/`, gitignored like the warehouse and
metastore it feeds into via `almanac.gold.runner --silver-path`. Safe to
rerun: both stages are idempotent (`replaceWhere` on their own
partitions), so a second run replaces exactly the two eras it wrote the
first time, rather than accumulating duplicates.
"""

from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from almanac.config import Settings
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.silver import run_silver
from almanac.pipeline.source import SourceConfig
from almanac.spark import local_session

# (fixture glob, event_date, event_hour) -- the same slice
# `tests/integration/test_pipeline.py` runs every era through.
_FIXTURES = (
    ("modern-*.jsonl.gz", "2025-08-13", 14),
    ("legacy-*.jsonl.gz", "2014-06-12", 14),
)

# Arbitrary but fixed, so a rerun is byte-for-byte reproducible rather than
# stamping a fresh wall-clock time every time this script happens to run.
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
