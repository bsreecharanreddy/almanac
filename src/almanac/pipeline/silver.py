"""Bronze -> Silver for one day of the archive. The I/O boundary.

Every transform this calls is a pure ``DataFrame -> DataFrame``; the reads
and writes live only here. The order -- parse, normalize, deduplicate, split
-- is load bearing: dedup needs the ``event_id`` normalization produces, and
the split runs last so a rejected row is still counted, not dropped.

The grain is a day, not an hour: §12 trap 4 duplicates cross hour-file
boundaries, so an hour-at-a-time Silver would never see them. Duplicates
spanning a day boundary stay out of scope, stated not accidental.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.pipeline.dedup import deduplicate
from almanac.pipeline.eras import normalize_events
from almanac.pipeline.payloads import parse_events
from almanac.pipeline.quality import apply_rules, split
from almanac.pipeline.source import SourceConfig

CLEAN_SUBDIR = "clean"
QUARANTINE_SUBDIR = "quarantine"


def read_bronze(spark: SparkSession, path: str, *, event_date: str) -> DataFrame:
    """One day of Bronze -- raw JSON strings plus ingestion provenance.

    Silver reads Bronze, never the source archive: Bronze exists so a Silver
    mistake is recomputable without re-downloading ~190 GB. The filter is on
    a partition column, so it prunes rather than scans.
    """
    return spark.read.format("delta").load(path).filter(F.col("event_date") == event_date)


def write_silver(df: DataFrame, path: str, *, event_date: str) -> None:
    """Idempotently write exactly one day's partitions via replaceWhere.

    Phase 1 wrote ``mode("overwrite")`` with no ``partitionBy``, deleting the
    whole table on every file -- invisible at one file, catastrophic at 2,208.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"event_date = '{event_date}'")
        .partitionBy("event_date", "event_hour")
        .save(path)
    )


def run_silver(
    spark: SparkSession,
    bronze_path: str,
    output_path: str,
    *,
    event_date: str,
    config: SourceConfig,
) -> tuple[DataFrame, DataFrame]:
    """Run one day of Bronze through to the clean and quarantined tables.

    No ``ingested_at`` parameter: it was stamped by Bronze and is read back
    with the row. A second clock read here would give a replayed day
    different rows while still passing a count-based idempotency check.
    """
    raw = read_bronze(spark, bronze_path, event_date=event_date)
    checked = apply_rules(deduplicate(normalize_events(parse_events(raw))), config.quality_rules)
    clean, quarantined = split(checked)

    for df, subdir in ((clean, CLEAN_SUBDIR), (quarantined, QUARANTINE_SUBDIR)):
        write_silver(df, f"{output_path}/{subdir}", event_date=event_date)
    return clean, quarantined
