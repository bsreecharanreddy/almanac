"""Bronze -> Silver for one day of the archive. The I/O boundary.

Every transform this calls is a pure ``DataFrame -> DataFrame`` function in
a module that performs no I/O; the reads and writes live here, and only
here. The order is the one the design declares for Silver -- parse,
normalize across eras, deduplicate, then split clean from quarantined --
and it is load bearing: dedup needs the ``event_id`` only normalization can
produce, and the quality split must run last so that a rejected row is
still counted rather than having been dropped by an earlier step.

**The grain is a day, not an hour**, and that is what makes cross-hour
deduplication possible at all. Design doc §12 trap 4 is that duplicate
event ids occur across hour-file boundaries; an hour-at-a-time Silver
would dedup within each file and never see the boundary, quietly undoing
the work Phase 1's Task 5 existed to do. Duplicates spanning a *day*
boundary remain out of scope, which is a real and stated limit rather than
an accident.
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
    """One day of Bronze, as raw JSON strings plus ingestion provenance.

    Silver reads **Bronze**, never the source archive. Bronze exists so that
    anything Silver gets wrong can be recomputed without re-downloading
    ~190 GB, and that guarantee is worth exactly nothing if Silver goes back
    to the network anyway. Phase 1 shipped with `run_silver` calling
    `spark.read.json(source_path)`, which made the medallion a claim rather
    than a structure and meant Bronze's output was read by nothing.

    The filter is on a partition column, so it prunes rather than scans.
    """
    return spark.read.format("delta").load(path).filter(F.col("event_date") == event_date)


def write_silver(df: DataFrame, path: str, *, event_date: str) -> None:
    """Idempotently write exactly one day's partitions.

    ``replaceWhere`` scopes the overwrite to this day, so a retry replaces
    that day and leaves every other partition untouched. Phase 1 wrote
    ``mode("overwrite")`` with neither ``partitionBy`` nor ``replaceWhere``,
    which deleted the entire table on every file processed -- invisible at
    a sample size of one file, and at Tier 3's 2,208 files it would have
    left Silver holding a single hour while every count-based check still
    looked plausible.
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

    There is no ``ingested_at`` parameter: it was stamped by Bronze and is
    read back with the row. That removes a whole class of error rather than
    documenting it -- a second clock read here would give a replayed day
    different rows than the original run, and the re-run would still pass a
    count-based idempotency check while doing so.
    """
    raw = read_bronze(spark, bronze_path, event_date=event_date)
    checked = apply_rules(deduplicate(normalize_events(parse_events(raw))), config.quality_rules)
    clean, quarantined = split(checked)

    for df, subdir in ((clean, CLEAN_SUBDIR), (quarantined, QUARANTINE_SUBDIR)):
        write_silver(df, f"{output_path}/{subdir}", event_date=event_date)
    return clean, quarantined
