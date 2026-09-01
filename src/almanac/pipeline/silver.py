"""Bronze -> Silver for one hourly archive file. The I/O boundary.

Every transform this calls is a pure ``DataFrame -> DataFrame`` function in
a module that performs no I/O; the reads and writes live here, and only
here. The order is the one the plan declares for Silver -- normalize across
eras, deduplicate, then split clean from quarantined -- and it is load
bearing: dedup needs the ``event_id`` only normalization can produce, and
the quality split must run last so that a rejected row is still counted
rather than having been dropped by an earlier step.
"""

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from almanac.pipeline.bronze import add_ingestion_metadata
from almanac.pipeline.dedup import deduplicate
from almanac.pipeline.eras import normalize_events
from almanac.pipeline.quality import apply_rules, split
from almanac.pipeline.source import SourceConfig

CLEAN_SUBDIR = "clean"
QUARANTINE_SUBDIR = "quarantine"


def is_legacy(df: DataFrame) -> bool:
    """Whether a parsed archive file is pre-2015, by its ``actor`` type.

    Measured in Phase 0 and confirmed against both committed fixtures:
    legacy ``actor`` is a bare login string, modern ``actor`` is an object.
    Read from the parsed schema rather than from the filename's date, so a
    file that lands under the wrong name is still read correctly rather
    than being flattened against the wrong shape.
    """
    return isinstance(df.schema["actor"].dataType, StringType)


def _raw_projection(legacy: bool) -> dict[str, Column]:
    """The era-specific mapping onto the raw contract ``normalize_events`` takes.

    Type dispatch lives at the read boundary, which is why the normalizer
    downstream can be a single branch-free shape.

    ``repo_name`` is the one field that is not a rename. Modern events carry
    ``repo.name`` already qualified as ``owner/repo``; legacy events split it
    across ``repository.owner`` and ``repository.name``, so an unreconstructed
    legacy name would mean something different from a modern one and Phase
    2's SCD2 would read the era boundary as a mass rename of every repo that
    survived it. Joined with ``concat``, which propagates nulls, rather than
    ``concat_ws``, which skips them -- a missing owner must yield no name at
    all, not a bare unqualified one masquerading as a valid repo.
    """
    if legacy:
        return {
            "created_at_raw": F.col("created_at"),
            "actor_raw": F.col("actor"),
            "repo_id": F.col("repository.id"),
            "repo_name": F.concat(F.col("repository.owner"), F.lit("/"), F.col("repository.name")),
            "event_type": F.col("type"),
            # Legacy events carry no id field at all -- 0 of 2,000 sampled.
            "id": F.lit(None).cast("string"),
            # Non-null on all 2,000 sampled, and the field that stops the
            # surrogate key merging two real events -- see `_content_hash`.
            "event_url": F.col("url"),
        }
    return {
        "created_at_raw": F.col("created_at"),
        "actor_raw": F.col("actor.login"),
        "repo_id": F.col("repo.id"),
        "repo_name": F.col("repo.name"),
        "event_type": F.col("type"),
        "id": F.col("id").cast("string"),
        # Modern events have no top-level url and never reach the content
        # hash, since they carry a native id.
        "event_url": F.lit(None).cast("string"),
    }


def read_events(spark: SparkSession, path: str) -> DataFrame:
    """Read one hourly archive file and flatten it onto the raw contract."""
    parsed = spark.read.json(path)
    projection = _raw_projection(is_legacy(parsed))
    return parsed.select(*[col.alias(name) for name, col in projection.items()])


def run_silver(
    spark: SparkSession,
    source_path: str,
    output_path: str,
    *,
    ingested_at: datetime,
    config: SourceConfig,
) -> tuple[DataFrame, DataFrame]:
    """Run one file through to the clean and quarantined Silver tables.

    ``ingested_at`` is a required argument rather than a clock read inside
    this function. It is stamped onto every row and is ``deduplicate``'s
    tiebreak, so reading the clock here would make a replay of the same file
    produce different rows -- and the re-run would still pass a count-based
    idempotency check while doing so.
    """
    raw = read_events(spark, source_path)
    stamped = add_ingestion_metadata(raw, ingested_at=ingested_at, source_file=source_path)
    checked = apply_rules(deduplicate(normalize_events(stamped)), config.quality_rules)
    clean, quarantined = split(checked)

    for df, subdir in ((clean, CLEAN_SUBDIR), (quarantined, QUARANTINE_SUBDIR)):
        df.write.format("delta").mode("overwrite").save(f"{output_path}/{subdir}")
    return clean, quarantined
