"""Bronze: land raw records with ingestion metadata. Never transform.

Bronze exists so Silver can be recomputed without re-downloading ~190 GB,
which holds only while the raw payload is preserved byte-for-byte.
"""

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, *, ingested_at: datetime, source_file: str) -> DataFrame:
    """Attach ingestion provenance. Pure: DataFrame in, DataFrame out.

    ``ingested_at`` is wall-clock time, never the event's ``created_at`` (§12
    trap 2), and a parameter rather than ``current_timestamp()`` so a
    replayed hour reproduces its rows. It must be timezone-aware: PySpark
    reads a naive datetime as driver-local and stores the wrong instant
    (measured across three driver timezones; correct on a UTC CI runner, so
    the guard lives here). Sibling of design doc §4.1b, write side.
    """
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise ValueError(
            f"ingested_at must be timezone-aware, got naive {ingested_at!r}; "
            "a naive datetime is read as driver-local time and stores the wrong instant"
        )
    return df.withColumn("ingested_at", F.lit(ingested_at).cast("timestamp")).withColumn(
        "source_file", F.lit(source_file)
    )


def write_bronze(df: DataFrame, path: str, *, event_date: str, event_hour: int) -> None:
    """Idempotently write exactly one hour's partition via replaceWhere.

    Plain overwrite would delete the table; plain append would duplicate on retry.
    """
    predicate = f"event_date = '{event_date}' AND event_hour = {event_hour}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .partitionBy("event_date", "event_hour")
        .save(path)
    )
