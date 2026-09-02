"""Bronze: land raw records with ingestion metadata. Never transform.

Bronze exists so that anything Silver gets wrong can be recomputed without
re-downloading ~190 GB. That guarantee holds only while the raw payload is
preserved byte-for-byte, so this module adds columns and does nothing else.
"""

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, *, ingested_at: datetime, source_file: str) -> DataFrame:
    """Attach ingestion provenance. Pure: DataFrame in, DataFrame out.

    ``ingested_at`` is wall-clock time and is **never** the event's
    ``created_at``. Conflating them is design doc §12 trap 2 and would make
    every point-in-time feature wrong in a way tests would not catch.

    It is a parameter rather than ``current_timestamp()`` on purpose: a
    replayed hour must reproduce the same rows it produced the first time,
    and a function that stamps itself from the clock cannot do that.

    It must also be timezone-aware. ``spark.sql.session.timeZone=UTC``
    governs computation and display; it does **not** govern how PySpark
    converts a Python datetime, in either direction. Measured 2026-09-01
    across three driver timezones: a naive ``12:00`` is read as local wall
    time and stores an instant off by the driver's offset (+4h on
    ``America/New_York``, -5h30 on ``Asia/Kolkata``, **0 on UTC**), while
    the aware equivalent stores the correct instant in all three.

    The naive path raises nothing and renders plausibly. It is also
    correct on a UTC CI runner, so CI cannot catch it and the guard has to
    live here. Sibling of design doc §4.1b, on the write side.
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
    """Idempotently write exactly one hour's partition.

    ``replaceWhere`` scopes the overwrite to this hour, so a retry replaces
    that hour and leaves every other partition untouched. Plain overwrite
    would delete the table; plain append would duplicate on retry.
    """
    predicate = f"event_date = '{event_date}' AND event_hour = {event_hour}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .partitionBy("event_date", "event_hour")
        .save(path)
    )
