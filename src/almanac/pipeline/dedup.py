"""Deduplication on event_id. Pure; no I/O.

Not partition-scoped: §12 trap 4 -- the archive repeats events across hour
boundaries, so per-partition dedup would keep both copies.
"""

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per event_id, earliest by event time (ingested_at breaks ties).

    Rows with no ``event_id`` pass through: two id-less rows cannot be shown
    to be the same event, so collapsing them is deletion. A window
    partitioned by ``event_id`` groups all nulls together and would keep one
    -- the explicit ``isNull()`` clause keeps them all.
    """
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("created_at").asc(), F.col("ingested_at").asc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(ordering))
        .filter((F.col("_rn") == 1) | F.col("event_id").isNull())
        .drop("_rn")
    )


def duplicate_stats(df: DataFrame) -> Row:
    """Run-report counts: total, distinct, and the difference."""
    row = df.select(
        F.count("*").alias("total"),
        F.countDistinct("event_id").alias("distinct"),
        (F.count("*") - F.countDistinct("event_id")).alias("duplicates"),
    ).first()
    assert row is not None
    return row
