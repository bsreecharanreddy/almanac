"""Deduplication on event_id. Pure; no I/O.

Deliberately **not** partition-scoped. Design doc §12 trap 4: the archive
can repeat an event across an hour boundary, so deduplicating within each
hourly partition would keep both copies. Phase 0 measured a duplicate ratio
but its sample used non-adjacent hours, so this case was never exercised
until Task 5's tests.
"""

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep exactly one row per ``event_id``, the earliest by event time.

    Earliest-by-``created_at`` rather than by ``ingested_at``: event time is
    the source of truth, and keeping a later copy would inflate every
    response-latency measurement derived from it. ``ingested_at`` breaks
    ties so the result is deterministic when both timestamps match.

    Rows with **no** ``event_id`` pass through untouched. Two id-less rows
    cannot be shown to be the same event, so collapsing them is deletion,
    not deduplication -- and a window partitioned by ``event_id`` groups
    every null together, so the default behaviour is to keep exactly one of
    them. Measured 2026-09-01: four distinct id-less events reduced to one.
    Silver runs dedup *before* the quality split, so the ``event_id_present``
    reject rule cannot protect this; it would instead report a single bad
    record where thousands were destroyed, under-stating the damage by
    exactly its own size. ``normalize_events`` cannot currently emit a null
    id, which is what makes this worth pinning rather than assuming.
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
    """Counts for the run report: total, distinct, and the difference."""
    row = df.select(
        F.count("*").alias("total"),
        F.countDistinct("event_id").alias("distinct"),
        (F.count("*") - F.countDistinct("event_id")).alias("duplicates"),
    ).first()
    # An aggregate with no `groupBy` yields exactly one row, always.
    assert row is not None
    return row
