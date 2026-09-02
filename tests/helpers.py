"""Shared assertions and builders for the Spark test modules."""

from datetime import datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze


def one(df: DataFrame) -> Row:
    """``first()`` is typed ``Row | None``; every call here expects a row."""
    row = df.first()
    assert row is not None
    return row


def epoch_of(df: DataFrame, column: str) -> int:
    """A timestamp column's stored instant as an epoch second.

    No test asserts on a collected datetime: PySpark returns it naive in the
    driver's timezone, so the comparison passes or fails by machine (and
    always passes on a UTC CI runner). An epoch carries no ambiguity.
    """
    return int(one(df.selectExpr(f"unix_timestamp({column}) AS epoch"))["epoch"])


def build_bronze(
    spark: SparkSession,
    fixture_path: Path,
    bronze_path: Path,
    *,
    event_date: str,
    event_hour: int,
    ingested_at: datetime,
    part: tuple[int, int] | None = None,
) -> None:
    """Land one fixture as a real Bronze partition, through the shipped ``write_bronze``.

    ``part=(index, total)`` lands a deterministic disjoint slice (split on a
    record hash, since Spark does not promise row order) so two hours can
    hold different events like two real hourly files.
    """
    raw = spark.read.text(str(fixture_path)).withColumnRenamed("value", "raw_json")
    if part is not None:
        index, total = part
        raw = raw.filter(F.pmod(F.crc32(F.col("raw_json")), F.lit(total)) == index)
    stamped = add_ingestion_metadata(raw, ingested_at=ingested_at, source_file=str(fixture_path))
    partitioned = stamped.withColumn("event_date", F.lit(event_date)).withColumn(
        "event_hour", F.lit(event_hour)
    )
    write_bronze(partitioned, str(bronze_path), event_date=event_date, event_hour=event_hour)


# The parse_events -> normalize_events contract, declared once (it lived in
# two test modules and drifted -- the two-places-one-contract failure).
RAW_SCHEMA = (
    "created_at_raw string, actor_raw string, repo_id long, "
    "repo_name string, event_type string, id string, "
    "event_url string, ingested_at timestamp"
)

type RawRow = tuple[str, str | None, int | None, str, str, str | None, str | None, datetime]


def _parsed_defaults() -> dict[str, Column]:
    """The rest of what ``parse_events`` produces, so each test names only
    the fields it is about. Built lazily -- ``F.lit`` needs a live SparkContext."""
    return {
        "event_action": F.lit(None).cast("string"),
        "pr_number": F.lit(None).cast("long"),
        "pr_merged": F.lit(None).cast("boolean"),
        "pr_draft": F.lit(None).cast("boolean"),
        "issue_is_pr": F.lit(None).cast("boolean"),
        "push_size": F.lit(None).cast("long"),
        "push_distinct_size": F.lit(None).cast("long"),
        "event_date": F.lit("2025-08-13"),
        "event_hour": F.lit(14),
    }


def raw(spark: SparkSession, *rows: RawRow) -> DataFrame:
    """Crafted rows in the shape `parse_events` hands to `normalize_events`."""
    df = spark.createDataFrame(list(rows), RAW_SCHEMA)
    for name, default in _parsed_defaults().items():
        df = df.withColumn(name, default)
    return df
