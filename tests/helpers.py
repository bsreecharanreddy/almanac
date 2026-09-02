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
    """A timestamp column's stored instant, as an epoch second.

    No test asserts on a *collected* datetime. PySpark hands one back naive,
    converted to the driver's timezone, so a datetime comparison passes or
    fails depending on the machine running it -- and passes on a UTC CI
    runner either way (Task 2's finding, STATUS.md 2026-09-01). An epoch is
    an instant and carries no timezone ambiguity at all.
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
    """Land one fixture as a real Bronze partition.

    Deliberately goes through the shipped ``write_bronze`` rather than
    writing Delta directly: Silver's input contract is whatever Bronze
    actually produces, and a test that hand-rolls its own Bronze can pass
    while the two have drifted apart.

    Reads with ``spark.read.text``, not ``read.json`` -- Bronze stores the
    record as an unparsed string. Task 7 measured why: per-file schema
    inference disagrees between hours of the same day, which broke the
    calibration's first Delta write, and parsing in Bronze is a transform
    Bronze is not allowed to do.

    Pass ``part=(index, total)`` to land a deterministic, disjoint slice of
    the fixture, so two hours can hold different events the way two real
    hourly files do. Split on a hash of the record rather than on row order,
    which Spark does not promise to preserve.
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


# The contract `parse_events` produces and `normalize_events` consumes,
# declared once. It lived in two test modules that each spelled it out, and
# adding a column to the parser broke the one that was not being looked at
# -- the same two-places-one-contract failure that cost Phase 1 a day
# between `normalize_events` and `deduplicate`.
RAW_SCHEMA = (
    "created_at_raw string, actor_raw string, repo_id long, "
    "repo_name string, event_type string, id string, "
    "event_url string, ingested_at timestamp"
)

type RawRow = tuple[str, str | None, int | None, str, str, str | None, str | None, datetime]


def _parsed_defaults() -> dict[str, Column]:
    """The rest of what `parse_events` produces.

    Defaulted so each test spells out only the fields it is about: a
    PushEvent fixture asserting on timestamp handling has no business naming
    a PR number, and threading every field through every case would bury the
    interesting one. Built lazily because `F.lit` needs a live
    SparkContext, which pytest's collection phase does not have.
    """
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
