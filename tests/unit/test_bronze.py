from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from tests.helpers import one

pytestmark = pytest.mark.spark

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def test_raw_payload_is_preserved_verbatim(spark: SparkSession) -> None:
    raw = spark.createDataFrame([('{"id":"1"}',)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    assert out.select("raw_json").collect() == raw.select("raw_json").collect()


def test_metadata_columns_added(spark: SparkSession) -> None:
    raw = spark.createDataFrame([("{}",)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    assert {"ingested_at", "source_file"} <= set(out.columns)
    assert one(out)["source_file"] == "f.gz"


def test_ingested_at_is_the_value_passed_in_not_wall_clock(spark: SparkSession) -> None:
    """Replay is the whole point of Bronze, and replay is only deterministic
    if ``ingested_at`` is an argument rather than ``current_timestamp()``.
    Without this the substitution is invisible: every other test in this
    module passes with a wall-clock stamp -- checked, they do.

    The assertion is on the **epoch second**, deliberately, and not on the
    datetime that ``.first()`` returns. PySpark converts a Spark timestamp
    to a *naive* Python datetime using the **driver's** local timezone, not
    ``spark.sql.session.timeZone``. Measured on this machine
    (``user.timezone=America/New_York``): a correctly stored 12:00Z reads
    back as ``08:00``. Comparing collected datetimes would therefore pass on
    a UTC CI runner and fail on a developer laptop -- the worst failure
    shape there is. An epoch is an instant and has no such ambiguity.
    """
    raw = spark.createDataFrame([("{}",)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    stored = one(out.selectExpr("unix_timestamp(ingested_at) AS epoch"))["epoch"]
    assert stored == int(INGESTED.timestamp())


def test_naive_ingested_at_is_rejected(spark: SparkSession) -> None:
    """A naive datetime is silently read as **driver-local** time.

    Measured: ``datetime(2026, 9, 2, 12, 0)`` with no tzinfo stores epoch
    1788364800 -- 16:00Z, four hours off -- on a UTC-4 driver, and stores
    the correct instant on a UTC one. Nothing errors and nothing renders
    oddly, because the value is a valid timestamp; it is just the wrong one.

    ``datetime.now()`` is naive, so this is the call a caller writes by
    default. Rejecting it at the boundary is cheaper than finding it in a
    point-in-time feature six phases later.
    """
    raw = spark.createDataFrame([("{}",)], "raw_json string")
    with pytest.raises(ValueError, match="timezone-aware"):
        add_ingestion_metadata(raw, ingested_at=datetime(2026, 9, 2, 12, 0), source_file="f.gz")


def test_ingested_at_is_not_created_at(spark: SparkSession) -> None:
    # The single most important separation in the whole pipeline.
    raw = spark.createDataFrame([("{}",)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    assert "created_at" not in out.columns


def test_rerunning_an_hour_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    # The property the whole backfill depends on: a retried hour must not
    # double its rows.
    path = str(tmp_path / "bronze")
    df = spark.createDataFrame(
        [("a", "2025-08-13", 14), ("b", "2025-08-13", 14)],
        "raw_json string, event_date string, event_hour int",
    )
    for _ in range(2):
        write_bronze(df, path, event_date="2025-08-13", event_hour=14)
    assert spark.read.format("delta").load(path).count() == 2


def test_writing_a_second_hour_does_not_disturb_the_first(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = str(tmp_path / "bronze")
    h14 = spark.createDataFrame(
        [("a", "2025-08-13", 14)],
        "raw_json string, event_date string, event_hour int",
    )
    h15 = spark.createDataFrame(
        [("b", "2025-08-13", 15)],
        "raw_json string, event_date string, event_hour int",
    )
    write_bronze(h14, path, event_date="2025-08-13", event_hour=14)
    write_bronze(h15, path, event_date="2025-08-13", event_hour=15)
    assert spark.read.format("delta").load(path).count() == 2
