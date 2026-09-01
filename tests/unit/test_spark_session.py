from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.mark.spark
def test_session_timezone_is_utc(spark: SparkSession) -> None:
    # Non-UTC silently shifts every event timestamp, which would corrupt
    # point-in-time correctness in a way no downstream test would catch.
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


@pytest.mark.spark
def test_delta_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string").write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 2


@pytest.mark.spark
def test_delta_replace_where_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    # The write mode Bronze depends on. Proving it here means Phase 1 can
    # rely on it rather than rediscovering it.
    path = str(tmp_path / "t")
    df = spark.createDataFrame([(1, "2025-01-01"), (2, "2025-01-02")], "id int, d string")
    df.write.format("delta").partitionBy("d").save(path)

    again = spark.createDataFrame([(1, "2025-01-01")], "id int, d string")
    for _ in range(2):
        again.write.format("delta").mode("overwrite").option(
            "replaceWhere", "d = '2025-01-01'"
        ).save(path)

    rows = {(r.id, r.d) for r in spark.read.format("delta").load(path).collect()}
    assert rows == {(1, "2025-01-01"), (2, "2025-01-02")}
