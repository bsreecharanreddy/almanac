"""is_bot_column: the Spark-native mirror of explore.measure.classify_bot
and the almanac_is_bot dbt macro. All three must agree -- test_bot_macro.py
already proves the Python regex and the SQL macro do; this proves the
PySpark column expression is the same rule a third way.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from almanac.features.bot import is_bot_column

pytestmark = pytest.mark.spark


def test_agrees_with_classify_bot_on_known_cases(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("dependabot[bot]",), ("renovate",), ("robotframework",), ("Abbott",), ("alice",)],
        "login string",
    )
    result = df.withColumn("is_bot", is_bot_column(F.col("login"))).collect()
    flags = {r["login"]: r["is_bot"] for r in result}

    assert flags["dependabot[bot]"] is True
    assert flags["renovate"] is True
    assert flags["robotframework"] is False  # documented false-positive case (§12 trap 6 origin)
    assert flags["Abbott"] is False
    assert flags["alice"] is False
