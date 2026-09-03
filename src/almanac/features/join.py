"""The point-in-time join every feature lookup in this package runs
through. Its correctness is the whole reason the feature platform exists
as its own subsystem (design doc §4.4a) -- see
tests/integration/test_features_leakage.py for the invariant this is
built to satisfy.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def as_of_join(
    spine: DataFrame,
    feature_table: DataFrame,
    *,
    on: list[str],
    spine_time_col: str = "as_of_timestamp",
    event_time_col: str = "event_time",
) -> DataFrame:
    """Left join spine to the latest feature_table row per `on`, strictly
    before spine_time_col.

    Every spine row survives. "No qualifying row" -- no match at all, or
    every match lies at or after the as-of time -- nulls every feature
    column rather than dropping the row (the boundary and cold-start
    cases below) or leaking a future value.
    """
    row_id = "_as_of_row_id"
    feature_cols = [c for c in feature_table.columns if c not in on]
    ft = feature_table.select(*on, *[F.col(c).alias(f"__ft_{c}") for c in feature_cols])
    ft_time = f"__ft_{event_time_col}"

    candidates = (
        spine.withColumn(row_id, F.monotonically_increasing_id())
        .join(ft, on=on, how="left")
        .withColumn(
            "_qualifies",
            F.coalesce(F.col(ft_time) < F.col(spine_time_col), F.lit(False)),
        )
    )
    window = Window.partitionBy(row_id).orderBy(
        F.col("_qualifies").desc(), F.col(ft_time).desc_nulls_last()
    )
    ranked = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop(row_id, "_rn")
    )

    result = ranked
    for c in feature_cols:
        prefixed = f"__ft_{c}"
        result = result.withColumn(
            prefixed, F.when(F.col("_qualifies"), F.col(prefixed)).otherwise(F.lit(None))
        )
        if c == event_time_col:
            result = result.drop(prefixed)
        else:
            result = result.withColumnRenamed(prefixed, c)
    return result.drop("_qualifies")
