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
    cases in tests/unit/test_features_join.py) or leaking a future value.

    One ordered timeline per `on` key, not a `spine join feature_table`:
    a handful of bot logins open a large enough share of every PR that
    that join on `author_login` is an O(n^2) blow-up on those keys --
    11.7 PiB of intermediate on the real quarter (2026-09-04,
    docs/findings/2026-09-04-author-activity-self-join.md). Unioning the
    two sides and carrying each feature row's values forward to the query
    rows after it is one shuffle-and-sort per key instead.
    """
    feature_cols = [c for c in feature_table.columns if c not in on and c != event_time_col]
    boxed = [f"_asof_{c}" for c in feature_cols]
    ts, tag, carried = "_asof_ts", "_asof_is_query", "_asof_carried"

    # A query event per spine row (its own columns kept, feature columns
    # null); a value event per feature row (the reverse). `tag` ascending
    # sorts a query ahead of a value at an equal instant -- that is the
    # strict `<` the governing invariant depends on (CLAUDE.md).
    query = spine.withColumn(ts, F.col(spine_time_col)).withColumn(tag, F.lit(0))
    for c, b in zip(feature_cols, boxed, strict=True):
        query = query.withColumn(b, F.lit(None).cast(feature_table.schema[c].dataType))

    value = feature_table.select(
        *on,
        F.col(event_time_col).alias(ts),
        F.lit(1).alias(tag),
        *[F.col(c).alias(b) for c, b in zip(feature_cols, boxed, strict=True)],
    )
    for c in spine.columns:
        if c not in on:
            value = value.withColumn(c, F.lit(None).cast(spine.schema[c].dataType))

    to_date = (
        Window.partitionBy(*on)
        .orderBy(ts, tag)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    # The struct is null on a query row, so `last(..., ignorenulls=True)`
    # carries the most recent *value* row -- and takes its columns whole,
    # so a legitimately null feature value is kept, not skipped for an
    # older non-null one.
    latest_value = F.when(
        F.col(tag) == 1,
        F.struct(*[F.col(b).alias(c) for c, b in zip(feature_cols, boxed, strict=True)]),
    )

    return (
        query.unionByName(value.select(*query.columns))
        .withColumn(carried, F.last(latest_value, ignorenulls=True).over(to_date))
        .where(F.col(tag) == 0)
        .select(*spine.columns, *[F.col(carried)[c].alias(c) for c in feature_cols])
    )
