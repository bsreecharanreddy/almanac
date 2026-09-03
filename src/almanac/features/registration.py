"""Unity Catalog registration for feature tables -- governance metadata
only, never the correctness mechanism (design doc §4.4a: the as-of join
in join.py owns correctness; this module owns nothing but a CREATE TABLE
and a PRIMARY KEY constraint).
"""

from pyspark.sql import SparkSession

from almanac.gold.sources import table_location


def register_feature_table(
    spark: SparkSession, *, table: str, path: str, schema: str = "features"
) -> None:
    location = table_location(path)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} USING DELTA LOCATION '{location}'")


def primary_key_sql(
    *, schema: str, table: str, entity_cols: list[str], event_time_col: str | None = None
) -> tuple[str, str]:
    """(drop_if_exists_sql, add_constraint_sql). Dropped before being
    re-added on every run, since Task 6's runner fully overwrites each
    table's data on every call -- without the drop, a second run's ADD
    CONSTRAINT would collide with the first run's still-live constraint.
    """
    constraint = f"{table}_pk"
    keys = list(entity_cols)
    if event_time_col is not None:
        keys.append(f"{event_time_col} TIMESERIES")
    drop_sql = f"ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {constraint}"
    add_sql = (
        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint} PRIMARY KEY ({', '.join(keys)})"
    )
    return drop_sql, add_sql
