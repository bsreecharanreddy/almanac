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


def key_columns(entity_cols: list[str], event_time_col: str | None) -> list[str]:
    """The primary key's columns: the entity keys, then the event-time key
    if the table is temporal. primary_key_sql and not_null_key_sql share
    this so the two cannot disagree about what the key is."""
    keys = list(entity_cols)
    if event_time_col is not None:
        keys.append(event_time_col)
    return keys


def primary_key_sql(
    *, schema: str, table: str, entity_cols: list[str], event_time_col: str | None = None
) -> tuple[str, str]:
    """(drop_if_exists_sql, add_constraint_sql). Dropped before being
    re-added on every run, since Task 6's runner fully overwrites each
    table's data on every call -- without the drop, a second run's ADD
    CONSTRAINT would collide with the first run's still-live constraint.
    """
    constraint = f"{table}_pk"
    keys = key_columns(entity_cols, event_time_col)
    if event_time_col is not None:
        keys[-1] = f"{event_time_col} TIMESERIES"
    drop_sql = f"ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {constraint}"
    add_sql = (
        f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint} PRIMARY KEY ({', '.join(keys)})"
    )
    return drop_sql, add_sql


def change_data_feed_sql(*, schema: str, table: str) -> str:
    """publish_table into a Lakebase online store refuses a source without
    Change Data Feed for its TRIGGERED and CONTINUOUS modes (Databricks
    online-feature-store docs, checked live 2026-09-06). embed/pipeline.py
    sets it at table-create time; feature tables are fully overwritten
    every run, so run_features re-asserts it -- SET TBLPROPERTIES is
    idempotent and round-trips on local Delta."""
    return f"ALTER TABLE {schema}.{table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"


def not_null_key_sql(
    *, schema: str, table: str, entity_cols: list[str], event_time_col: str | None = None
) -> list[str]:
    """One ALTER COLUMN per primary-key column: publish_table refuses a
    nullable key, and UC refuses a PRIMARY KEY constraint on a nullable
    column. Same key set as primary_key_sql, without the TIMESERIES word.

    Not executed against this repo's local metastore, for the same reason
    primary_key_sql is not: open-source Delta 4.4.0 refuses
    ALTER COLUMN ... SET NOT NULL on a populated table ("cannot change
    nullable column to non-nullable"), where Databricks-managed Delta
    accepts it. Verified live during Phase 6's cloud burn (design §4.6).
    """
    return [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN {col} SET NOT NULL"
        for col in key_columns(entity_cols, event_time_col)
    ]
