"""Register Silver's Delta output into the metastore dbt's sources read.

``source()`` resolves through the metastore, not a path, and Silver never
registers anything (design doc §3.3: Bronze and Silver are pure PySpark).
Without this, ``source('silver', 'events')`` cannot resolve.
"""

from pathlib import Path

from pyspark.sql import SparkSession


def table_location(path: str) -> str:
    """A LOCATION Spark can resolve: URIs untouched, local paths made absolute.

    Not ``Path`` anywhere on this route. ``Path('abfss://c@acct/x')`` collapses
    the '//' to 'abfss:/c@acct/x', which is then a *relative* path, so
    ``.resolve()`` anchors it under the driver's cwd -- Spark reported
    "Missing cloud file system scheme" and the A/B arm died at the Gold step.
    A relative *local* path still has to be resolved, for the reason below.
    """
    return path if "://" in path else str(Path(path).resolve())


def register_silver_sources(
    spark: SparkSession, *, clean_path: str, quarantine_path: str, schema: str = "silver"
) -> None:
    """Point ``{schema}.events`` / ``.events_quarantine`` at Silver's output.

    ``CREATE TABLE ... LOCATION`` registers an external table -- a pointer at
    files Silver owns, not a copy. ``IF NOT EXISTS`` makes a repeat call a
    no-op. Local paths are resolved to absolute first: a relative ``LOCATION``
    on a non-``default`` schema silently nests under that schema's managed dir
    and registers a table pointing at nothing, failing only at first read.
    """
    clean, quarantine = table_location(clean_path), table_location(quarantine_path)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.events USING DELTA LOCATION '{clean}'")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.events_quarantine USING DELTA LOCATION '{quarantine}'"
    )
