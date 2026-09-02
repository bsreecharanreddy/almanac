"""Register Silver's Delta output into the metastore dbt's sources read.

``source()`` resolves through the metastore, not a path, and Silver never
registers anything (design doc §3.3: Bronze and Silver are pure PySpark).
Without this, ``source('silver', 'events')`` cannot resolve.
"""

from pathlib import Path

from pyspark.sql import SparkSession


def register_silver_sources(
    spark: SparkSession, *, clean_path: Path, quarantine_path: Path, schema: str = "silver"
) -> None:
    """Point ``{schema}.events`` / ``.events_quarantine`` at Silver's output.

    ``CREATE TABLE ... LOCATION`` registers an external table -- a pointer at
    files Silver owns, not a copy. ``IF NOT EXISTS`` makes a repeat call a
    no-op. Paths are resolved to absolute first: a relative ``LOCATION`` on a
    non-``default`` schema silently nests under that schema's managed dir and
    registers a table pointing at nothing, failing only at first read.
    """
    clean_path, quarantine_path = clean_path.resolve(), quarantine_path.resolve()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.events USING DELTA LOCATION '{clean_path}'")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.events_quarantine "
        f"USING DELTA LOCATION '{quarantine_path}'"
    )
