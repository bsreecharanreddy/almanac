"""Register Silver's Delta output into the metastore dbt's sources read.

`dbt/models/sources.yml` declares `silver.events` and
`silver.events_quarantine`, and `source()` resolves that name through the
**metastore**, not a filesystem path. Silver itself never registers
anything -- `almanac.pipeline.silver.write_silver` writes plain Delta files
with no catalog entry at all (design doc §3.3: Bronze and Silver are pure
PySpark, I/O at the edges, and neither owns a metastore concept). Without
this, `source('silver', 'events')` fails to resolve no matter how correct
the YAML declaration is -- a gap named in STATUS.md as unbuilt and
unexercised until the first Gold model actually selected from a source.
"""

from pathlib import Path

from pyspark.sql import SparkSession


def register_silver_sources(
    spark: SparkSession, *, clean_path: Path, quarantine_path: Path, schema: str = "silver"
) -> None:
    """Point `{schema}.events` / `{schema}.events_quarantine` at Silver's output.

    `CREATE TABLE ... USING DELTA LOCATION` registers an **external** table
    -- a metastore pointer at files Silver already owns, not a copy. dbt
    only ever reads through it; ownership of the data stays exactly where
    CLAUDE.md says it must.

    Both statements are `IF NOT EXISTS`: registration is a one-time pointer,
    not a resync, so a second call after Silver appends more partitions is
    a correctness no-op rather than something that needs to run again.

    Both paths are resolved to absolute before reaching SQL. Measured: a
    relative ``LOCATION`` on a non-``default`` schema does not resolve
    against the caller's working directory the way a bare filesystem path
    would -- Spark silently nests it under that schema's own managed
    directory (``<warehouse>/{schema}.db/<the relative path>``) instead,
    which registers a table pointing at nothing Silver ever wrote and
    fails not at registration time but the first time a model reads it.
    """
    clean_path, quarantine_path = clean_path.resolve(), quarantine_path.resolve()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.events USING DELTA LOCATION '{clean_path}'")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.events_quarantine "
        f"USING DELTA LOCATION '{quarantine_path}'"
    )
