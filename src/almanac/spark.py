"""SparkSession construction. The only place Spark is configured."""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "almanac"


def _builder(app_name: str) -> SparkSession.Builder:
    """The config every session shares.

    ``local[*]``, not a Compose cluster: at this data volume a real cluster
    is slower and costs days on executor OOMs (design doc §8).
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Non-negotiable: event-time correctness depends on it.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        # This session only ever runs local[*] (design doc §8) against test
        # or fixture data of at most a few thousand rows. Spark's default
        # 200 shuffle partitions is then pure scheduler overhead -- ~200
        # near-empty tasks per join/window/aggregation, and the suite runs
        # thousands. 8 fits a typical local core count without
        # oversubscribing a 2-core CI runner. Databricks sets its own from
        # the cluster (a job task takes `active_or_local_session`'s existing
        # session, never this builder), and `embed.pipeline`'s one
        # partition-count-sensitive path passes an explicit `repartition()`,
        # so neither is affected. The UI binds a port on every session start
        # and nothing here reads it.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
    )


def local_session(app_name: str = DEFAULT_APP_NAME) -> SparkSession:
    """A local-mode SparkSession with Delta enabled.

    ``configure_spark_with_delta_pip`` is required: the pip package ships
    Python bindings only, and it resolves the matching JARs from Maven, so a
    new environment's first run needs network access.
    """
    return configure_spark_with_delta_pip(_builder(app_name)).getOrCreate()


def active_or_local_session(app_name: str) -> SparkSession:
    """A job task's own session if one is already active (Databricks
    provides one), else a fresh local one -- every runner's own CLI needs
    this, so it lives here once rather than once per runner."""
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session(app_name)


def dbt_session(
    *, warehouse: str, metastore: Path, app_name: str = f"{DEFAULT_APP_NAME}-dbt"
) -> SparkSession:
    """The session dbt runs against. Hive support here is a correctness requirement.

    Without a persistent metastore, Spark falls back to its in-memory
    catalog: a prior process's relation is invisible, ``is_incremental()`` is
    false, and every model silently rebuilds with ``CREATE OR REPLACE`` while
    dbt reports success and row counts look right. See
    ``tests/integration/test_dbt_target.py``.

    ``warehouse`` and ``metastore`` are explicit params so a caller cannot
    scatter a ``metastore_db`` into the working directory.
    """
    builder = (
        _builder(app_name)
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
