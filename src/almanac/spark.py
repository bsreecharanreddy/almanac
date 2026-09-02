"""SparkSession construction. The only place Spark is configured."""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "almanac"


def _builder(app_name: str) -> SparkSession.Builder:
    """The configuration every session in this project shares.

    Deliberately ``local[*]`` rather than a Compose master/worker cluster:
    at this data volume a real cluster is slower, and it costs days on
    executor OOMs that teach nothing (design doc §8).
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
    )


def local_session(app_name: str = DEFAULT_APP_NAME) -> SparkSession:
    """A local-mode SparkSession with Delta enabled.

    ``configure_spark_with_delta_pip`` is required, not optional. The
    ``delta-spark`` pip package ships Python bindings only; without this
    the JVM has no Delta data source on its classpath and every
    ``.format("delta")`` call dies with ``ClassNotFoundException``.
    It resolves the matching JARs from Maven, so the first run of a new
    environment needs network access.
    """
    return configure_spark_with_delta_pip(_builder(app_name)).getOrCreate()


def dbt_session(
    *, warehouse: Path, metastore: Path, app_name: str = f"{DEFAULT_APP_NAME}-dbt"
) -> SparkSession:
    """The session dbt runs against. Hive support here is a correctness
    requirement wearing a configuration costume.

    dbt-spark's session method reaches for the live session with
    ``SparkSession.builder.enableHiveSupport().getOrCreate()``. Without a
    **persistent** metastore behind that call, Spark falls back to its
    in-memory catalog: the relation a previous process created is invisible,
    ``is_incremental()`` is false, and every model silently rebuilds itself
    from scratch with ``CREATE OR REPLACE TABLE AS SELECT``.

    dbt reports ``success=True`` either way. Measured, in
    ``tests/integration/test_dbt_target.py``: two consecutive runs against an
    ephemeral catalog produce two ``CREATE OR REPLACE`` commits and no
    ``MERGE``, while the row counts stay exactly as expected. An SCD2
    dimension that has quietly become a snapshot of "now" looks perfectly
    healthy from the outside, which is why the test asserts on the Delta
    commit log rather than on rows.

    ``warehouse`` is where managed tables land; ``metastore`` is the embedded
    Derby database that remembers they exist. Both are explicit parameters
    rather than defaults so that a caller cannot accidentally scatter a
    ``metastore_db`` directory into whatever the working directory happened
    to be.
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
