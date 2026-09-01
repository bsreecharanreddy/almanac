"""SparkSession construction. The only place Spark is configured."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_session(app_name: str = "almanac") -> SparkSession:
    """A local-mode SparkSession with Delta enabled.

    Deliberately ``local[*]`` rather than a Compose master/worker cluster:
    at this data volume a real cluster is slower, and it costs days on
    executor OOMs that teach nothing (design doc §8).

    ``configure_spark_with_delta_pip`` is required, not optional. The
    ``delta-spark`` pip package ships Python bindings only; without this
    the JVM has no Delta data source on its classpath and every
    ``.format("delta")`` call dies with ``ClassNotFoundException``.
    It resolves the matching JARs from Maven, so the first run of a new
    environment needs network access.
    """
    builder = (
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
    return configure_spark_with_delta_pip(builder).getOrCreate()
