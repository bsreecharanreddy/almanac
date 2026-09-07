"""Unity Catalog column lineage, resolved to one node identity per table. Pure; I/O at the edges."""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

COLUMN_LINEAGE = "system.access.column_lineage"
TABLE_LINEAGE = "system.access.table_lineage"

# The lake's containers are its medallion tiers; a node outside them is
# either a managed table (tier comes from its UC schema) or foreign.
LAKE_TIERS = ("bronze", "silver", "gold", "features")


def read_lineage(spark: SparkSession, table: str = COLUMN_LINEAGE) -> DataFrame:
    """Raw lineage rows. The one I/O function here."""
    return spark.read.table(table)


def read_table_locations(spark: SparkSession) -> DataFrame:
    """`full_name` -> `storage_path` for every registered table. The other I/O function."""
    return (
        spark.read.table("system.information_schema.tables")
        .where(F.col("storage_path").isNotNull())
        .select(
            F.concat_ws(".", "table_catalog", "table_schema", "table_name").alias("full_name"),
            F.col("storage_path"),
        )
        .distinct()
    )


def _trimmed(path: Column) -> Column:
    """Storage paths compare unequal on a trailing slash alone often enough to matter."""
    return F.regexp_replace(F.coalesce(path, F.lit("")), r"/+$", "")


def _container(node: Column) -> Column:
    """The ADLS container in `abfss://<container>@account/...`, or '' for a non-path."""
    return F.regexp_extract(node, r"^abfss://([^@]+)@", 1)


def _schema_of(node: Column) -> Column:
    """The middle part of `catalog.schema.table`, or '' if this is not a three-part name."""
    return F.regexp_extract(node, r"^[^.:/]+\.([^.:/]+)\.[^.:/]+$", 1)


def tier_of(node: Column) -> Column:
    """Medallion tier of a node: its UC schema, else its lake container, else 'external'."""
    schema, container = _schema_of(node), _container(node)
    return (
        F.when(schema.isin(*LAKE_TIERS), schema)
        .when(container.isin(*LAKE_TIERS), container)
        .otherwise(F.lit("external"))
    )


def canonicalize(edges: DataFrame, locations: DataFrame) -> DataFrame:
    """Resolve both ends of each edge to one node id, whether UC named it or only pathed it.

    Measured 2026-09-07: **4,133 of this repo's 4,778 lineage rows (86.5%)
    carry only `source_path`**, because Bronze, Silver and the feature tier
    are external Delta paths rather than registered tables. An implementation
    keyed on `*_table_full_name` alone returns 13.5% of the graph and reports
    no error, which is why the path side is not an optional extra here.

    Resolution runs **path -> name**, never the reverse: a managed table's
    `storage_path` points into `unity-catalog-storage`, so canonicalizing to
    paths would collapse every managed Gold table into one meaningless tier.
    """
    by_path = locations.select(
        _trimmed(F.col("storage_path")).alias("_lookup_path"),
        F.col("full_name").alias("_lookup_name"),
    ).distinct()

    resolved = edges
    for side in ("source", "target"):
        lookup = by_path.select(
            F.col("_lookup_path").alias(f"_{side}_path_key"),
            F.col("_lookup_name").alias(f"_{side}_resolved"),
        )
        resolved = resolved.join(
            lookup,
            _trimmed(F.col(f"{side}_path")) == F.col(f"_{side}_path_key"),
            "left",
        ).withColumn(
            f"{side}_node",
            # The registered name wins wherever one exists, so the same table
            # read by name and by path is one node rather than two.
            F.coalesce(
                F.col(f"{side}_table_full_name"),
                F.col(f"_{side}_resolved"),
                _trimmed(F.col(f"{side}_path")),
            ),
        )

    return resolved.drop(
        "_source_path_key", "_source_resolved", "_target_path_key", "_target_resolved"
    )


def column_edges(edges: DataFrame, locations: DataFrame) -> DataFrame:
    """One row per distinct source-column -> target-column edge, with both tiers."""
    resolved = canonicalize(edges, locations)
    return (
        resolved.where(F.col("source_node").isNotNull() & F.col("target_node").isNotNull())
        .where(F.col("source_node") != F.col("target_node"))
        .select(
            "source_node",
            "source_column_name",
            "target_node",
            "target_column_name",
            tier_of(F.col("source_node")).alias("source_tier"),
            tier_of(F.col("target_node")).alias("target_tier"),
        )
        .distinct()
    )


def tier_edges(column_level: DataFrame) -> DataFrame:
    """Tier-to-tier rollup, carrying how many column edges support each one."""
    return (
        column_level.groupBy("source_tier", "target_tier")
        .agg(
            F.count("*").alias("column_edges"),
            F.countDistinct("source_node").alias("source_tables"),
            F.countDistinct("target_node").alias("target_tables"),
        )
        .orderBy("source_tier", "target_tier")
    )
