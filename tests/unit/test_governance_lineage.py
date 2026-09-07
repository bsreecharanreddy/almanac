"""Lineage node resolution: the path side is the majority of this repo's own graph."""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.governance.lineage import canonicalize, column_edges, tier_edges, tier_of

pytestmark = [pytest.mark.spark]

LAKE = "abfss://silver@almanaclakekoctmh.dfs.core.windows.net/events/clean"
BRONZE = "abfss://bronze@almanaclakekoctmh.dfs.core.windows.net/events"
SILVER_NAME = "almanac_dbx.silver.events"
GOLD_NAME = "almanac_dbx.gold.fact_pull_request"
# A managed table's storage_path points at the metastore's own container, not
# at a medallion tier -- the reason resolution runs path -> name.
GOLD_MANAGED_PATH = "abfss://unity-catalog-storage@dbstoragexyz.dfs.core.windows.net/1234/tables/aa"

_EDGE_SCHEMA = (
    "source_table_full_name string, source_path string, source_column_name string, "
    "target_table_full_name string, target_path string, target_column_name string"
)


def _locations(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(SILVER_NAME, LAKE), (GOLD_NAME, GOLD_MANAGED_PATH)],
        "full_name string, storage_path string",
    )


def _edges(spark: SparkSession, rows: list[tuple[str | None, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, _EDGE_SCHEMA)


def test_a_path_only_edge_resolves_to_the_registered_table_name(spark: SparkSession) -> None:
    """The 86.5% case. A name-only implementation returns nothing here."""
    edges = _edges(spark, [(None, BRONZE, "raw_json", None, LAKE, "event_id")])
    got = canonicalize(edges, _locations(spark)).select("source_node", "target_node").collect()

    assert got[0]["source_node"] == BRONZE, "an unregistered path stays addressable by path"
    assert got[0]["target_node"] == SILVER_NAME, "a registered path resolves to its table name"


def test_the_same_table_by_name_and_by_path_is_one_node(spark: SparkSession) -> None:
    """Read by name and by path, Silver must not appear as two distinct nodes."""
    edges = _edges(
        spark,
        [
            (SILVER_NAME, None, "event_id", GOLD_NAME, None, "event_id"),
            (None, LAKE, "event_id", GOLD_NAME, None, "event_id"),
        ],
    )
    nodes = {r["source_node"] for r in canonicalize(edges, _locations(spark)).collect()}

    assert nodes == {SILVER_NAME}, f"one logical table, one node; got {nodes}"


def test_a_managed_table_keeps_its_schema_tier(spark: SparkSession) -> None:
    """Canonicalizing to paths instead would tier this as 'unity-catalog-storage'."""
    edges = _edges(spark, [(None, LAKE, "event_id", GOLD_NAME, GOLD_MANAGED_PATH, "event_id")])
    row = column_edges(edges, _locations(spark)).collect()[0]

    assert row["source_tier"] == "silver"
    assert row["target_tier"] == "gold"


def test_a_trailing_slash_does_not_split_a_node(spark: SparkSession) -> None:
    edges = _edges(spark, [(None, BRONZE, "raw_json", None, LAKE + "/", "event_id")])
    got = canonicalize(edges, _locations(spark)).collect()[0]

    assert got["target_node"] == SILVER_NAME


def test_a_self_edge_is_not_an_edge(spark: SparkSession) -> None:
    """A table rewritten in place reads and writes itself; that is not lineage."""
    edges = _edges(spark, [(SILVER_NAME, None, "event_id", None, LAKE, "event_id")])

    assert column_edges(edges, _locations(spark)).count() == 0


def test_an_unknown_node_is_external_not_silently_tiered(spark: SparkSession) -> None:
    assert (
        spark.range(1)
        .select(tier_of(F.lit("some_catalog.other.table")).alias("t"))
        .collect()[0]["t"]
        == "external"
    )


def test_tier_edges_count_the_columns_supporting_each_tier_hop(spark: SparkSession) -> None:
    edges = _edges(
        spark,
        [
            (None, LAKE, "event_id", GOLD_NAME, None, "event_id"),
            (None, LAKE, "repo_id", GOLD_NAME, None, "repo_id"),
            (None, BRONZE, "raw_json", None, LAKE, "event_id"),
        ],
    )
    rolled = {
        (r["source_tier"], r["target_tier"]): r["column_edges"]
        for r in tier_edges(column_edges(edges, _locations(spark))).collect()
    }

    assert rolled[("silver", "gold")] == 2
    assert rolled[("bronze", "silver")] == 1
