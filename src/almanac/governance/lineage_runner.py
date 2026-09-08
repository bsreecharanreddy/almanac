"""Generate the committed column-lineage artifact from Unity Catalog's own record."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.governance.lineage import column_edges, tier_edges
from almanac.spark import active_or_local_session

# The production medallion. Phase 2's Photon A/B harness wrote parallel
# `ab_photon_*` / `ab_standard_*` schemas which UC records as real tiers; they
# are a one-off experiment, not part of the platform, so the artifact scopes
# them out **and says so** rather than dropping them silently.
PRODUCTION_TIERS = (
    "external",
    "landing",
    "bronze",
    "silver",
    "gold",
    "features",
    "embeddings",
    "models",
    "serving_logs",
)

_EDGE_COLUMNS = (
    "source_table_full_name",
    "source_path",
    "source_column_name",
    "target_table_full_name",
    "target_path",
    "target_column_name",
)

_EDGE_SCHEMA = ", ".join(f"{c} string" for c in _EDGE_COLUMNS)
_LOCATION_SCHEMA = "full_name string, storage_path string"

_LINEAGE_SQL = f"""
SELECT {", ".join(_EDGE_COLUMNS)}
FROM system.access.column_lineage
WHERE coalesce(source_path, '') LIKE '%almanaclake%'
   OR coalesce(target_path, '') LIKE '%almanaclake%'
   OR coalesce(source_table_full_name, '') LIKE 'almanac_dbx%'
   OR coalesce(target_table_full_name, '') LIKE 'almanac_dbx%'
"""

_LOCATIONS_SQL = """
SELECT concat_ws('.', table_catalog, table_schema, table_name) AS full_name, storage_path
FROM system.information_schema.tables
WHERE storage_path IS NOT NULL
"""

# Stated on the artifact itself, where someone reading the graph will see them,
# rather than in a doc they would have to already know to look for.
BLIND_SPOTS = [
    "Local Spark runs are invisible. Unity Catalog records lineage for work "
    "executed on Databricks; this repo's entire test suite runs on local Spark "
    "and contributes nothing here. An edge's absence is not evidence it does "
    "not exist in code.",
    "The lineage system tables keep a rolling 1-year window. Catalog Explorer "
    "and the lineage API retain indefinitely for lineage captured after "
    "2024-09-01, so this artifact is the cheap view, not the archival one.",
    "Lineage is emitted only where it can be inferred: a column written from a "
    "literal, rather than read from somewhere, produces no edge at all.",
    "Phase 2's Photon A/B schemas (ab_photon_*, ab_standard_*) are excluded on "
    "purpose. They are a one-off experiment harness, and including them would "
    "present four extra tiers as though they were part of the platform.",
    "`external` is a residual bucket, not a place. It holds anything not in "
    "this catalog, not in a lake container, and not under a volume -- staging "
    "paths and foreign tables alike. An `x -> external` edge says only that "
    "something was written somewhere this taxonomy does not name; do not read "
    "it as an export.",
]


def _rows(warehouse_id: str, statement: str) -> list[tuple[Any, ...]]:
    """Run one statement on a SQL warehouse. The only network I/O here."""
    result = WorkspaceClient().statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    if result.status is not None and result.status.error is not None:
        raise RuntimeError(f"lineage query failed: {result.status.error.message}")
    data = result.result.data_array if result.result is not None else None
    return [tuple(row) for row in (data or [])]


def _mermaid(tier_rows: list[dict[str, Any]]) -> str:
    """A tier-level graph. Column-level detail lives in the JSON, not in a picture."""
    lines = ["```mermaid", "graph LR"]
    for row in tier_rows:
        lines.append(f"  {row['source_tier']} -->|{row['column_edges']} cols| {row['target_tier']}")
    lines.append("```")
    return "\n".join(lines)


def render(tier_rows: list[dict[str, Any]], column_edge_count: int, generated: str) -> str:
    """The human-facing artifact: the graph, its counts, and what it cannot see."""
    header = [
        "# Column lineage — generated, not asserted",
        "",
        f"**Generated:** {generated} by `make lineage`  ",
        "**Source:** `system.access.column_lineage` (Unity Catalog's own record)  ",
        f"**Scope:** the production medallion — {', '.join(PRODUCTION_TIERS)}",
        "",
        "Regenerate with `make lineage`. Nothing here is hand-written; if a tier "
        "is missing from the graph below, the pipeline stopped producing it, or "
        "the extraction broke. `tests/unit/test_governance_lineage_artifact.py` "
        "fails on either.",
        "",
        "## The graph",
        "",
        _mermaid(tier_rows),
        "",
        f"**{column_edge_count} distinct column-to-column edges** across "
        f"{len(tier_rows)} tier hops.",
        "",
        "| From | To | Column edges | Source tables | Target tables |",
        "|---|---|---|---|---|",
    ]
    for row in tier_rows:
        header.append(
            f"| `{row['source_tier']}` | `{row['target_tier']}` | {row['column_edges']} "
            f"| {row['source_tables']} | {row['target_tables']} |"
        )

    header += ["", "## What this cannot see", ""]
    header += [f"{i}. {spot}" for i, spot in enumerate(BLIND_SPOTS, start=1)]
    header += [
        "",
        "Stated here rather than in a separate document because a lineage graph "
        "is read as complete unless it says otherwise, and this one is not.",
        "",
    ]
    return "\n".join(header)


def _production_only(edges: DataFrame) -> DataFrame:
    return edges.where(
        F.col("source_tier").isin(*PRODUCTION_TIERS) & F.col("target_tier").isin(*PRODUCTION_TIERS)
    )


def build(spark: SparkSession, warehouse_id: str) -> tuple[list[dict[str, Any]], int]:
    """Fetch, resolve and roll up. Returns the tier rollup and the column-edge count."""
    edges = spark.createDataFrame(_rows(warehouse_id, _LINEAGE_SQL), _EDGE_SCHEMA)
    locations = spark.createDataFrame(_rows(warehouse_id, _LOCATIONS_SQL), _LOCATION_SCHEMA)

    scoped = _production_only(column_edges(edges, locations)).cache()
    rolled = [row.asDict() for row in tier_edges(scoped).collect()]
    return rolled, scoped.count()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--out", type=Path, default=Path("docs/lineage"))
    args = parser.parse_args()

    spark = active_or_local_session("almanac-lineage")
    tier_rows, column_edge_count = build(spark, args.warehouse_id)
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "column-lineage.json").write_text(
        json.dumps(
            {
                "generated": generated,
                "source": "system.access.column_lineage",
                "scope": list(PRODUCTION_TIERS),
                "column_edges": column_edge_count,
                "tier_edges": tier_rows,
                "blind_spots": BLIND_SPOTS,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    (args.out / "lineage.md").write_text(render(tier_rows, column_edge_count, generated))
    print(f"wrote {args.out}/column-lineage.json and lineage.md: {column_edge_count} column edges")


if __name__ == "__main__":
    main()
