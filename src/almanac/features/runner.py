"""Silver -> feature tables. The I/O boundary for the feature platform,
in the same shape as gold/runner.py and gold/sources.py's own split
between pure transforms and the one module that touches Spark I/O and
the metastore.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from almanac.cli import run_cli
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)
from almanac.features.registration import primary_key_sql, register_feature_table
from almanac.spark import local_session


@dataclass(frozen=True)
class FeatureTableSpec:
    name: str
    compute: Callable[[DataFrame], DataFrame]
    entity_cols: list[str]
    event_time_col: str | None


FEATURE_TABLES: list[FeatureTableSpec] = [
    FeatureTableSpec("author_activity", compute_author_activity, ["author_login"], "event_time"),
    FeatureTableSpec("repo_activity", compute_repo_activity, ["repo_id"], "event_time"),
    FeatureTableSpec("pr_static", compute_pr_static, ["repo_id", "pr_number"], None),
]


def _active_or_local_session() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-features")


def run_features(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    register: bool,
    schema: str = "features",
) -> None:
    """Recompute every feature table from the whole of Silver, overwriting each.

    Full recompute, not incremental: v1's cumulative aggregates (running
    counts, author_activity's self-join merge rate) would need Gold's
    recompute-touched incremental-merge machinery to update correctly in
    place, and nothing here builds that yet -- stated in the plan's
    Deferred section, not silently accepted. Correct and fine at this
    project's data volume; the known cost is a full Silver scan per run.
    """
    events = spark.read.format("delta").load(f"{silver_path}/clean")
    for spec in FEATURE_TABLES:
        path = f"{features_path}/{spec.name}"
        spec.compute(events).write.format("delta").mode("overwrite").save(path)
        if register:
            register_feature_table(spark, table=spec.name, path=path, schema=schema)
            drop_sql, add_sql = primary_key_sql(
                schema=schema,
                table=spec.name,
                entity_cols=spec.entity_cols,
                event_time_col=spec.event_time_col,
            )
            spark.sql(drop_sql)
            spark.sql(add_sql)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Recompute the feature platform's tables from Silver."
    )
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--schema", default="features")
    parser.add_argument(
        "--register",
        action="store_true",
        help=(
            "Register each table's UC TIMESERIES constraint after writing. "
            "Needs a live Unity Catalog metastore -- the local Derby metastore "
            "does not support it, so this is off unless explicitly requested."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_features(
        _active_or_local_session(),
        silver_path=args.silver_path,
        features_path=args.features_path,
        register=args.register,
        schema=args.schema,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
