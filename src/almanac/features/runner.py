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
from almanac.contracts import Contract, apply_constraints, enforce
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)
from almanac.features.registration import (
    change_data_feed_sql,
    key_columns,
    not_null_key_sql,
    primary_key_sql,
    register_feature_table,
)
from almanac.spark import active_or_local_session


@dataclass(frozen=True)
class FeatureTableSpec:
    """A feature table's whole definition: how to build it, and what it promises."""

    name: str
    compute: Callable[[DataFrame], DataFrame]
    entity_cols: list[str]
    event_time_col: str | None
    columns: dict[str, str]
    checks: dict[str, str]

    @property
    def contract(self) -> Contract:
        """The key comes from `entity_cols`/`event_time_col`, so it cannot disagree
        with the UC primary key `registration` builds from the same two fields."""
        return Contract(
            surface=self.name,
            columns=self.columns,
            keys=tuple(key_columns(self.entity_cols, self.event_time_col)),
            checks=self.checks,
        )


FEATURE_TABLES: list[FeatureTableSpec] = [
    FeatureTableSpec(
        "author_activity",
        compute_author_activity,
        ["author_login"],
        "event_time",
        columns={
            "author_login": "string",
            "event_time": "timestamp",
            "prior_pr_count": "bigint",
            "prior_merge_rate": "double",
        },
        checks={
            "prior_pr_count_nonneg": "prior_pr_count >= 0",
            # A null rate means "no prior PRs to judge by", never a lost value --
            # the unknown-vs-zero distinction compute_author_activity is built on.
            "rate_known_iff_prior_prs": "(prior_pr_count = 0) = (prior_merge_rate IS NULL)",
            "rate_is_a_rate": "prior_merge_rate IS NULL OR prior_merge_rate BETWEEN 0 AND 1",
        },
    ),
    FeatureTableSpec(
        "repo_activity",
        compute_repo_activity,
        ["repo_id"],
        "event_time",
        columns={
            "repo_id": "bigint",
            "event_time": "timestamp",
            "events_total_to_date": "bigint",
            "bot_events_to_date": "bigint",
            "prs_opened_to_date": "bigint",
            "bot_share_to_date": "double",
        },
        checks={
            # Inclusive of the event on its own row, so never zero.
            "events_total_positive": "events_total_to_date > 0",
            "bot_events_within_total": "bot_events_to_date BETWEEN 0 AND events_total_to_date",
            "prs_opened_within_total": "prs_opened_to_date BETWEEN 0 AND events_total_to_date",
            "bot_share_is_a_share": "bot_share_to_date BETWEEN 0 AND 1",
        },
    ),
    FeatureTableSpec(
        "pr_static",
        compute_pr_static,
        ["repo_id", "pr_number"],
        None,
        columns={
            "repo_id": "bigint",
            "pr_number": "bigint",
            "is_draft": "boolean",
            "is_bot_author": "boolean",
            "opened_day_of_week": "int",
            "opened_hour": "int",
        },
        checks={
            "day_of_week_in_range": "opened_day_of_week BETWEEN 1 AND 7",
            "hour_in_range": "opened_hour BETWEEN 0 AND 23",
        },
    ),
]


def write_and_register(
    spark: SparkSession,
    spec: FeatureTableSpec,
    events: DataFrame,
    *,
    features_path: str,
    register: bool,
    schema: str,
) -> str:
    """Overwrite one feature table and, when asked, make it publishable.

    Shared with the streaming path (``almanac.stream.runner``), which builds
    different features from a different source but needs this exact sequence
    -- one statement of what "a registered feature table" means, rather than
    two that drift.

    The contract is enforced on both sides of the write and outside the
    ``register`` branch, unlike the UC constraints below: the shape is checked
    before anything lands, and the CHECK constraints go on by path, which the
    local metastore supports and every run therefore exercises.
    """
    path = f"{features_path}/{spec.name}"
    frame = spec.compute(events)
    enforce(frame, spec.contract)
    frame.write.format("delta").mode("overwrite").save(path)
    apply_constraints(spark, path, spec.contract)
    if register:
        register_feature_table(spark, table=spec.name, path=path, schema=schema)
        # Online-publish prerequisites, in dependency order: NOT NULL keys
        # before the PRIMARY KEY that needs them, CDF anytime (design §4.6).
        # Verified against real UC in Phase 6's cloud burn.
        spark.sql(change_data_feed_sql(schema=schema, table=spec.name))
        for stmt in not_null_key_sql(
            schema=schema,
            table=spec.name,
            entity_cols=spec.entity_cols,
            event_time_col=spec.event_time_col,
        ):
            spark.sql(stmt)
        drop_sql, add_sql = primary_key_sql(
            schema=schema,
            table=spec.name,
            entity_cols=spec.entity_cols,
            event_time_col=spec.event_time_col,
        )
        spark.sql(drop_sql)
        spark.sql(add_sql)
    return path


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
        write_and_register(
            spark, spec, events, features_path=features_path, register=register, schema=schema
        )


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
        active_or_local_session("almanac-features"),
        silver_path=args.silver_path,
        features_path=args.features_path,
        register=args.register,
        schema=args.schema,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
