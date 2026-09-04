"""The model's training frame: Phase 3's point-in-time feature rows,
plus Gold's label.

This module reads Gold, unlike almanac/features/: a label is supervision
about the future outcome, not a point-in-time feature, so §3.1's
never-read-Gold boundary does not extend to it (design doc §5.2). Every
read is independently version-pinnable, so a specific training frame
stays byte-for-byte reproducible after later Gold or Silver activity --
Task 7's leakage-suite invariant, extended to the label side.

Silver `/clean` and the three feature tables are path Delta on the lake
(hand-written by the backfill and `run_features`). Gold's fact is a dbt
model -- a metastore table -- and is read by name: on a Unity Catalog
workspace `CREATE TABLE gold.x` lands in the default catalog's managed
storage, not under any `--warehouse` path (2026-09-04, Task 9).
"""

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.features.assemble import assemble_training_set
from almanac.features.spine import build_pr_opened_spine


def join_label(training_frame: DataFrame, fact_pull_request: DataFrame) -> DataFrame:
    """Inner-join Gold's label on, keeping only rows with a defined outcome."""
    label = fact_pull_request.select(
        "repo_id", "pr_number", "time_to_first_response_seconds", "label_exclusion"
    )
    joined = training_frame.join(label, on=["repo_id", "pr_number"], how="inner")
    return joined.where(F.col("label_exclusion").isNull()).drop("label_exclusion")


def _read_delta(spark: SparkSession, path: str, version: int | None) -> DataFrame:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def _read_table(spark: SparkSession, name: str, version: int | None) -> DataFrame:
    reader = spark.read
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.table(name)


def build_training_frame(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    silver_version: int | None = None,
    features_version: int | None = None,
    gold_version: int | None = None,
) -> pd.DataFrame:
    """Read Silver, the three v1 feature tables, and Gold's fact -- each at
    its own optionally-pinned Delta version -- and collect one pandas frame.
    """
    events = _read_delta(spark, f"{silver_path}/clean", silver_version)
    spine = build_pr_opened_spine(events)

    author_activity = _read_delta(spark, f"{features_path}/author_activity", features_version)
    repo_activity = _read_delta(spark, f"{features_path}/repo_activity", features_version)
    pr_static = _read_delta(spark, f"{features_path}/pr_static", features_version)
    training_frame = assemble_training_set(
        spine, author_activity=author_activity, repo_activity=repo_activity, pr_static=pr_static
    )

    fact_pull_request = _read_table(spark, gold_table, gold_version)
    return join_label(training_frame, fact_pull_request).toPandas()
