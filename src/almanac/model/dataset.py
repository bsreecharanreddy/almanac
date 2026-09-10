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

from collections.abc import Mapping

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


def compute_breach_labels(fact_pull_request: DataFrame, *, threshold_seconds: int) -> DataFrame:
    """Every PR with a defined breach/no-breach outcome (design doc §5.3):
    (repo_id, pr_number, closed_at, breach). The one definition of
    "resolved" this project trains on -- reused as-is by
    `features/similarity.py`'s `resolutions` (Phase 5, §8.3a) rather than
    re-derived, since two copies of this CASE expression is exactly the
    kind of duplication that goes stale silently.

    Wider than `join_label`'s population: a `closed_no_response` row has
    no duration to regress on, but still answers "did it breach" once it
    sat open at least `threshold_seconds` before closing unanswered.
    `author_unobserved` / `right_censored` / `draft` stay excluded --
    no anchor, outcome not yet known, and not in the response-SLA
    population, respectively, unchanged from `join_label`.
    """
    trainable = fact_pull_request.where(
        F.col("label_exclusion").isNull() | (F.col("label_exclusion") == "closed_no_response")
    )
    breach = F.when(
        F.col("label_exclusion").isNull(),
        F.col("time_to_first_response_seconds") > threshold_seconds,
    ).when(
        F.col("label_exclusion") == "closed_no_response",
        (F.unix_timestamp("closed_at") - F.unix_timestamp("opened_at")) >= threshold_seconds,
    )
    return trainable.select("repo_id", "pr_number", "closed_at", breach.alias("breach"))


def join_breach_label(
    training_frame: DataFrame, fact_pull_request: DataFrame, *, threshold_seconds: int
) -> DataFrame:
    """Inner-join `compute_breach_labels`'s derived breach/no-breach label
    onto a training frame."""
    label = compute_breach_labels(fact_pull_request, threshold_seconds=threshold_seconds).drop(
        "closed_at"
    )
    return training_frame.join(label, on=["repo_id", "pr_number"], how="inner")


def join_similarity_features(training_frame: DataFrame, similarity_frame: DataFrame) -> DataFrame:
    """Left-join `compute_pr_similarity`'s output (Phase 5, §8.3a) --
    unlike the label joins above, a PR the similarity index never scored
    (no embedding, or run before Task 7's real index existed) keeps its
    row with the similar_* columns null, not dropped."""
    return training_frame.join(similarity_frame, on=["repo_id", "pr_number"], how="left")


def read_delta(spark: SparkSession, path: str, version: int | None) -> DataFrame:
    """A path Delta table, optionally pinned. Public: `agent.tools` reads
    feature tables through this same primitive rather than a second copy.
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def _read_table(spark: SparkSession, name: str, version: int | None) -> DataFrame:
    reader = spark.read
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.table(name)


# The tables `_build_feature_frame` reads, and therefore the exact keys a
# pin must carry. Kept here rather than imported from `features.runner`,
# which already imports this module through `similarity_runner`;
# `test_the_pinnable_tables_are_the_tables_the_runner_writes` is what keeps
# the two in step.
FEATURE_TABLE_NAMES: tuple[str, ...] = ("author_activity", "repo_activity", "pr_static")


def feature_pins(versions: Mapping[str, int] | None) -> dict[str, int | None]:
    """One version per feature table, refusing a pin that does not name them all.

    A partial pin must not fall back to "latest" for the rest. The three feature
    tables take a different number of Delta commits per run -- 5, 6 and 4
    constraint commits respectively (Phase 7 Task 4) -- so their versions
    diverge, and pinning two while reading the third live mixes points in time
    and raises nothing at all. That silence is what this refusal exists to end.
    """
    if versions is None:
        return dict.fromkeys(FEATURE_TABLE_NAMES)
    missing = sorted(set(FEATURE_TABLE_NAMES) - set(versions))
    unknown = sorted(set(versions) - set(FEATURE_TABLE_NAMES))
    if missing or unknown:
        raise KeyError(
            f"features_versions must name every table in {list(FEATURE_TABLE_NAMES)}, "
            f"or be None: missing {missing}, unknown {unknown}"
        )
    return dict(versions)


def _build_feature_frame(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
) -> DataFrame:
    """Silver -> the PR-opened spine -> the assembled v1 feature set, no
    label. Shared by every training-frame builder -- only the label join
    differs between the regression and classification paths (§5.3).
    """
    # Checked before the first read: a bad pin should not cost a Silver scan
    # to discover, and half a frame is worse than none.
    pins = feature_pins(features_versions)

    events = read_delta(spark, f"{silver_path}/clean", silver_version)
    spine = build_pr_opened_spine(events)

    def read(table: str) -> DataFrame:
        return read_delta(spark, f"{features_path}/{table}", pins[table])

    return assemble_training_set(
        spine,
        author_activity=read("author_activity"),
        repo_activity=read("repo_activity"),
        pr_static=read("pr_static"),
    )


def build_training_frame(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
    gold_version: int | None = None,
) -> pd.DataFrame:
    """Read Silver, the three v1 feature tables, and Gold's fact -- each at
    its own optionally-pinned Delta version -- and collect one pandas frame.

    `features_versions` is per table because their Delta versions genuinely
    differ; one shared number was only ever correct by coincidence.
    """
    training_frame = _build_feature_frame(
        spark,
        silver_path=silver_path,
        features_path=features_path,
        silver_version=silver_version,
        features_versions=features_versions,
    )
    fact_pull_request = _read_table(spark, gold_table, gold_version)
    return join_label(training_frame, fact_pull_request).toPandas()


def build_classification_frame(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    threshold_seconds: int,
    similarity_frame: DataFrame | None = None,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
    gold_version: int | None = None,
) -> pd.DataFrame:
    """Same shape as `build_training_frame`, with the wider §5.3 breach
    label in place of the continuous one.

    `similarity_frame` is `compute_pr_similarity`'s own output (Phase 5,
    §8.3a) -- left-joined on (repo_id, pr_number) when given. Absent, not
    zero-filled, when not: every existing call site passes nothing at all,
    so the frame it gets back is unchanged from before this parameter
    existed.
    """
    training_frame = _build_feature_frame(
        spark,
        silver_path=silver_path,
        features_path=features_path,
        silver_version=silver_version,
        features_versions=features_versions,
    )
    if similarity_frame is not None:
        training_frame = join_similarity_features(training_frame, similarity_frame)
    fact_pull_request = _read_table(spark, gold_table, gold_version)
    return join_breach_label(
        training_frame, fact_pull_request, threshold_seconds=threshold_seconds
    ).toPandas()
