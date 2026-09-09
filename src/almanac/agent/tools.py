"""Tool 1: point-in-time features, read through the platform's own as-of join.

Design doc S6 calls this "the demo that carries the interview": two calls
at different `as_of` values on the same entity return different vectors,
and each is exactly what the real training-time join would have produced
-- not a second implementation that merely resembles it.
"""

from __future__ import annotations

from collections.abc import Mapping

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.agent.schemas import FeatureProvenance, GetFeaturesInput, GetFeaturesResult
from almanac.features.assemble import assemble_training_set
from almanac.features.spine import build_pr_opened_spine
from almanac.model.dataset import FEATURE_TABLE_NAMES, feature_pins, read_delta
from almanac.model.train import FEATURE_COLUMNS


def get_features(
    spark: SparkSession,
    request: GetFeaturesInput,
    *,
    silver_path: str,
    features_path: str,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
) -> GetFeaturesResult:
    """The entity and the as-of instant Task 2 typed, in; a typed vector out.

    Reuses the platform's own path end to end: `build_pr_opened_spine` for
    the entity's `author_login` (author_activity's join key, absent from
    `EntityKey` itself), with its `as_of_timestamp` overridden to the
    requested instant, then `assemble_training_set` -- the exact function
    `model/dataset.py` builds training frames through -- for the as-of join
    proper. No shortcut around either: a tool that computed features by any
    other path would demonstrate nothing about the platform.
    """
    pins = feature_pins(features_versions)
    silver_delta_path = f"{silver_path}/clean"

    entity_events = read_delta(spark, silver_delta_path, silver_version).where(
        (F.col("repo_id") == request.entity.repo_id)
        & (F.col("pr_number") == request.entity.pr_number)
    )
    spine = build_pr_opened_spine(entity_events).withColumn(
        "as_of_timestamp", F.lit(request.as_of).cast("timestamp")
    )
    if spine.isEmpty():
        raise ValueError(
            f"no 'opened' PullRequestEvent for {request.entity!r} "
            f"(repo_id={request.entity.repo_id}, pr_number={request.entity.pr_number}) "
            f"in {silver_delta_path}"
        )

    def feature_table(table: str) -> DataFrame:
        return read_delta(spark, f"{features_path}/{table}", pins[table])

    assembled = assemble_training_set(
        spine,
        author_activity=feature_table("author_activity"),
        repo_activity=feature_table("repo_activity"),
        pr_static=feature_table("pr_static"),
    )
    row = assembled.select(*FEATURE_COLUMNS).collect()[0]
    features = {name: float(row[name]) for name in FEATURE_COLUMNS}

    delta_versions = {"events": _resolved_version(spark, silver_delta_path, silver_version)}
    for table in FEATURE_TABLE_NAMES:
        delta_versions[table] = _resolved_version(spark, f"{features_path}/{table}", pins[table])

    return GetFeaturesResult(
        entity=request.entity,
        as_of=request.as_of,
        features=features,
        provenance=FeatureProvenance(delta_versions=delta_versions),
    )


def _resolved_version(spark: SparkSession, path: str, pinned: int | None) -> int:
    """The Delta version actually read: the pin if one was given, else the
    table's own latest commit -- provenance must name what was read, not
    merely what could have been asked for.
    """
    if pinned is not None:
        return pinned
    row = DeltaTable.forPath(spark, path).history(1).select("version").first()
    if row is None:
        raise RuntimeError(f"{path} has no Delta history")
    return int(row["version"])
