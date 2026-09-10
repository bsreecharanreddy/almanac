"""Tools 1-3: point-in-time features, the champion's score, and the
contributions behind it -- each read through the platform's own path rather
than a parallel implementation.

Design doc S6 calls `get_features` "the demo that carries the interview":
two calls at different `as_of` values on the same entity return different
vectors, and each is exactly what the real training-time join would have
produced. `predict` and `explain` are S4.2's recorded response to Phase 8's
drift finding, executable for the first time: a reduced-era window gets a
structured refusal naming the missing feature, never a fabricated number.
"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.agent.schemas import (
    Contribution,
    Direction,
    ExplainInput,
    ExplainOutput,
    ExplainResult,
    FeatureProvenance,
    GetFeaturesInput,
    GetFeaturesResult,
    ModelProvenance,
    PredictInput,
    PredictOutput,
    PredictResult,
    Refusal,
)
from almanac.features.assemble import assemble_training_set
from almanac.features.spine import build_pr_opened_spine
from almanac.model import drift
from almanac.model.contributions import BASELINE_COLUMN, ContributionModel, contribution_frame
from almanac.model.dataset import FEATURE_TABLE_NAMES, feature_pins, read_delta
from almanac.model.score import ProbabilityModel, positive_class_probability
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
    # None survives here rather than crashing on float(None): a reduced-era
    # window genuinely has no is_draft, and reporting that honestly is this
    # tool's job (schemas.py) -- refusing to score it is Task 4's predict.
    features = {name: (None if row[name] is None else float(row[name])) for name in FEATURE_COLUMNS}

    delta_versions = {"events": _resolved_version(spark, silver_delta_path, silver_version)}
    for table in FEATURE_TABLE_NAMES:
        delta_versions[table] = _resolved_version(spark, f"{features_path}/{table}", pins[table])

    return GetFeaturesResult(
        entity=request.entity,
        as_of=request.as_of,
        features=features,
        provenance=FeatureProvenance(delta_versions=delta_versions),
    )


def predict(
    spark: SparkSession,
    model: ProbabilityModel,
    request: PredictInput,
    *,
    model_version: str,
    silver_path: str,
    features_path: str,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
) -> PredictOutput:
    """The champion's score for one entity at one instant -- or a structured
    refusal naming the feature the champion needs but this window does not
    have. `Refusal` carries no numeric field at all (schemas.py): the
    fabrication `predict` must never commit is structurally impossible, not
    merely discouraged.
    """
    fetched = get_features(
        spark,
        GetFeaturesInput(entity=request.entity, as_of=request.as_of),
        silver_path=silver_path,
        features_path=features_path,
        silver_version=silver_version,
        features_versions=features_versions,
    )

    refusal = _refusal_for(fetched)
    if refusal is not None:
        return refusal

    frame = pd.DataFrame([fetched.features])
    score = float(positive_class_probability(model, frame).iloc[0])
    return PredictResult(
        entity=request.entity,
        as_of=request.as_of,
        breach_risk=score,
        provenance=ModelProvenance(
            model_version=model_version, delta_versions=fetched.provenance.delta_versions
        ),
    )


def explain(
    spark: SparkSession,
    model: ContributionModel,
    request: ExplainInput,
    *,
    model_version: str,
    silver_path: str,
    features_path: str,
    silver_version: int | None = None,
    features_versions: Mapping[str, int] | None = None,
) -> ExplainOutput:
    """The champion's own per-feature contributions behind one entity's score,
    strongest pull first and truncated to `top_k` -- or the same refusal
    `predict` returns, on the same condition.

    Refusing matters more here than it looks: LightGBM routes a null down a
    default direction and still emits a contribution for it, so explaining a
    window `predict` refuses would publish a number for a feature the window
    does not contain -- and Phase 10's verifier would read it as grounded.
    """
    fetched = get_features(
        spark,
        GetFeaturesInput(entity=request.entity, as_of=request.as_of),
        silver_path=silver_path,
        features_path=features_path,
        silver_version=silver_version,
        features_versions=features_versions,
    )

    refusal = _refusal_for(fetched)
    if refusal is not None:
        return refusal

    contributions = contribution_frame(model, pd.DataFrame([fetched.features])).iloc[0]
    # Ties break on the name so a replayed transcript orders identically
    # every time, which is what Phase 10's recorded runs rest on.
    ranked = sorted(FEATURE_COLUMNS, key=lambda name: (-abs(contributions[name]), name))

    return ExplainResult(
        entity=request.entity,
        as_of=request.as_of,
        baseline=float(contributions[BASELINE_COLUMN]),
        contributions=[
            _contribution(name, contributions[name]) for name in ranked[: request.top_k]
        ],
        top_k=request.top_k,
        provenance=ModelProvenance(
            model_version=model_version, delta_versions=fetched.provenance.delta_versions
        ),
    )


def _contribution(feature: str, value: float) -> Contribution:
    """`Contribution` re-derives the direction and refuses a mismatch, so this
    mapping is checked rather than trusted (schemas.py).
    """
    direction: Direction = "increases_risk" if value >= 0 else "decreases_risk"
    return Contribution(feature=feature, contribution=float(value), direction=direction)


def _refusal_for(fetched: GetFeaturesResult) -> Refusal | None:
    """The one schema-drift gate both scoring tools sit behind: `missing_for`
    in front of the model, never behind it, so a null is caught before it
    silently becomes a NaN the champion happily consumes.

    Shared rather than copied because both tools refuse on the same fact and
    must name the same offending feature; the first is reported because
    `missing_for` sorts, so the choice is deterministic rather than incidental.
    """
    missing = drift.missing_for(fetched.features)
    if not missing:
        return None
    offending = missing[0]
    return Refusal(
        entity=fetched.entity,
        as_of=fetched.as_of,
        missing_feature=offending.feature,
        reason=offending.response,
        provenance=fetched.provenance,
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
