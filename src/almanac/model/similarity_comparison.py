"""Task 6's real gate (design doc §8.3a): does adding `pr_similarity`
beat the already-registered champion, not its own without-similarity
sibling. Two full `train_classifier` runs against the same split, same
population -- the only difference between them is which columns the
model can see.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.model.dataset import build_classification_frame
from almanac.model.registry import register_champion, registered_model_name
from almanac.model.train import (
    FEATURE_COLUMNS,
    SIMILARITY_FEATURE_COLUMNS,
    ClassificationResult,
    log_classification_run,
    train_classifier,
)
from almanac.spark import active_or_local_session

# Measured 2026-09-04 (docs/findings/2026-09-04-classification-model-
# serving-measured.md): the currently-registered @champion's own
# average_precision. The gate this task exists to check against -- not
# read live from the registry, since this project already states it as a
# specific, documented fact rather than a moving target this comparison
# would otherwise have to fetch.
CHAMPION_AVERAGE_PRECISION = 0.612


@dataclass(frozen=True)
class SimilarityComparisonResult:
    without_similarity: ClassificationResult
    with_similarity: ClassificationResult
    beats_champion: bool


def run_similarity_comparison(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    threshold_seconds: int,
    similarity_path: str,
    tracking_uri: str,
    experiment_name: str,
    register: bool,
    catalog: str = "almanac_dbx",
    schema: str = "models",
    model_name: str = "pr_review_sla_risk",
    registry_uri: str = "databricks-uc",
) -> SimilarityComparisonResult:
    similarity_frame = spark.read.format("delta").load(similarity_path)
    frame = build_classification_frame(
        spark,
        silver_path=silver_path,
        features_path=features_path,
        gold_table=gold_table,
        threshold_seconds=threshold_seconds,
        similarity_frame=similarity_frame,
    )
    # Same population for both arms, not just the same split: a row the
    # similarity runner never sampled has null similar_* columns, and
    # comparing a full-population "without" arm against a sampled-only
    # "with" arm would confound the similarity columns' effect with a
    # plain sample-size difference.
    scored = frame[frame["similar_neighbor_count"].notna()].reset_index(drop=True)

    without_similarity = train_classifier(scored, feature_columns=FEATURE_COLUMNS)
    with_similarity = train_classifier(
        scored, feature_columns=FEATURE_COLUMNS + SIMILARITY_FEATURE_COLUMNS
    )

    log_classification_run(
        without_similarity,
        experiment_name=f"{experiment_name}-without-similarity",
        tracking_uri=tracking_uri,
    )
    with_uris = log_classification_run(
        with_similarity, experiment_name=experiment_name, tracking_uri=tracking_uri
    )

    with_ap = with_similarity.candidates[with_similarity.best_candidate].average_precision
    beats_champion = with_ap > CHAMPION_AVERAGE_PRECISION
    if register and beats_champion:
        name = registered_model_name(catalog, schema, model_name)
        register_champion(
            with_uris[with_similarity.best_candidate], name=name, registry_uri=registry_uri
        )

    return SimilarityComparisonResult(
        without_similarity=without_similarity,
        with_similarity=with_similarity,
        beats_champion=beats_champion,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare with/without pr_similarity against the registered champion."
    )
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--threshold-seconds", type=int, required=True)
    parser.add_argument("--similarity-path", required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--catalog", default="almanac_dbx")
    parser.add_argument("--schema", default="models")
    parser.add_argument("--model-name", default="pr_review_sla_risk")
    parser.add_argument("--registry-uri", default="databricks-uc")
    parser.add_argument("--register", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run_similarity_comparison(
        active_or_local_session("almanac-similarity-comparison"),
        silver_path=args.silver_path,
        features_path=args.features_path,
        gold_table=args.gold_table,
        threshold_seconds=args.threshold_seconds,
        similarity_path=args.similarity_path,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
        register=args.register,
        catalog=args.catalog,
        schema=args.schema,
        model_name=args.model_name,
        registry_uri=args.registry_uri,
    )
    without_ap = result.without_similarity.candidates[result.without_similarity.best_candidate]
    with_ap = result.with_similarity.candidates[result.with_similarity.best_candidate]
    print(f"without-similarity average_precision: {without_ap.average_precision}")
    print(f"with-similarity average_precision: {with_ap.average_precision}")
    print(f"beats champion ({CHAMPION_AVERAGE_PRECISION}): {result.beats_champion}")
    return 0


if __name__ == "__main__":
    run_cli(main)
