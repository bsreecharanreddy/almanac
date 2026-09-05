"""build_training_frame -> train_model -> log_training_run -> (maybe)
register_champion -- and the same shape for the classification path
(§5.3): build_classification_frame -> train_classifier ->
log_classification_run -> (maybe) register_champion on the winning
candidate. The I/O boundary for Phase 4, in the same shape as
almanac/features/runner.py.
"""

from __future__ import annotations

import argparse
from typing import Literal

from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.model.dataset import build_classification_frame, build_training_frame
from almanac.model.registry import register_champion, registered_model_name
from almanac.model.train import (
    ClassificationResult,
    TrainResult,
    log_classification_run,
    log_training_run,
    train_classifier,
    train_model,
)
from almanac.spark import active_or_local_session


def run_training(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    tracking_uri: str,
    experiment_name: str,
    register: bool,
    objective: Literal["regression", "classification"] = "regression",
    threshold_seconds: int | None = None,
    catalog: str = "almanac",
    schema: str = "models",
    model_name: str = "pr_review_sla_risk",
    registry_uri: str = "databricks-uc",
    silver_version: int | None = None,
    features_version: int | None = None,
    gold_version: int | None = None,
) -> TrainResult | ClassificationResult:
    """Registration fires only when `register` is set AND the model beat
    the baseline -- §5.1's gate for regression, §5.3's for classification
    (the winning candidate, by average precision) -- enforced here rather
    than left to a human.
    """
    if objective == "classification" and threshold_seconds is None:
        raise ValueError("threshold_seconds is required when objective='classification' (§5.3)")

    if objective == "classification":
        assert threshold_seconds is not None  # narrowed by the guard above
        classification_frame = build_classification_frame(
            spark,
            silver_path=silver_path,
            features_path=features_path,
            gold_table=gold_table,
            threshold_seconds=threshold_seconds,
            silver_version=silver_version,
            features_version=features_version,
            gold_version=gold_version,
        )
        classification_result = train_classifier(classification_frame)
        model_uris = log_classification_run(
            classification_result, experiment_name=experiment_name, tracking_uri=tracking_uri
        )
        if register and classification_result.beats_baseline:
            name = registered_model_name(catalog, schema, model_name)
            winner = model_uris[classification_result.best_candidate]
            register_champion(winner, name=name, registry_uri=registry_uri)
        return classification_result

    frame = build_training_frame(
        spark,
        silver_path=silver_path,
        features_path=features_path,
        gold_table=gold_table,
        silver_version=silver_version,
        features_version=features_version,
        gold_version=gold_version,
    )
    result = train_model(frame)
    model_uri = log_training_run(result, experiment_name=experiment_name, tracking_uri=tracking_uri)
    if register and result.beats_baseline:
        name = registered_model_name(catalog, schema, model_name)
        register_champion(model_uri, name=name, registry_uri=registry_uri)
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the PR review-SLA risk model.")
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument(
        "--gold-table",
        required=True,
        help="Fully-qualified name of Gold's fact_pull_request (a dbt model, a metastore table).",
    )
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument(
        "--objective",
        choices=["regression", "classification"],
        default="regression",
        help="regression (§5.1, seconds) or classification (§5.3, breach/no-breach).",
    )
    parser.add_argument(
        "--threshold-seconds",
        type=int,
        default=None,
        help="The SLA breach threshold, in seconds. Required with --objective classification; "
        "passed explicitly, never recomputed by the job (§5.3).",
    )
    parser.add_argument("--catalog", default="almanac")
    parser.add_argument("--schema", default="models")
    parser.add_argument("--model-name", default="pr_review_sla_risk")
    parser.add_argument("--registry-uri", default="databricks-uc")
    parser.add_argument(
        "--register",
        action="store_true",
        help=(
            "Register as @champion in Unity Catalog if the model beats the "
            "baseline. Needs a live UC metastore -- off unless explicitly "
            "requested, same convention as almanac.features.runner's --register."
        ),
    )
    return parser


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """`_build_parser().parse_args` plus the one cross-field rule argparse
    itself cannot express: `--threshold-seconds` is required exactly when
    `--objective classification` is (§5.3), enforced here so a caller
    parsing args directly (as the CLI tests do) sees the same `SystemExit`
    `main()` would.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.objective == "classification" and args.threshold_seconds is None:
        parser.error("--threshold-seconds is required with --objective classification")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_training(
        active_or_local_session("almanac-model"),
        silver_path=args.silver_path,
        features_path=args.features_path,
        gold_table=args.gold_table,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
        register=args.register,
        objective=args.objective,
        threshold_seconds=args.threshold_seconds,
        catalog=args.catalog,
        schema=args.schema,
        model_name=args.model_name,
        registry_uri=args.registry_uri,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
