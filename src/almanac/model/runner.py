"""build_training_frame -> train_model -> log_training_run -> (maybe)
register_champion. The I/O boundary for Phase 4, in the same shape as
almanac/features/runner.py.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.model.dataset import build_training_frame
from almanac.model.registry import register_champion, registered_model_name
from almanac.model.train import TrainResult, log_training_run, train_model
from almanac.spark import local_session


def _active_or_local_session() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-model")


def run_training(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_table: str,
    tracking_uri: str,
    experiment_name: str,
    register: bool,
    catalog: str = "almanac",
    schema: str = "models",
    model_name: str = "pr_review_sla_risk",
    registry_uri: str = "databricks-uc",
    silver_version: int | None = None,
    features_version: int | None = None,
    gold_version: int | None = None,
) -> TrainResult:
    """Registration fires only when `register` is set AND the model beat the
    baseline (§5.1's gate, enforced here rather than left to a human).
    """
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


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_training(
        _active_or_local_session(),
        silver_path=args.silver_path,
        features_path=args.features_path,
        gold_table=args.gold_table,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
        register=args.register,
        catalog=args.catalog,
        schema=args.schema,
        model_name=args.model_name,
        registry_uri=args.registry_uri,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
