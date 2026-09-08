"""Score the quarter with the registered champion and write the predictions table."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping

import mlflow
from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.contracts import apply_constraints, enforce
from almanac.model.dataset import FEATURE_TABLE_NAMES
from almanac.model.score import PREDICTIONS_CONTRACT, SCORE_COLUMN, ProbabilityModel, score_quarter
from almanac.spark import active_or_local_session


def load_champion(model_uri: str, registry_uri: str) -> ProbabilityModel:
    """The registered champion, as its native LightGBM object.

    `mlflow.pyfunc.load_model` would hand back the *serving* behaviour, which is
    exactly the boolean this job exists to get around -- the flavor's own loader
    returns the estimator, which still has `predict_proba`.
    """
    mlflow.set_registry_uri(registry_uri)
    return mlflow.lightgbm.load_model(model_uri)  # type: ignore[no-any-return]


def write_predictions(
    spark: SparkSession,
    *,
    out_path: str,
    model_uri: str,
    registry_uri: str,
    silver_path: str,
    features_path: str,
    gold_table: str,
    threshold_seconds: int,
    features_versions: Mapping[str, int] | None = None,
) -> int:
    """Score, enforce the contract, write, then put the rules on the table."""
    model = load_champion(model_uri, registry_uri)
    scored = score_quarter(
        spark,
        model,
        silver_path=silver_path,
        features_path=features_path,
        gold_table=gold_table,
        threshold_seconds=threshold_seconds,
        features_versions=features_versions,
    )
    enforce(scored, PREDICTIONS_CONTRACT)
    scored.write.format("delta").mode("overwrite").save(out_path)
    apply_constraints(spark, out_path, PREDICTIONS_CONTRACT)
    return scored.count()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--out-path", required=True)
    parser.add_argument("--model-uri", required=True, help="e.g. models:/cat.schema.name/1")
    parser.add_argument("--registry-uri", default="databricks-uc")
    parser.add_argument(
        "--threshold-seconds",
        type=int,
        required=True,
        help="The SLA breach threshold. Passed explicitly, never recomputed (§5.3).",
    )
    parser.add_argument(
        "--features-versions",
        default=None,
        help=(
            "JSON object pinning every feature table to its own Delta version, e.g. "
            f"'{{\"{FEATURE_TABLE_NAMES[0]}\": 3, ...}}'. Omit to read each live."
        ),
    )
    args = parser.parse_args()

    rows = write_predictions(
        active_or_local_session("almanac-score"),
        out_path=args.out_path,
        model_uri=args.model_uri,
        registry_uri=args.registry_uri,
        silver_path=args.silver_path,
        features_path=args.features_path,
        gold_table=args.gold_table,
        threshold_seconds=args.threshold_seconds,
        features_versions=json.loads(args.features_versions) if args.features_versions else None,
    )
    print(f"wrote {rows} scored rows to {args.out_path} ({SCORE_COLUMN} from {args.model_uri})")
    return 0


if __name__ == "__main__":
    run_cli(main)
