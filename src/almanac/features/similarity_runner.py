"""Materialises `compute_pr_similarity`'s output for real (design doc
§8.3a). Deliberately not a FeatureTableSpec / run_features entry
(features/runner.py's own docstring): this queries an external ANN index
per spine row, so it gets its own small runner instead of being forced
into that shape.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.cli import run_cli
from almanac.embed.query import load_index
from almanac.features.similarity import SimilarityIndex, compute_pr_similarity
from almanac.features.spine import build_pr_opened_spine
from almanac.model.dataset import compute_breach_labels
from almanac.spark import active_or_local_session


def build_similarity_spine(events: DataFrame, embeddings: DataFrame) -> DataFrame:
    """The PR-opened spine, each row's own embedding joined in by
    entity_key -- the same "pr:{repo_id}:{pr_number}" format Task 2's
    extract_texts builds from Bronze's raw JSON strings, reconstructed
    here from Silver's typed columns instead.
    """
    spine = build_pr_opened_spine(events)
    keyed = spine.withColumn(
        "entity_key",
        F.concat_ws(
            ":", F.lit("pr"), F.col("repo_id").cast("string"), F.col("pr_number").cast("string")
        ),
    )
    return keyed.join(
        embeddings.select("entity_key", "embedding"), on="entity_key", how="left"
    ).select("repo_id", "pr_number", "as_of_timestamp", "embedding")


def run_similarity(
    spark: SparkSession,
    *,
    silver_path: str,
    embeddings_path: str,
    similarity_path: str,
    gold_table: str,
    threshold_seconds: int,
    index: SimilarityIndex,
    k: int = 10,
    sample_size: int | None = None,
    sample_seed: int = 42,
) -> int:
    """Compute and overwrite the similarity feature table.

    `sample_size` bounds the spine before any index query runs: unlike
    every other v1 feature group (a pure Spark transform), each spine row
    here costs one real call against a paid, live ANN endpoint -- at the
    real ~13.18M-row PR-opened population (design doc §8.3a's own corpus
    measurement), querying all of it is a real-money, real-time decision,
    not a compute-distribution one. A fixed seed keeps the same sample
    reproducible across the with/without comparison this feeds.
    """
    events = spark.read.format("delta").load(f"{silver_path}/clean")
    embeddings = spark.read.format("delta").load(embeddings_path)
    spine = build_similarity_spine(events, embeddings)
    if sample_size is not None:
        spine = spine.orderBy(F.rand(seed=sample_seed)).limit(sample_size)

    fact_pull_request = spark.read.table(gold_table)
    resolutions = compute_breach_labels(fact_pull_request, threshold_seconds=threshold_seconds)

    result = compute_pr_similarity(spine, index, resolutions, k=k)
    result.write.format("delta").mode("overwrite").save(similarity_path)
    return result.count()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute pr_similarity over a (possibly sampled) real spine."
    )
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--embeddings-path", required=True)
    parser.add_argument("--similarity-path", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--threshold-seconds", type=int, required=True)
    parser.add_argument("--endpoint-name", required=True)
    parser.add_argument("--index-name", required=True)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Bound the spine before querying the real index. Omit for the full population.",
    )
    parser.add_argument("--sample-seed", type=int, default=42)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    index = load_index(endpoint_name=args.endpoint_name, index_name=args.index_name)
    written = run_similarity(
        active_or_local_session("almanac-similarity"),
        silver_path=args.silver_path,
        embeddings_path=args.embeddings_path,
        similarity_path=args.similarity_path,
        gold_table=args.gold_table,
        threshold_seconds=args.threshold_seconds,
        index=index,
        k=args.k,
        sample_size=args.sample_size,
        sample_seed=args.sample_seed,
    )
    print(f"Computed similarity for {written} spine rows.")
    return 0


if __name__ == "__main__":
    run_cli(main)
