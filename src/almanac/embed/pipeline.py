"""Bronze's raw_json straight to embedded text (design doc §8.3a). PR/issue
title+body live only in Bronze -- Silver's typed schema never carries them,
since nothing needed them until now
(docs/findings/2026-09-04-phase-5-corpus-and-index-choice.md) -- so this
reads Bronze directly rather than through Silver, a deliberate exception to
the feature platform's Silver-native rule (§4.4a): point-in-time
correctness, the reason that rule exists, depends only on event ordering,
which Bronze carries exactly as authoritatively as Silver.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterator
from typing import Literal, Protocol, cast

import numpy as np
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from sentence_transformers import SentenceTransformer

from almanac.cli import run_cli
from almanac.features.registration import register_feature_table
from almanac.spark import active_or_local_session

DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDINGS_TABLE = "pr_issue_embeddings"

# event_type -> (GitHub's own event type name, the payload key its entity sits under)
_EVENT_SPECS: dict[str, tuple[str, str]] = {
    "pr": ("PullRequestEvent", "pull_request"),
    "issue": ("IssuesEvent", "issue"),
}

_EMBEDDING_SCHEMA = StructType(
    [
        StructField("entity_key", StringType()),
        StructField("event_time", TimestampType()),
        StructField("text_hash", StringType()),
        StructField("embedding", ArrayType(DoubleType())),
    ]
)


class TextEncoder(Protocol):
    """What embed_texts needs -- SentenceTransformer already satisfies this;
    a test double only needs these two lines, not the real model.

    `texts` is positional-only: SentenceTransformer.encode's own heavily
    overloaded signature names it differently and returns a broader union
    (Tensor/list[Tensor] variants) than this Protocol needs, so `load_encoder`
    casts rather than lean on structural typing to reconcile the two.
    """

    def encode(self, texts: list[str], /, *, batch_size: int) -> np.ndarray: ...


def load_encoder(model_name: str = DEFAULT_MODEL) -> TextEncoder:
    """The real encoder. Deliberately not unit-tested beyond this import --
    downloading weights belongs to Task 7's real run, not a unit test."""
    return cast(TextEncoder, SentenceTransformer(model_name))


def extract_texts(bronze: DataFrame, *, event_type: Literal["pr", "issue"]) -> DataFrame:
    """One row per opened PR/issue carrying non-null title *and* body:
    (entity_key, event_time, text, text_hash). text_hash identifies
    *content*, not entity -- two entities sharing identical text still get
    two output rows, each embedded (a Task 4 neighbor query needs both).
    """
    gh_type, payload_key = _EVENT_SPECS[event_type]
    title = F.get_json_object("raw_json", f"$.payload.{payload_key}.title")
    body = F.get_json_object("raw_json", f"$.payload.{payload_key}.body")
    text = F.concat_ws("\n\n", title, body)

    opened = (
        (F.get_json_object("raw_json", "$.type") == gh_type)
        & (F.get_json_object("raw_json", "$.payload.action") == "opened")
        & title.isNotNull()
        & body.isNotNull()
    )
    return bronze.where(opened).select(
        F.concat_ws(
            ":",
            F.lit(event_type),
            F.get_json_object("raw_json", "$.repo.id"),
            F.get_json_object("raw_json", "$.payload.number"),
        ).alias("entity_key"),
        F.to_timestamp(F.get_json_object("raw_json", f"$.payload.{payload_key}.created_at")).alias(
            "event_time"
        ),
        text.alias("text"),
        F.sha2(text, 256).alias("text_hash"),
    )


def embed_texts(texts: list[str], encoder: TextEncoder, *, batch_size: int = 64) -> np.ndarray:
    """Vectors in `texts`' own order -- callers zip them back onto entity_key."""
    return np.asarray(encoder.encode(texts, batch_size=batch_size))


def _pending_texts(
    spark: SparkSession,
    *,
    bronze_path: str,
    embeddings_path: str,
    event_type: Literal["pr", "issue"],
) -> DataFrame:
    """extract_texts, minus whatever text_hash the embeddings table already
    carries -- shared by both run_embedding_pipeline variants below, since
    "which text is new" does not depend on how the encode step scales.
    """
    bronze = spark.read.format("delta").load(bronze_path)
    texts = extract_texts(bronze, event_type=event_type)
    if DeltaTable.isDeltaTable(spark, embeddings_path):
        already_embedded = spark.read.format("delta").load(embeddings_path).select("text_hash")
        texts = texts.join(already_embedded, on="text_hash", how="left_anti")
    return texts


def _register_if_requested(
    spark: SparkSession, *, embeddings_path: str, register: bool, schema: str
) -> None:
    if register and DeltaTable.isDeltaTable(spark, embeddings_path):
        # Same external-table registration run_features already uses --
        # Vector Search's source_table wants a three-level UC name, not a
        # bare path, same reason Gold's fact table stopped being a path
        # (Task 9). Governance metadata only: the join logic that makes
        # retrieval point-in-time-correct lives in features/similarity.py,
        # not here.
        register_feature_table(spark, table=EMBEDDINGS_TABLE, path=embeddings_path, schema=schema)


def run_embedding_pipeline(
    spark: SparkSession,
    *,
    bronze_path: str,
    embeddings_path: str,
    event_type: Literal["pr", "issue"],
    encoder: TextEncoder,
    batch_size: int = 64,
    register: bool = False,
    schema: str = "embeddings",
) -> int:
    """Bronze -> this event type's slice of the embeddings table, incremental,
    collecting every pending row to the driver for one `encoder.encode` call.

    Correct and fast enough for a small corpus or a test double -- wrong at
    the real 14.9M-text scale (Task 1's measurement): 151.1 texts/sec
    single-threaded is ~27h, and `.toPandas()` on the whole pending set
    risks driver memory too. `run_embedding_pipeline_distributed` is the
    real run's own path; this one stays because its encoder is directly
    injectable, which a `mapInPandas` closure's per-partition model load
    cannot be (tests never download real weights).
    """
    texts = _pending_texts(
        spark, bronze_path=bronze_path, embeddings_path=embeddings_path, event_type=event_type
    )

    pdf = texts.toPandas()
    written = 0
    if not pdf.empty:
        vectors = embed_texts(pdf["text"].tolist(), encoder, batch_size=batch_size)
        # createDataFrame with an explicit schema maps by position, not by name
        # -- and the left_anti join above reorders columns (its `on` column
        # moves first), so this reselects by name rather than trust either
        # side's order.
        ordered = pdf.assign(embedding=[v.tolist() for v in vectors])[
            [f.name for f in _EMBEDDING_SCHEMA.fields]
        ]
        to_write = spark.createDataFrame(ordered, schema=_EMBEDDING_SCHEMA)
        # CDF is a Vector Search prerequisite, not this project's own choice:
        # a Standard endpoint's DELTA_SYNC index refuses to sync from a table
        # without it (Databricks' own vector-search docs, checked live
        # 2026-09-04). Effective only on the write that creates the table --
        # both PR and issue rows share one table, so whichever event_type
        # runs first sets it for both.
        to_write.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save(embeddings_path)
        written = len(pdf)

    _register_if_requested(spark, embeddings_path=embeddings_path, register=register, schema=schema)
    return written


def _embed_partition(
    model_name: str, batch_size: int
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """One `load_encoder` call per Spark partition, not per row or per
    call: `SentenceTransformer` does not serialize across the driver/
    executor boundary the way a model *name* does, and shipping one
    already-loaded instance per task would pay its own weight-transfer
    cost for nothing. Standard `mapInPandas` shape, same reason
    `embed_texts` already batches within a partition.
    """

    def embed(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        encoder: TextEncoder | None = None
        for pdf in pdfs:
            if pdf.empty:
                continue
            if encoder is None:  # loaded lazily: an empty partition never pays for it
                encoder = load_encoder(model_name)
            vectors = embed_texts(pdf["text"].tolist(), encoder, batch_size=batch_size)
            yield pdf.assign(embedding=[v.tolist() for v in vectors])[
                [f.name for f in _EMBEDDING_SCHEMA.fields]
            ]

    return embed


def run_embedding_pipeline_distributed(
    spark: SparkSession,
    *,
    bronze_path: str,
    embeddings_path: str,
    event_type: Literal["pr", "issue"],
    model_name: str = DEFAULT_MODEL,
    batch_size: int = 64,
    num_partitions: int = 16,
    register: bool = False,
    schema: str = "embeddings",
) -> int:
    """Same incremental shape as `run_embedding_pipeline`, but the encode
    step runs through `mapInPandas` -- one model load per partition,
    parallel across every executor, not collected to the driver.

    `num_partitions` bounds how many times `load_encoder` runs: measured
    for real 2026-09-05 (docs/findings/), Spark's own default partition
    count on the real corpus was 967 -- one `SentenceTransformer`
    construction per ~15K-row split, each taking ~12-14 minutes,
    overwhelmingly dominated by model-load overhead rather than the
    actual encode work, turning a ~1.7h estimate into 50+ real hours
    before the run was cancelled. Repartitioning first bounds the number
    of model loads to `num_partitions`, matching this project's own
    4-worker cluster shape rather than the upstream read's arbitrary
    split. Deliberately not unit-tested beyond `_pending_texts`' own
    coverage and the partition-count test above -- a real `mapInPandas`
    call downloads real weights inside a spawned worker process, the
    same "verified only against the real endpoint" boundary as
    `load_encoder` itself.
    """
    texts = _pending_texts(
        spark, bronze_path=bronze_path, embeddings_path=embeddings_path, event_type=event_type
    )
    written = texts.count()
    if written > 0:
        embedded = texts.repartition(num_partitions).mapInPandas(
            _embed_partition(model_name, batch_size), schema=_EMBEDDING_SCHEMA
        )
        embedded.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save(embeddings_path)

    _register_if_requested(spark, embeddings_path=embeddings_path, register=register, schema=schema)
    return written


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Embed Bronze's opened-PR/issue text incrementally (design doc §8.3a)."
    )
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--embeddings-path", required=True)
    parser.add_argument("--model-name", default=DEFAULT_MODEL)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=16,
        help=(
            "Bounds how many times load_encoder runs (one SentenceTransformer "
            "construction per partition, not per row). Default matches this "
            "project's 4-worker job-cluster shape (16 vCPUs) -- measured "
            "2026-09-05 that Spark's own default partition count on the real "
            "corpus (967) turned a ~1.7h estimate into 50+ real hours, "
            "dominated by model-load overhead, not the encode work itself."
        ),
    )
    parser.add_argument("--schema", default="embeddings")
    parser.add_argument(
        "--register",
        action="store_true",
        help=(
            "Register the embeddings table as a UC external table after writing "
            "-- Task 3's Vector Search index needs this; the local Derby "
            "metastore never runs this flag."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Both event types, one job run: a single embeddings table serves
    Task 3's one index, and `run_embedding_pipeline_distributed` already
    dedupes by text_hash regardless of which type lands first.
    """
    args = _build_parser().parse_args(argv)
    spark = active_or_local_session("almanac-embed")
    written = 0
    for event_type in ("pr", "issue"):
        written += run_embedding_pipeline_distributed(
            spark,
            bronze_path=args.bronze_path,
            embeddings_path=args.embeddings_path,
            event_type=event_type,
            model_name=args.model_name,
            batch_size=args.batch_size,
            num_partitions=args.num_partitions,
            register=args.register,
            schema=args.schema,
        )
    print(f"Embedded {written} new texts.")
    return 0


if __name__ == "__main__":
    run_cli(main)
