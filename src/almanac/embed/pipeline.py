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

from typing import Literal, Protocol, cast

import numpy as np
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

DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

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


def run_embedding_pipeline(
    spark: SparkSession,
    *,
    bronze_path: str,
    embeddings_path: str,
    event_type: Literal["pr", "issue"],
    encoder: TextEncoder,
    batch_size: int = 64,
) -> int:
    """Bronze -> this event type's slice of the embeddings table, incremental.

    Only text whose hash isn't already embedded reaches the encoder: an
    embedding is expensive per row and a PR's text never changes once
    Bronze lands it, so re-embedding unchanged rows on every run would be
    pure waste -- run_features's full-overwrite shape is wrong here, not
    just simpler.
    """
    bronze = spark.read.format("delta").load(bronze_path)
    texts = extract_texts(bronze, event_type=event_type)

    if DeltaTable.isDeltaTable(spark, embeddings_path):
        already_embedded = spark.read.format("delta").load(embeddings_path).select("text_hash")
        texts = texts.join(already_embedded, on="text_hash", how="left_anti")

    pdf = texts.toPandas()
    if pdf.empty:
        return 0

    vectors = embed_texts(pdf["text"].tolist(), encoder, batch_size=batch_size)
    # createDataFrame with an explicit schema maps by position, not by name --
    # and the left_anti join above reorders columns (its `on` column moves
    # first), so this reselects by name rather than trust either side's order.
    ordered = pdf.assign(embedding=[v.tolist() for v in vectors])[
        [f.name for f in _EMBEDDING_SCHEMA.fields]
    ]
    to_write = spark.createDataFrame(ordered, schema=_EMBEDDING_SCHEMA)
    to_write.write.format("delta").mode("append").save(embeddings_path)
    return len(pdf)
