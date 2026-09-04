"""extract_texts / embed_texts / run_embedding_pipeline: Bronze's raw_json
straight to embedded text (§8.3a) -- Silver never carries title/body
(nothing needed them before this), so this reads Bronze directly rather
than through Silver's typed schema, docs/findings/2026-09-04-phase-5-corpus-and-index-choice.md.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.embed.pipeline import (
    embed_texts,
    extract_texts,
    run_embedding_pipeline,
)

pytestmark = pytest.mark.spark

_BRONZE_SCHEMA = (
    "raw_json string, ingested_at timestamp, source_file string, event_date string, event_hour int"
)


def _event(event_type: str, *, action: str, repo_id: int, number: int, **payload: object) -> str:
    return json.dumps(
        {
            "type": event_type,
            "repo": {"id": repo_id},
            "payload": {"action": action, "number": number, **payload},
        }
    )


def _pr_opened(repo_id: int, number: int, *, title: str | None, body: str | None) -> str:
    return _event(
        "PullRequestEvent",
        action="opened",
        repo_id=repo_id,
        number=number,
        pull_request={"title": title, "body": body, "created_at": "2025-08-01T00:00:00Z"},
    )


def _issue_opened(repo_id: int, number: int, *, title: str | None, body: str | None) -> str:
    return _event(
        "IssuesEvent",
        action="opened",
        repo_id=repo_id,
        number=number,
        issue={"title": title, "body": body, "created_at": "2025-08-01T00:00:00Z"},
    )


def _bronze(spark: SparkSession, *raw_jsons: str) -> DataFrame:
    now = datetime(2026, 9, 4, tzinfo=UTC)
    rows = [(raw, now, "fixture", "2025-08-01", 0) for raw in raw_jsons]
    return spark.createDataFrame(rows, _BRONZE_SCHEMA)


def test_extract_texts_keeps_only_opened_prs_with_both_title_and_body(
    spark: SparkSession,
) -> None:
    bronze = _bronze(
        spark,
        _pr_opened(1, 10, title="Fix the thing", body="Because it was broken."),
        _pr_opened(1, 11, title="No body PR", body=None),  # reduced era / empty -- excluded
        _event("PullRequestEvent", action="closed", repo_id=1, number=12),  # not opened
        _issue_opened(1, 13, title="An issue", body="Issue body"),  # wrong event_type for this call
    )

    texts = extract_texts(bronze, event_type="pr")

    rows = {r["entity_key"]: r["text"] for r in texts.collect()}
    assert rows == {"pr:1:10": "Fix the thing\n\nBecause it was broken."}


def test_extract_texts_reads_issues_when_asked(spark: SparkSession) -> None:
    bronze = _bronze(
        spark,
        _pr_opened(1, 10, title="A PR", body="body"),
        _issue_opened(1, 20, title="An issue", body="Issue body text"),
    )

    texts = extract_texts(bronze, event_type="issue")

    rows = {r["entity_key"]: r["text"] for r in texts.collect()}
    assert rows == {"issue:1:20": "An issue\n\nIssue body text"}


def test_extract_texts_hashes_content_not_entity(spark: SparkSession) -> None:
    bronze = _bronze(
        spark,
        _pr_opened(1, 10, title="Same", body="text"),
        _pr_opened(2, 99, title="Same", body="text"),
    )

    texts = extract_texts(bronze, event_type="pr").collect()

    assert len(texts) == 2
    hashes = {r["text_hash"] for r in texts}
    assert hashes == {texts[0]["text_hash"]}  # one distinct hash, shared by both rows


class _FakeEncoder:
    """Records every call so a test can assert what actually reached it,
    without downloading real weights."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def encode(self, texts: list[str], *, batch_size: int) -> np.ndarray:
        self.calls.append(list(texts))
        return np.array([[float(len(t)), 0.0] for t in texts])


def test_embed_texts_preserves_order_and_uses_the_injected_encoder() -> None:
    encoder = _FakeEncoder()

    vectors = embed_texts(["a", "bb", "ccc"], encoder, batch_size=2)

    assert encoder.calls == [["a", "bb", "ccc"]]
    np.testing.assert_array_equal(vectors, [[1.0, 0.0], [2.0, 0.0], [3.0, 0.0]])


def test_run_embedding_pipeline_skips_already_embedded_hashes(
    spark: SparkSession, tmp_path: Path
) -> None:
    bronze_path = str(tmp_path / "bronze")
    embeddings_path = str(tmp_path / "embeddings")
    bronze = _bronze(
        spark,
        _pr_opened(1, 10, title="Old", body="already embedded"),
        _pr_opened(1, 11, title="New", body="not yet embedded"),
    )
    bronze.write.format("delta").save(bronze_path)

    # Seed the embeddings table with row 10 already present, in the exact
    # shape run_embedding_pipeline itself would have written it -- only its
    # text_hash matters for the skip, so the embedding value here is a stand-in.
    already_embedded = (
        extract_texts(bronze, event_type="pr")
        .where("entity_key = 'pr:1:10'")
        .drop("text")
        .withColumn("embedding", F.array(F.lit(0.0)))
    )
    already_embedded.write.format("delta").save(embeddings_path)

    encoder = _FakeEncoder()
    written = run_embedding_pipeline(
        spark,
        bronze_path=bronze_path,
        embeddings_path=embeddings_path,
        event_type="pr",
        encoder=encoder,
    )

    assert written == 1
    assert encoder.calls == [["New\n\nnot yet embedded"]]
