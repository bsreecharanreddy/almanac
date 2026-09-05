"""extract_texts / embed_texts / run_embedding_pipeline: Bronze's raw_json
straight to embedded text (§8.3a) -- Silver never carries title/body
(nothing needed them before this), so this reads Bronze directly rather
than through Silver's typed schema, docs/findings/2026-09-04-phase-5-corpus-and-index-choice.md.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.embed import pipeline
from almanac.embed.pipeline import (
    embed_texts,
    extract_texts,
    run_embedding_pipeline,
    run_embedding_pipeline_distributed,
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


def test_run_embedding_pipeline_registers_when_asked(spark: SparkSession, tmp_path: Path) -> None:
    bronze_path = str(tmp_path / "bronze")
    embeddings_path = str(tmp_path / "embeddings")
    bronze = _bronze(spark, _pr_opened(1, 10, title="A PR", body="body text"))
    bronze.write.format("delta").save(bronze_path)

    # A schema unique to this test, not a shared "embeddings": the spark
    # fixture's metastore is one session-wide Derby instance, and CREATE
    # TABLE IF NOT EXISTS would silently keep a stale LOCATION from an
    # earlier test that reused the same schema.table name -- the same
    # isolation hazard AuditChainTest documented in Canopica.
    schema = f"embeddings_{tmp_path.name}"

    written = run_embedding_pipeline(
        spark,
        bronze_path=bronze_path,
        embeddings_path=embeddings_path,
        event_type="pr",
        encoder=_FakeEncoder(),
        register=True,
        schema=schema,
    )

    assert written == 1
    assert spark.catalog.tableExists(f"{schema}.pr_issue_embeddings")


def test_embed_partition_batches_by_name_not_position(monkeypatch: pytest.MonkeyPatch) -> None:
    """`_embed_partition`'s closure is the only piece of
    run_embedding_pipeline_distributed real Spark's mapInPandas would
    otherwise hide inside a spawned worker process -- called directly here,
    with load_encoder monkeypatched, the same reselect-by-name discipline
    run_embedding_pipeline's own reselect exists for gets checked without
    needing a real model or real Spark distribution.
    """
    monkeypatch.setattr(pipeline, "load_encoder", lambda model_name: _FakeEncoder())
    embed = pipeline._embed_partition("fake-model", batch_size=2)

    # Deliberately out of _EMBEDDING_SCHEMA's own column order, the same
    # shape a left_anti join can produce upstream.
    pdf = pd.DataFrame(
        {
            "text_hash": ["h1", "h2"],
            "entity_key": ["pr:1:10", "pr:1:11"],
            "event_time": [datetime(2025, 8, 1, tzinfo=UTC)] * 2,
            "text": ["a", "bb"],
        }
    )

    (result,) = list(embed(iter([pdf])))

    assert list(result.columns) == ["entity_key", "event_time", "text_hash", "embedding"]
    assert result["embedding"].tolist() == [[1.0, 0.0], [2.0, 0.0]]


def test_embed_partition_sets_fork_safe_env_and_reports_progress(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The mapInPandas worker is forked from pyspark.daemon; tokenizers'
    Rayon pool and torch's OpenMP pool both deadlock across that fork
    unless told not to parallelise (2026-09-05 findings -- run
    630533628470951 froze here, workers alive, no output for 10-50 min).
    The closure sets both env vars defensively and prints per-batch
    progress to stderr, the one channel that reaches the delivered logs.
    """
    monkeypatch.delenv("TOKENIZERS_PARALLELISM", raising=False)
    monkeypatch.delenv("OMP_NUM_THREADS", raising=False)
    monkeypatch.setattr(pipeline, "load_encoder", lambda model_name: _FakeEncoder())
    embed = pipeline._embed_partition("fake-model", batch_size=2)

    pdf = pd.DataFrame(
        {
            "entity_key": ["pr:1:10", "pr:1:11"],
            "event_time": [datetime(2025, 8, 1, tzinfo=UTC)] * 2,
            "text_hash": ["h1", "h2"],
            "text": ["a", "bb"],
        }
    )
    list(embed(iter([pdf])))

    assert os.environ["TOKENIZERS_PARALLELISM"] == "false"
    assert os.environ["OMP_NUM_THREADS"] == "1"
    assert "[embed] 2 texts" in capsys.readouterr().err


def test_run_embedding_pipeline_distributed_skips_without_loading_a_model(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing pending -> `written == 0` before `_embed_partition` (and
    therefore `load_encoder`) ever runs -- monkeypatching load_encoder to
    fail proves this path never reaches it, not just that the count is 0.
    """

    def _fail(model_name: str) -> None:
        raise AssertionError("load_encoder must not run when nothing is pending")

    monkeypatch.setattr(pipeline, "load_encoder", _fail)

    bronze_path = str(tmp_path / "bronze")
    embeddings_path = str(tmp_path / "embeddings")
    bronze = _bronze(spark, _pr_opened(1, 10, title="Old", body="already embedded"))
    bronze.write.format("delta").save(bronze_path)
    extract_texts(bronze, event_type="pr").withColumn("embedding", F.array(F.lit(0.0))).drop(
        "text"
    ).write.format("delta").save(embeddings_path)

    written = run_embedding_pipeline_distributed(
        spark, bronze_path=bronze_path, embeddings_path=embeddings_path, event_type="pr"
    )

    assert written == 0


def test_run_embedding_pipeline_distributed_bounds_model_loads_to_num_partitions(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real first run (2026-09-05, docs/findings/) measured Spark's
    own default partition count on the real corpus at 967 -- one
    SentenceTransformer construction per partition, each costing ~12-14
    minutes, dominated by model-load overhead rather than the actual
    encode work. repartition(num_partitions) is what bounds that; this
    proves it holds regardless of how many rows are pending, with a
    monkeypatched load_encoder that counts its own calls rather than
    downloading real weights.
    """
    calls: list[str] = []

    def _counting_load_encoder(model_name: str) -> _FakeEncoder:
        calls.append(model_name)
        return _FakeEncoder()

    monkeypatch.setattr(pipeline, "load_encoder", _counting_load_encoder)

    bronze_path = str(tmp_path / "bronze")
    embeddings_path = str(tmp_path / "embeddings")
    bronze = _bronze(
        spark,
        *[_pr_opened(1, n, title=f"PR {n}", body=f"body {n}") for n in range(10, 18)],
    )
    bronze.write.format("delta").save(bronze_path)

    written = run_embedding_pipeline_distributed(
        spark,
        bronze_path=bronze_path,
        embeddings_path=embeddings_path,
        event_type="pr",
        num_partitions=2,
    )

    assert written == 8
    assert len(calls) <= 2


def test_run_embedding_pipeline_distributed_limit_caps_the_pending_set(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--limit` is the bounded proof run (2026-09-05 fork-deadlock
    findings): the count and the encode both see the capped set, and only
    that many rows land in the table.
    """
    monkeypatch.setattr(pipeline, "load_encoder", lambda model_name: _FakeEncoder())
    bronze_path = str(tmp_path / "bronze")
    embeddings_path = str(tmp_path / "embeddings")
    bronze = _bronze(
        spark,
        *[_pr_opened(1, n, title=f"PR {n}", body=f"body {n}") for n in range(10, 18)],
    )
    bronze.write.format("delta").save(bronze_path)

    written = run_embedding_pipeline_distributed(
        spark,
        bronze_path=bronze_path,
        embeddings_path=embeddings_path,
        event_type="pr",
        num_partitions=2,
        limit=3,
    )

    assert written == 3
    assert spark.read.format("delta").load(embeddings_path).count() == 3
