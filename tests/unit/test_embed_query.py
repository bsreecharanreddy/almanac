"""similar_prs: §6's whole POST /similar-prs contract, demoed as a plain
callable against Task 3's index (design doc §8.3a) -- VectorSearchIndex
itself is deliberately not exercised here, the same "verified only against
the real endpoint" precedent as embed.pipeline's load_encoder.
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from almanac.embed.query import similar_prs
from almanac.features.similarity import Neighbor
from tests.helpers import FakeSimilarityIndex

pytestmark = pytest.mark.spark

_AS_OF = datetime(2025, 8, 13, tzinfo=UTC)
_BEFORE = datetime(2025, 8, 1, tzinfo=UTC)
_AFTER = datetime(2025, 8, 20, tzinfo=UTC)


def _embeddings(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("pr:1:10", [0.0, 0.0]), ("pr:1:11", None)],
        "entity_key string, embedding array<double>",
    )


def test_similar_prs_returns_neighbours_and_scores(spark: SparkSession) -> None:
    embeddings = _embeddings(spark)
    index = FakeSimilarityIndex(
        [
            (Neighbor("pr:2:1", _BEFORE, 0.91), [0.0, 0.0]),
            (Neighbor("pr:2:2", _AFTER, 0.99), [0.0, 0.0]),  # after as_of -- must not appear
        ]
    )

    result = similar_prs(1, 10, _AS_OF, index=index, embeddings=embeddings)

    assert result == [{"entity_key": "pr:2:1", "event_time": _BEFORE.isoformat(), "score": 0.91}]


def test_similar_prs_returns_empty_when_the_pr_has_no_embedding(spark: SparkSession) -> None:
    embeddings = _embeddings(spark)
    index = FakeSimilarityIndex([(Neighbor("pr:2:1", _BEFORE, 0.91), [0.0, 0.0])])

    assert similar_prs(1, 11, _AS_OF, index=index, embeddings=embeddings) == []


def test_similar_prs_returns_empty_for_an_unknown_pr(spark: SparkSession) -> None:
    embeddings = _embeddings(spark)
    index = FakeSimilarityIndex([(Neighbor("pr:2:1", _BEFORE, 0.91), [0.0, 0.0])])

    assert similar_prs(1, 999, _AS_OF, index=index, embeddings=embeddings) == []
