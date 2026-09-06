"""build_similarity_spine: the PR-opened spine, each row's own embedding
joined in by entity_key -- the real-run glue between Task 2's embeddings
table and Task 4's compute_pr_similarity (design doc §8.3a).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.similarity_runner import build_similarity_spine

pytestmark = pytest.mark.spark

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string"
)


def test_reconstructs_entity_key_from_typed_columns_to_match_task_2s_format(
    spark: SparkSession,
) -> None:
    """Task 2's extract_texts builds entity_key from Bronze's raw JSON
    strings ("pr:{repo_id}:{pr_number}"); this reconstructs the same
    string from Silver's typed long columns -- proven here by joining
    successfully, not just by inspection.
    """
    events = spark.createDataFrame(
        [
            (1, 10, datetime(2025, 8, 1, tzinfo=UTC), "PullRequestEvent", "opened", "alice"),
            (1, 11, datetime(2025, 8, 2, tzinfo=UTC), "PullRequestEvent", "opened", "bob"),
        ],
        _SILVER_SCHEMA,
    )
    embeddings = spark.createDataFrame(
        [("pr:1:10", [0.1, 0.2])],  # only pr_number=10 was ever embedded
        "entity_key string, embedding array<double>",
    )

    result = {
        r["pr_number"]: r["embedding"] for r in build_similarity_spine(events, embeddings).collect()
    }

    assert result == {10: [0.1, 0.2], 11: None}


def test_non_opened_and_non_pr_events_are_excluded(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            (1, 10, datetime(2025, 8, 1, tzinfo=UTC), "PullRequestEvent", "closed", "alice"),
            (1, 11, datetime(2025, 8, 1, tzinfo=UTC), "IssuesEvent", "opened", "bob"),
        ],
        _SILVER_SCHEMA,
    )
    embeddings = spark.createDataFrame([], "entity_key string, embedding array<double>")

    assert build_similarity_spine(events, embeddings).count() == 0


def test_since_date_scopes_the_spine_to_the_embedded_window(spark: SparkSession) -> None:
    """The spine must not reach back before the embeddings job's own
    --since-date: the index has no vectors there and the point-in-time
    filter only returns older neighbors, so those rows just cost a no-op
    (2026-09-06 findings -- Task 4's first real run did exactly this)."""
    events = spark.createDataFrame(
        [
            (
                1,
                10,
                datetime(2025, 8, 1, tzinfo=UTC),
                "PullRequestEvent",
                "opened",
                "a",
                "2025-08-01",
            ),
            (
                1,
                11,
                datetime(2025, 9, 25, tzinfo=UTC),
                "PullRequestEvent",
                "opened",
                "b",
                "2025-09-25",
            ),
        ],
        _SILVER_SCHEMA + ", event_date string",
    )
    embeddings = spark.createDataFrame([], "entity_key string, embedding array<double>")

    scoped = build_similarity_spine(events, embeddings, since_date="2025-09-20")

    assert [r["pr_number"] for r in scoped.collect()] == [11]
