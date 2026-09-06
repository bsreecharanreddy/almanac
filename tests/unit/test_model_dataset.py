"""join_label / join_breach_label: attach Gold's label onto a point-in-
time-correct training frame -- a label is supervision, not a feature, so
this lives outside almanac/features/ (which never reads Gold, §3.1)
without extending that same boundary to the label (§5.2).
"""

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from almanac.model.dataset import join_breach_label, join_label, join_similarity_features

pytestmark = pytest.mark.spark


def test_joins_the_label_and_drops_rows_with_no_defined_outcome(spark: SparkSession) -> None:
    training_frame = spark.createDataFrame(
        [(1, 5, "alice"), (1, 6, "bob")],
        "repo_id long, pr_number long, author_login string",
    )
    fact_pull_request = spark.createDataFrame(
        [
            (1, 5, 3600, None),
            (1, 6, None, "right_censored"),
        ],
        "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string",
    )

    result = join_label(training_frame, fact_pull_request).collect()

    assert len(result) == 1
    assert result[0]["pr_number"] == 5
    assert result[0]["time_to_first_response_seconds"] == 3600


def test_breach_label_covers_trainable_and_closed_no_response_and_excludes_the_rest(
    spark: SparkSession,
) -> None:
    """design doc §5.3: the trainable population widens to `closed_no_response`
    rows that sat open at least the threshold before closing unanswered;
    `author_unobserved` / `right_censored` / `draft` stay excluded.
    """
    threshold = 100
    t0 = datetime(2025, 8, 1, tzinfo=UTC)

    training_frame = spark.createDataFrame(
        [(1, n, "alice") for n in range(1, 8)],
        "repo_id long, pr_number long, author_login string",
    )
    fact_pull_request = spark.createDataFrame(
        [
            (1, 1, 150, None, None, None),  # trainable, over threshold -> breach
            (1, 2, 50, None, None, None),  # trainable, at/under threshold -> not a breach
            (
                1,
                3,
                None,
                "closed_no_response",
                t0,
                t0 + timedelta(seconds=150),
            ),  # open long -> breach
            (
                1,
                4,
                None,
                "closed_no_response",
                t0,
                t0 + timedelta(seconds=50),
            ),  # closed fast -> not
            (1, 5, None, "author_unobserved", None, None),  # excluded: no opened_at anchor
            (1, 6, None, "right_censored", None, None),  # excluded: outcome not yet known
            (1, 7, None, "draft", None, None),  # excluded: not in the response-SLA population
        ],
        "repo_id long, pr_number long, time_to_first_response_seconds long, "
        "label_exclusion string, opened_at timestamp, closed_at timestamp",
    )

    result = {
        r["pr_number"]: r["breach"]
        for r in join_breach_label(
            training_frame, fact_pull_request, threshold_seconds=threshold
        ).collect()
    }

    assert result == {1: True, 2: False, 3: True, 4: False}


def test_join_similarity_features_keeps_every_row_even_when_never_scored(
    spark: SparkSession,
) -> None:
    """A left join, not inner (design doc §8.3a): a PR the similarity index
    never scored keeps its row, with the similar_* columns null -- unlike
    the label joins above, absence here is not exclusion."""
    training_frame = spark.createDataFrame(
        [(1, 5, "alice"), (1, 6, "bob")],
        "repo_id long, pr_number long, author_login string",
    )
    similarity_frame = spark.createDataFrame(
        [(1, 5, 3, 0.5)],
        "repo_id long, pr_number long, similar_neighbor_count long, "
        "similar_prior_breach_rate double",
    )

    result = {
        r["pr_number"]: r["similar_prior_breach_rate"]
        for r in join_similarity_features(training_frame, similarity_frame).collect()
    }

    assert result == {5: 0.5, 6: None}
