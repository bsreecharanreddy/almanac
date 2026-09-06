"""compute_pr_similarity: retrieval's own point-in-time-correctness axis
(design doc §8.3a), on top of `as_of_join`'s -- a neighbor's own outcome
counts only if the neighbor itself resolved strictly before *this* spine
row's own as_of, not just because `resolutions` has since resolved it.
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import Column, DataFrame, SparkSession

from almanac.features.similarity import Neighbor, compute_pr_similarity
from tests.helpers import FakeSimilarityIndex

pytestmark = pytest.mark.spark

_T1 = datetime(2025, 8, 1, tzinfo=UTC)
_T2 = datetime(2025, 8, 2, tzinfo=UTC)
_T3 = datetime(2025, 8, 3, tzinfo=UTC)
_T4 = datetime(2025, 8, 4, tzinfo=UTC)

_SpineRow = tuple[int, int, datetime, list[float] | None]
_ResolutionRow = tuple[int, int, datetime, bool]


def _spine(spark: SparkSession, rows: list[_SpineRow]) -> DataFrame:
    return spark.createDataFrame(
        rows, "repo_id long, pr_number long, as_of_timestamp timestamp, embedding array<double>"
    )


def _resolutions(spark: SparkSession, rows: list[_ResolutionRow]) -> DataFrame:
    return spark.createDataFrame(
        rows, "repo_id long, pr_number long, closed_at timestamp, breach boolean"
    )


def test_a_neighbor_resolved_before_as_of_contributes_to_the_rate(spark: SparkSession) -> None:
    spine = _spine(spark, [(1, 100, _T3, [0.0, 0.0])])
    index = FakeSimilarityIndex([(Neighbor("pr:1:1", _T1, 0.0), [0.0, 0.0])])
    resolutions = _resolutions(spark, [(1, 1, _T2, True)])  # closed before spine's as_of

    result = compute_pr_similarity(spine, index, resolutions).collect()

    assert len(result) == 1
    assert result[0]["similar_neighbor_count"] == 1
    assert result[0]["similar_prior_breach_rate"] == 1.0


def test_an_unresolved_neighbor_is_excluded_from_the_rate_but_counted(
    spark: SparkSession,
) -> None:
    """The neighbor opened before as_of (eligible to be found at all) but
    its own outcome was not yet knowable at the spine row's as_of --
    resolutions has it resolving *after*, which is knowledge this spine
    row could not have had.
    """
    spine = _spine(spark, [(1, 100, _T3, [0.0, 0.0])])
    index = FakeSimilarityIndex([(Neighbor("pr:1:1", _T1, 0.0), [0.0, 0.0])])
    resolutions = _resolutions(spark, [(1, 1, _T4, True)])  # closes after spine's as_of

    result = compute_pr_similarity(spine, index, resolutions).collect()

    assert result[0]["similar_neighbor_count"] == 1
    assert result[0]["similar_prior_breach_rate"] is None  # null, not 0.0


def test_a_neighbor_opened_after_as_of_never_appears_at_all(spark: SparkSession) -> None:
    """Task 3's index-level filter is the real enforcement; this is the
    feature-group-level proof it holds end to end -- the too-late neighbor
    is a closer vector match and a guaranteed breach, so if it leaked in
    the rate would read 1.0 instead of the true single resolved neighbor's 0.0.
    """
    spine = _spine(spark, [(1, 100, _T3, [0.0, 0.0])])
    index = FakeSimilarityIndex(
        [
            (Neighbor("pr:1:1", _T1, 0.0), [1.0, 1.0]),  # eligible, farther
            (Neighbor("pr:1:2", _T4, 0.0), [0.0, 0.0]),  # opened after as_of -- never returned
        ]
    )
    resolutions = _resolutions(
        spark,
        [
            (1, 1, _T2, False),  # the only neighbor that should count
            (1, 2, _T4, True),  # would flip the rate to 1.0 if it leaked in
        ],
    )

    result = compute_pr_similarity(spine, index, resolutions).collect()

    assert result[0]["similar_neighbor_count"] == 1
    assert result[0]["similar_prior_breach_rate"] == 0.0


def test_a_spine_row_with_no_embedding_gets_a_null_rate_not_dropped(
    spark: SparkSession,
) -> None:
    """Task 2's extract_texts drops PRs missing title/body, so some spine
    rows have nothing to query with -- unknown, not zero, and every spine
    row still survives (the same rule every as_of_join in this package
    already follows).
    """
    spine = _spine(spark, [(1, 100, _T3, None)])
    index = FakeSimilarityIndex([(Neighbor("pr:1:1", _T1, 0.0), [0.0, 0.0])])
    resolutions = _resolutions(spark, [(1, 1, _T2, True)])

    result = compute_pr_similarity(spine, index, resolutions).collect()

    assert len(result) == 1
    assert result[0]["similar_neighbor_count"] == 0
    assert result[0]["similar_prior_breach_rate"] is None


def test_an_issue_neighbor_never_joins_a_same_numbered_pr(spark: SparkSession) -> None:
    """entity_key's "issue:" prefix must gate the join, not just repo_id/
    number: an issue and a PR in the same repo can share a number, and
    resolutions only carries PR outcomes."""
    spine = _spine(spark, [(1, 100, _T3, [0.0, 0.0])])
    index = FakeSimilarityIndex([(Neighbor("issue:1:1", _T1, 0.0), [0.0, 0.0])])
    resolutions = _resolutions(spark, [(1, 1, _T2, True)])  # a PR #1, not the issue

    result = compute_pr_similarity(spine, index, resolutions).collect()

    assert result[0]["similar_neighbor_count"] == 1
    assert result[0]["similar_prior_breach_rate"] is None


def test_a_malformed_neighbor_key_counts_but_never_resolves(spark: SparkSession) -> None:
    """A real 2026-09-06 run found degenerate `issue:<repo_id>` keys in the
    index (Task 2's extract_texts read the wrong number path). Such a
    neighbor can't be identified as a specific entity, so it counts toward
    similar_neighbor_count but can never join resolutions -- not a crash."""
    spine = _spine(spark, [(1, 100, _T3, [0.0, 0.0])])
    index = FakeSimilarityIndex([(Neighbor("issue:1", _T1, 0.0), [0.0, 0.0])])
    resolutions = _resolutions(spark, [(1, 1, _T2, True)])

    result = compute_pr_similarity(spine, index, resolutions).collect()

    assert result[0]["similar_neighbor_count"] == 1
    assert result[0]["similar_prior_breach_rate"] is None


def test_the_spine_is_read_once_so_a_nondeterministic_sample_cannot_shift(
    spark: SparkSession,
) -> None:
    """`run_similarity` samples the spine with `orderBy(rand(seed)).limit(n)`,
    which Spark classifies as nondeterministic. Task 4's 2026-09-06 run read
    the spine twice -- neighbor pairs came from one evaluation, the final
    join's keys from another -- and only the 44 of 10,000 rows the two
    samples shared survived. Counting reads pins the fix at the point it
    broke, since a second evaluation is invisible in the output whenever the
    two happen to agree (as they do locally)."""

    class CountingSpine:
        def __init__(self, df: DataFrame) -> None:
            self._df = df
            self.reads = 0

        def select(self, *cols: Column | str) -> DataFrame:
            self.reads += 1
            return self._df.select(*cols)

        @property
        def sparkSession(self) -> SparkSession:  # noqa: N802
            return self._df.sparkSession

    spine = CountingSpine(_spine(spark, [(1, 100, _T3, [0.0, 0.0]), (1, 101, _T3, None)]))
    index = FakeSimilarityIndex([(Neighbor("pr:1:1", _T1, 0.0), [0.0, 0.0])])
    resolutions = _resolutions(spark, [(1, 1, _T2, True)])

    result = compute_pr_similarity(spine, index, resolutions).collect()  # type: ignore[arg-type]

    assert spine.reads == 1
    assert {row["pr_number"] for row in result} == {100, 101}
