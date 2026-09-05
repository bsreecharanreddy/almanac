"""The governing invariant, tested directly (CLAUDE.md): a feature vector
computed as-of T must be reproducible byte-for-byte from the same Delta
version, a year later -- even after more data, including a late-arriving
correction whose own event_time is before T, has since been appended.

This is not the same property as as_of_join's boundary test
(tests/unit/test_features_join.py): that test proves a single query never
lets a future row leak in. This test proves that recomputing a *specific,
already-built* training set later reproduces it exactly only if the Delta
version is pinned -- a live "filter on event_time" re-query is leak-free
but not reproducible, because more history can arrive between builds.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession

from almanac.features.groups import compute_repo_activity
from almanac.features.join import as_of_join
from almanac.features.similarity import Neighbor, compute_pr_similarity

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


_Event = tuple[int, None, datetime, str, None, str, None, None, None, datetime]


def _event(repo_id: int, created_at: datetime, ingested_at: datetime, actor: str) -> _Event:
    return (repo_id, None, created_at, "WatchEvent", None, actor, None, None, None, ingested_at)


def _latest_version(spark: SparkSession, path: Path) -> int:
    row = DeltaTable.forPath(spark, str(path)).history(1).select("version").collect()[0]
    return int(row["version"])


def test_pinning_the_delta_version_reproduces_the_original_result_after_a_late_arrival(
    spark: SparkSession, tmp_path: Path
) -> None:
    events_path = tmp_path / "events"
    t = datetime(2025, 8, 13, 12, tzinfo=UTC)

    spark.createDataFrame(
        [
            _event(
                1, datetime(2025, 8, 10, tzinfo=UTC), datetime(2025, 8, 10, 1, tzinfo=UTC), "alice"
            )
        ],
        _SCHEMA,
    ).write.format("delta").save(str(events_path))
    v1 = _latest_version(spark, events_path)

    spine = spark.createDataFrame([(1, t)], "repo_id long, as_of_timestamp timestamp")

    def as_of_result(version: int) -> list[Row]:
        events = spark.read.format("delta").option("versionAsOf", version).load(str(events_path))
        return as_of_join(spine, compute_repo_activity(events), on=["repo_id"]).collect()

    original = as_of_result(v1)
    assert original[0]["events_total_to_date"] == 1

    # A late-arriving correction: event_time is BEFORE t, but it is
    # ingested and appended as a real second Delta version well after the
    # original build -- not backdated in place.
    spark.createDataFrame(
        [_event(1, datetime(2025, 8, 11, tzinfo=UTC), datetime(2025, 8, 20, tzinfo=UTC), "carol")],
        _SCHEMA,
    ).write.format("delta").mode("append").save(str(events_path))
    v2 = _latest_version(spark, events_path)

    pinned = as_of_result(v1)
    live = as_of_result(v2)

    # Reproducible: the pinned version is untouched by the append.
    assert pinned == original
    # Leak-free (event_time < t) but changes the "live" answer -- exactly
    # why the version gets pinned at build time, not re-derived from
    # "current".
    assert live[0]["events_total_to_date"] == 2
    assert live != original


class _StaticIndex:
    """query_similar's own filtering already excludes a too-late neighbor
    (tests/unit/test_features_similarity.py) -- this fixture only needs to
    return one fixed, already-eligible neighbor, since what this test
    exercises is compute_pr_similarity's *second* input, resolutions.
    """

    def query(self, vector: list[float], *, as_of: object, k: int) -> list[Neighbor]:
        return [Neighbor("pr:1:1", datetime(2025, 8, 1, tzinfo=UTC), 0.0)]


def test_a_neighbors_late_arriving_resolution_is_leak_free_but_not_reproducible(
    spark: SparkSession,
) -> None:
    """Retrieval's own leakage axis (§8.3a), same shape as the Delta-version
    test above: `resolutions` is an ordinary DataFrame a caller reads from
    Gold at some version, not something compute_pr_similarity re-derives --
    a neighbor's resolution landing in a later Gold version, with a
    closed_at that was always < as_of, is leak-free to include (event time,
    not arrival time, is what governs), but it means the answer depends on
    which `resolutions` snapshot was passed in, exactly like a live re-read
    of Silver would for `events_total_to_date` above.
    """
    as_of = datetime(2025, 8, 13, tzinfo=UTC)
    spine = spark.createDataFrame(
        [(9, 1, as_of, [0.0, 0.0])],
        "repo_id long, pr_number long, as_of_timestamp timestamp, embedding array<double>",
    )
    index = _StaticIndex()
    resolutions_schema = "repo_id long, pr_number long, closed_at timestamp, breach boolean"

    before = spark.createDataFrame([], resolutions_schema)
    after_late_arrival = spark.createDataFrame(
        [(1, 1, datetime(2025, 8, 10, tzinfo=UTC), True)],  # closed_at < as_of, always was
        resolutions_schema,
    )

    pinned = compute_pr_similarity(spine, index, before).collect()
    live = compute_pr_similarity(spine, index, after_late_arrival).collect()

    assert pinned[0]["similar_prior_breach_rate"] is None
    assert live[0]["similar_prior_breach_rate"] == 1.0
