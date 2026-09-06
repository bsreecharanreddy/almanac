"""compute_repo_stream_features / compute_actor_stream_features: entity
activity as known strictly before each event, for the online store. The
strict `<` lives in the feature here, not in a downstream join (§features.py).
"""

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from almanac.stream.features import (
    compute_actor_stream_features,
    compute_repo_stream_features,
)
from tests.helpers import one

pytestmark = pytest.mark.spark

_SCHEMA = "event_id string, repo_id long, actor_login string, created_at timestamp"
_T0 = datetime(2025, 11, 3, 14, 0, 0, tzinfo=UTC)

_Row = tuple[str, int, str, datetime]


def _event(event_id: str, *, repo_id: int, actor: str, at: datetime) -> _Row:
    return (event_id, repo_id, actor, at)


def _by_epoch(features: DataFrame) -> dict[int, Row]:
    """Keyed on the event time as an epoch second -- never on a collected
    datetime, which PySpark returns naive in the driver's timezone (the
    trap `tests.helpers.epoch_of` exists for)."""
    rows = features.withColumn("_epoch", F.unix_timestamp("event_time")).collect()
    return {row["_epoch"]: row for row in rows}


def _epoch(at: datetime) -> int:
    return int(at.timestamp())


def test_rolling_counts_exclude_events_at_or_after_the_events_own_time(spark: SparkSession) -> None:
    """The feature on the row at T counts prior events only -- never T's own
    event, never a later one. Strict `<`, matching as_of_join."""
    events = spark.createDataFrame(
        [
            _event("a", repo_id=1, actor="alice", at=_T0),
            _event("b", repo_id=1, actor="bob", at=_T0 + timedelta(minutes=10)),
            _event("c", repo_id=1, actor="cara", at=_T0 + timedelta(minutes=20)),
            _event("d", repo_id=1, actor="dan", at=_T0 + timedelta(minutes=30)),
        ],
        _SCHEMA,
    )

    by_time = _by_epoch(compute_repo_stream_features(events))

    # Row c (T0+20m): events a and b are strictly before it, d is after.
    assert by_time[_epoch(_T0 + timedelta(minutes=20))]["events_prior_1h"] == 2
    # Row d (T0+30m): a, b, c are before it.
    assert by_time[_epoch(_T0 + timedelta(minutes=30))]["events_prior_1h"] == 3


def test_a_same_second_event_is_not_counted_as_prior(spark: SparkSession) -> None:
    """Two events for one repo at the identical instant: neither sees the
    other -- `< T`, not `<= T`."""
    events = spark.createDataFrame(
        [
            _event("x", repo_id=7, actor="alice", at=_T0),
            _event("y", repo_id=7, actor="bob", at=_T0),
        ],
        _SCHEMA,
    )

    rows = compute_repo_stream_features(events).collect()
    assert all(r["events_prior_1h"] is None for r in rows), "each is the other's cold start"


def test_cold_start_entity_yields_null_not_zero(spark: SparkSession) -> None:
    """A repo's first-ever event has no prior activity -- unknown, not a
    genuine zero (the discipline compute_author_activity applies to an
    unclosed prior PR)."""
    events = spark.createDataFrame([_event("a", repo_id=1, actor="alice", at=_T0)], _SCHEMA)

    row = one(compute_repo_stream_features(events))
    assert row["events_prior_1h"] is None
    assert row["events_prior_24h"] is None
    assert row["secs_since_last_event"] is None
    assert row["arrival_per_hour_24h"] is None


def test_zero_in_window_is_kept_once_the_entity_has_been_seen(spark: SparkSession) -> None:
    """A prior event two hours ago: the repo is known, so 0-events-in-the-
    last-hour is a real signal, not unknown."""
    events = spark.createDataFrame(
        [
            _event("a", repo_id=1, actor="alice", at=_T0),
            _event("b", repo_id=1, actor="bob", at=_T0 + timedelta(hours=2)),
        ],
        _SCHEMA,
    )

    later = _by_epoch(compute_repo_stream_features(events))[_epoch(_T0 + timedelta(hours=2))]
    assert later["events_prior_1h"] == 0
    assert later["events_prior_24h"] == 1
    assert later["secs_since_last_event"] == pytest.approx(7200.0)


def test_actor_features_partition_on_actor_not_repo(spark: SparkSession) -> None:
    """Same actor across two repos accumulates one timeline; the repo split
    is irrelevant to an actor-keyed feature."""
    events = spark.createDataFrame(
        [
            _event("a", repo_id=1, actor="alice", at=_T0),
            _event("b", repo_id=2, actor="alice", at=_T0 + timedelta(minutes=5)),
            _event("c", repo_id=3, actor="alice", at=_T0 + timedelta(minutes=10)),
        ],
        _SCHEMA,
    )

    third = _by_epoch(compute_actor_stream_features(events))[_epoch(_T0 + timedelta(minutes=10))]
    assert third["events_prior_1h"] == 2
