"""get_features: the platform's own as-of join, exercised through the tool.

Leakage axis covered: **row time** only -- the earlier `as_of` call must
contain nothing from an event whose own `created_at` is at or after it.
This is not a test of split time, entity overlap, label construction, or
target definition (`.claude/skills/almanac-leakage-review` gate 1); those
axes do not apply here, since a tool call has no split and no label.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from almanac.agent.schemas import EntityKey, GetFeaturesInput
from almanac.agent.tools import get_features
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)

pytestmark = pytest.mark.spark

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)

_INGESTED = datetime(2025, 8, 20, tzinfo=UTC)

type _Row = tuple[
    int, int | None, datetime, str, str | None, str, bool | None, bool | None, None, datetime
]


def _event(
    repo_id: int,
    pr_number: int | None,
    created_at: datetime,
    event_type: str,
    *,
    event_action: str | None,
    actor_login: str,
    pr_merged: bool | None = None,
    pr_draft: bool | None = None,
) -> _Row:
    return (
        repo_id,
        pr_number,
        created_at,
        event_type,
        event_action,
        actor_login,
        pr_merged,
        pr_draft,
        None,
        _INGESTED,
    )


def _pr_opened(repo_id: int, pr_number: int, created_at: datetime, actor: str) -> _Row:
    return _event(
        repo_id,
        pr_number,
        created_at,
        "PullRequestEvent",
        event_action="opened",
        actor_login=actor,
        pr_draft=False,
    )


def _pr_closed(
    repo_id: int, pr_number: int, created_at: datetime, actor: str, *, merged: bool
) -> _Row:
    return _event(
        repo_id,
        pr_number,
        created_at,
        "PullRequestEvent",
        event_action="closed",
        actor_login=actor,
        pr_merged=merged,
    )


def _land(spark: SparkSession, events: DataFrame, tmp_path: Path) -> tuple[str, str]:
    """Real Delta tables under `tmp_path`, exactly what `get_features` reads --
    not in-memory frames, so the tool's own I/O is what is under test.
    """
    silver_path = str(tmp_path / "events")
    features_path = str(tmp_path / "features")

    events.write.format("delta").save(f"{silver_path}/clean")
    compute_author_activity(events).write.format("delta").save(f"{features_path}/author_activity")
    compute_repo_activity(events).write.format("delta").save(f"{features_path}/repo_activity")
    compute_pr_static(events).write.format("delta").save(f"{features_path}/pr_static")
    return silver_path, features_path


# PR 3: opened, then closed *merged*, before AS_OF_EARLY -- alice's one
# settled prior PR at the early checkpoint.
# PR 4: opened before AS_OF_EARLY but closed *unmerged* only after it --
# invisible at AS_OF_EARLY, visible at AS_OF_LATE. This is the row-time
# leakage probe: its own created_at (2025-08-14T10:00) sits strictly
# between the two as_of values.
# PR 5: the query entity, opened before both checkpoints so both queries
# are "explain this PR while it is still open", not "before it existed".
# PR 6: alice's *next* PR after PR 5, opened after PR 4 resolves. Author
# activity is recorded per author-opened-event, not continuously
# (`features/groups.py:compute_author_activity`), so PR 4 resolving is
# only visible to `prior_pr_count`/`prior_merge_rate` once carried forward
# from a later row of alice's own -- PR 6's -- not from PR 4's close event
# directly.
# One extra WatchEvent on the repo lands in the same window, so
# events_total_to_date also moves between the two calls.
_EVENTS = [
    _pr_opened(1, 3, datetime(2025, 8, 10, 9, tzinfo=UTC), "alice"),
    _pr_closed(1, 3, datetime(2025, 8, 12, 10, tzinfo=UTC), "alice", merged=True),
    _pr_opened(1, 4, datetime(2025, 8, 11, 9, tzinfo=UTC), "alice"),
    _pr_closed(1, 4, datetime(2025, 8, 14, 10, tzinfo=UTC), "alice", merged=False),
    _pr_opened(1, 5, datetime(2025, 8, 13, 8, tzinfo=UTC), "alice"),
    _pr_opened(1, 6, datetime(2025, 8, 14, 12, tzinfo=UTC), "alice"),
    _event(
        1,
        None,
        datetime(2025, 8, 14, tzinfo=UTC),
        "WatchEvent",
        event_action=None,
        actor_login="bob",
    ),
]

_ENTITY = EntityKey(repo_id=1, pr_number=5)
_AS_OF_EARLY = datetime(2025, 8, 13, 12, tzinfo=UTC)
_AS_OF_LATE = datetime(2025, 8, 15, tzinfo=UTC)


def test_two_as_of_values_on_the_same_entity_return_different_vectors(
    spark: SparkSession, tmp_path: Path
) -> None:
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path, features_path = _land(spark, events, tmp_path)

    early = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_EARLY),
        silver_path=silver_path,
        features_path=features_path,
    )
    late = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_LATE),
        silver_path=silver_path,
        features_path=features_path,
    )

    assert early.features != late.features


def test_the_earlier_vector_contains_nothing_after_its_own_as_of(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Row time only (module docstring). PR 4's close event -- the only
    event whose created_at sits strictly between the two checkpoints --
    must be invisible at AS_OF_EARLY and visible at AS_OF_LATE.
    """
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path, features_path = _land(spark, events, tmp_path)

    early = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_EARLY),
        silver_path=silver_path,
        features_path=features_path,
    )
    late = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_LATE),
        silver_path=silver_path,
        features_path=features_path,
    )

    # Only PR 3 (merged) is settled at the early checkpoint.
    assert early.features["prior_pr_count"] == 1.0
    assert early.features["prior_merge_rate"] == 1.0
    # PR 4 (unmerged) settles between the two checkpoints, carried forward
    # once PR 6 records it.
    assert late.features["prior_pr_count"] == 2.0
    assert late.features["prior_merge_rate"] == 0.5
    # events_total_to_date: 4 events precede AS_OF_EARLY, all 7 precede AS_OF_LATE.
    assert early.features["events_total_to_date"] == 4.0
    assert late.features["events_total_to_date"] == 7.0


def test_static_features_are_unaffected_by_as_of(spark: SparkSession, tmp_path: Path) -> None:
    """`is_draft`, `is_bot_author`, `opened_day_of_week`, `opened_hour` are
    fixed at PR-open time and joined directly, never through `as_of_join`
    (`features/assemble.py`) -- they must be identical at both checkpoints.
    """
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path, features_path = _land(spark, events, tmp_path)
    static_columns = ("is_draft", "is_bot_author", "opened_day_of_week", "opened_hour")

    early = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_EARLY),
        silver_path=silver_path,
        features_path=features_path,
    )
    late = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_LATE),
        silver_path=silver_path,
        features_path=features_path,
    )

    for column in static_columns:
        assert early.features[column] == late.features[column]


def test_provenance_names_the_delta_version_actually_read(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Unpinned reports the table's *current* commit -- not a constant.

    `author_activity` is overwritten a second time with identical content
    after landing, bumping it to version 1 while the other three tables
    stay at 0. A provenance report that returned a fixed number, or that
    never actually looked the version up, could not tell these apart.
    """
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path, features_path = _land(spark, events, tmp_path)
    compute_author_activity(events).write.format("delta").mode("overwrite").save(
        f"{features_path}/author_activity"
    )

    result = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_EARLY),
        silver_path=silver_path,
        features_path=features_path,
    )

    assert result.provenance.delta_versions == {
        "events": 0,
        "author_activity": 1,
        "repo_activity": 0,
        "pr_static": 0,
    }


def test_a_pinned_version_is_reported_as_is_not_re_resolved(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The inverse of the test above: `author_activity` is bumped to
    version 1 again, but this call pins it to 0. The pin must be reported
    as given, not re-resolved to the table's now-current version.
    """
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path, features_path = _land(spark, events, tmp_path)
    compute_author_activity(events).write.format("delta").mode("overwrite").save(
        f"{features_path}/author_activity"
    )

    result = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_EARLY),
        silver_path=silver_path,
        features_path=features_path,
        silver_version=0,
        features_versions={"author_activity": 0, "repo_activity": 0, "pr_static": 0},
    )

    assert result.provenance.delta_versions == {
        "events": 0,
        "author_activity": 0,
        "repo_activity": 0,
        "pr_static": 0,
    }


def test_an_entity_with_no_opened_event_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    events = spark.createDataFrame(_EVENTS, _SCHEMA)
    silver_path, features_path = _land(spark, events, tmp_path)

    with pytest.raises(ValueError, match="repo_id=999"):
        get_features(
            spark,
            GetFeaturesInput(entity=EntityKey(repo_id=999, pr_number=1), as_of=_AS_OF_EARLY),
            silver_path=silver_path,
            features_path=features_path,
        )
