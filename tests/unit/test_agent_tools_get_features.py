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
from pyspark.sql import SparkSession

from almanac.agent.schemas import EntityKey, GetFeaturesInput
from almanac.agent.tools import get_features
from almanac.features.groups import compute_author_activity
from tests.agent_fixtures import (
    SilverEventRow,
    land_silver_and_features,
    pr_closed,
    pr_opened,
    silver_event,
)
from tests.agent_fixtures import silver_frame as _silver_frame

pytestmark = pytest.mark.spark


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
_EVENTS: list[SilverEventRow] = [
    pr_opened(1, 3, datetime(2025, 8, 10, 9, tzinfo=UTC), "alice"),
    pr_closed(1, 3, datetime(2025, 8, 12, 10, tzinfo=UTC), "alice", merged=True),
    pr_opened(1, 4, datetime(2025, 8, 11, 9, tzinfo=UTC), "alice"),
    pr_closed(1, 4, datetime(2025, 8, 14, 10, tzinfo=UTC), "alice", merged=False),
    pr_opened(1, 5, datetime(2025, 8, 13, 8, tzinfo=UTC), "alice"),
    pr_opened(1, 6, datetime(2025, 8, 14, 12, tzinfo=UTC), "alice"),
    silver_event(
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
    events = _silver_frame(spark, _EVENTS)
    silver_path, features_path = land_silver_and_features(spark, events, tmp_path)

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
    events = _silver_frame(spark, _EVENTS)
    silver_path, features_path = land_silver_and_features(spark, events, tmp_path)

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
    events = _silver_frame(spark, _EVENTS)
    silver_path, features_path = land_silver_and_features(spark, events, tmp_path)
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
    events = _silver_frame(spark, _EVENTS)
    silver_path, features_path = land_silver_and_features(spark, events, tmp_path)
    compute_author_activity(events).write.format("delta").mode("overwrite").save(
        f"{features_path}/author_activity"
    )

    result = get_features(
        spark,
        GetFeaturesInput(entity=_ENTITY, as_of=_AS_OF_EARLY),
        silver_path=silver_path,
        features_path=features_path,
    )

    assert result.provenance.read_delta_versions == {
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
    events = _silver_frame(spark, _EVENTS)
    silver_path, features_path = land_silver_and_features(spark, events, tmp_path)
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

    assert result.provenance.read_delta_versions == {
        "events": 0,
        "author_activity": 0,
        "repo_activity": 0,
        "pr_static": 0,
    }


def test_an_entity_with_no_opened_event_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    events = _silver_frame(spark, _EVENTS)
    silver_path, features_path = land_silver_and_features(spark, events, tmp_path)

    with pytest.raises(ValueError, match="repo_id=999"):
        get_features(
            spark,
            GetFeaturesInput(entity=EntityKey(repo_id=999, pr_number=1), as_of=_AS_OF_EARLY),
            silver_path=silver_path,
            features_path=features_path,
        )
