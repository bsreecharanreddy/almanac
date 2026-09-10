"""The Silver and feature tables the agent tools read, built as real Delta.

Kept out of `tests/helpers.py` because this is a scenario rather than a
generic builder, and out of each tool's own test module because three of
them now land the same table -- the third copy is the duplication CLAUDE.md
item 4 names, and the one that would go stale silently.
"""

from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from almanac.agent.schemas import EntityKey
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)

SILVER_EVENT_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)

_INGESTED = datetime(2025, 8, 20, tzinfo=UTC)

type SilverEventRow = tuple[
    int, int | None, datetime, str, str | None, str, bool | None, bool | None, None, datetime
]


def silver_event(
    repo_id: int,
    pr_number: int | None,
    created_at: datetime,
    event_type: str,
    *,
    event_action: str | None,
    actor_login: str,
    pr_merged: bool | None = None,
    pr_draft: bool | None = None,
) -> SilverEventRow:
    """One row of `events/clean`, positional to match `SILVER_EVENT_SCHEMA`."""
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


def pr_opened(
    repo_id: int, pr_number: int, created_at: datetime, actor: str, *, draft: bool | None = False
) -> SilverEventRow:
    """`draft=None` is the reduced era: the column exists, this row has no value."""
    return silver_event(
        repo_id,
        pr_number,
        created_at,
        "PullRequestEvent",
        event_action="opened",
        actor_login=actor,
        pr_draft=draft,
    )


def pr_closed(
    repo_id: int, pr_number: int, created_at: datetime, actor: str, *, merged: bool
) -> SilverEventRow:
    return silver_event(
        repo_id,
        pr_number,
        created_at,
        "PullRequestEvent",
        event_action="closed",
        actor_login=actor,
        pr_merged=merged,
    )


def silver_frame(spark: SparkSession, rows: list[SilverEventRow]) -> DataFrame:
    return spark.createDataFrame(rows, SILVER_EVENT_SCHEMA)


def land_silver_and_features(
    spark: SparkSession, events: DataFrame, tmp_path: Path
) -> tuple[str, str]:
    """Real Delta tables under `tmp_path`, exactly what the tools read -- not
    in-memory frames, so each tool's own I/O is what is under test.
    """
    silver_path = str(tmp_path / "events")
    features_path = str(tmp_path / "features")

    events.write.format("delta").save(f"{silver_path}/clean")
    compute_author_activity(events).write.format("delta").save(f"{features_path}/author_activity")
    compute_repo_activity(events).write.format("delta").save(f"{features_path}/repo_activity")
    compute_pr_static(events).write.format("delta").save(f"{features_path}/pr_static")
    return silver_path, features_path


# One Silver table holding both eras, so a scoring tool's two paths differ in
# nothing but the era.
# PR 8: carol's first PR, opened then closed *merged* -- gives PR 10 and PR 11
# a settled prior_pr_count=1, prior_merge_rate=1.0, so the only difference
# between the two query entities is is_draft.
# PR 10: rich era, pr_draft recorded False.
# PR 11: reduced era -- pr_draft omitted entirely, the exact shape of the
# 2026-09-05 window (design doc S4.2).
TWO_ERA_EVENTS: list[SilverEventRow] = [
    pr_opened(1, 8, datetime(2025, 8, 10, 9, tzinfo=UTC), "carol"),
    pr_closed(1, 8, datetime(2025, 8, 11, 9, tzinfo=UTC), "carol", merged=True),
    pr_opened(1, 10, datetime(2025, 8, 12, 9, tzinfo=UTC), "carol"),
    pr_opened(1, 11, datetime(2025, 8, 13, 9, tzinfo=UTC), "carol", draft=None),
]

RICH_ENTITY = EntityKey(repo_id=1, pr_number=10)
RICH_AS_OF = datetime(2025, 8, 12, 9, 5, tzinfo=UTC)
REDUCED_ENTITY = EntityKey(repo_id=1, pr_number=11)
REDUCED_AS_OF = datetime(2025, 8, 13, 9, 5, tzinfo=UTC)
