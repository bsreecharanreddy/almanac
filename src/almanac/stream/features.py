"""Online feature groups from the live streaming Silver table.

Scope is fixed by §4.6's first finding: reduced-era events carry no merge
outcome, no PR author history, no PR text, so §5.1's label and every
offline text feature are uncomputable here. What the reduced payload does
support is entity *activity* -- how busy a repo or an actor has been, and
how recently -- so that is what this computes, per repo and per actor.

Point-in-time discipline is not relaxed because the compute model changed
(CLAUDE.md's governing principle). Every aggregate on an event's row
counts only that entity's *strictly prior* events: `rowsBetween(..., -1)`
and `rangeBetween(..., -1)`, never `currentRow`. Offline groups
(`features.groups`) are inclusive-of-self and lean on `as_of_join`'s
strict `<` for the boundary; the online store serves the latest row per
entity with no join in the path, so the strict `<` has to live in the
feature itself.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

_HOUR_SECONDS = 3600
_DAY_SECONDS = 86_400


def _prior_rows(entity: str) -> WindowSpec:
    """Every strictly-earlier event for this entity, ties broken by `event_id`
    so the frame is deterministic when two events share a `created_at`."""
    return (
        Window.partitionBy(entity)
        .orderBy("_ts", "event_id")
        .rowsBetween(Window.unboundedPreceding, -1)
    )


def _prior_range(entity: str, seconds: int) -> WindowSpec:
    """Strictly-earlier events within `seconds` -- `-1` upper bound excludes
    both the current event and any other at the same second."""
    return Window.partitionBy(entity).orderBy("_ts").rangeBetween(-seconds, -1)


def _stream_features(events: DataFrame, entity: str) -> DataFrame:
    """One row per event carrying `entity`: that entity's activity as known
    strictly before this event. `entity` unseen before now -> every feature
    is null, not zero: no history is unknown, not "quiet" (the same
    unknown-vs-false discipline `compute_author_activity` applies to an
    unclosed prior PR).
    """
    scoped = events.where(F.col(entity).isNotNull()).select(
        F.col(entity),
        "event_id",
        F.col("created_at").alias("event_time"),
        F.unix_timestamp("created_at").alias("_ts"),
    )

    prior_events = F.count(F.lit(1)).over(_prior_rows(entity))
    events_1h = F.count(F.lit(1)).over(_prior_range(entity, _HOUR_SECONDS))
    events_24h = F.count(F.lit(1)).over(_prior_range(entity, _DAY_SECONDS))
    prev_ts = F.lag("_ts").over(Window.partitionBy(entity).orderBy("_ts", "event_id"))

    def when_seen(value: Column) -> Column:
        return F.when(prior_events > 0, value)

    return scoped.select(
        entity,
        "event_time",
        when_seen(events_1h).alias("events_prior_1h"),
        when_seen(events_24h).alias("events_prior_24h"),
        when_seen((F.col("_ts") - prev_ts).cast("double")).alias("secs_since_last_event"),
        when_seen(events_24h / F.lit(float(_DAY_SECONDS // _HOUR_SECONDS))).alias(
            "arrival_per_hour_24h"
        ),
    ).dropDuplicates([entity, "event_time"])


def compute_repo_stream_features(events: DataFrame) -> DataFrame:
    """Per-repo activity, keyed `(repo_id, event_time)` for the online store."""
    return _stream_features(events, "repo_id")


def compute_actor_stream_features(events: DataFrame) -> DataFrame:
    """Per-actor activity, keyed `(actor_login, event_time)`."""
    return _stream_features(events, "actor_login")
