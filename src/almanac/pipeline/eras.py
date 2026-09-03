"""Normalize the three schema eras into one shape. Pure; no I/O.

Every era branch here traces to a Phase 0 measurement, not an expectation --
docs/findings/2026-09-01-schema-eras.md and -third-schema-era.md.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from almanac.explore.schema import (
    ERA_BOUNDARY_MODERN,
    ERA_BOUNDARY_REDUCED,
    SchemaEra,
)

# The canonical Silver shape, declared so raw input columns cannot leak
# downstream. `ingested_at` is carried, not dropped: it is the provenance a
# determination is reproduced against and the tiebreak `deduplicate` orders on.
SILVER_COLUMNS = (
    "event_id",
    "event_id_source",
    "actor_login",
    "created_at",
    "repo_id",
    "repo_name",
    "event_type",
    "event_action",
    "schema_era",
    "ingested_at",
    # event_date/event_hour describe the archive file, not the event time --
    # created_at is the event-time column. A legacy file named hour 14 spans
    # two UTC hours (findings: 2026-09-01-schema-eras.md), so partitioning by
    # event time would break idempotent replaceWhere.
    "event_date",
    "event_hour",
    # Gold: (repo_id, pr_number) is fact_pull_request's natural key in all
    # three eras -- `number` survived the Oct 2025 payload reduction.
    "pr_number",
    "pr_merged",
    "pr_draft",
    "is_pr_comment",
    # agg_repo_daily sums push volume from these, never size(commits) (§12
    # trap 3 caps it at 20). push_distinct_size is null pre-2015.
    "push_size",
    "push_distinct_size",
)

# concat_ws skips nulls, so ("a", null, "c") and ("a", "c", null) would hash
# alike; a sentinel holds each null's position. Legacy events have no native id.
_NULL_SENTINEL = "\\N"


def _content_hash() -> Column:
    """Deterministic surrogate id for events (legacy) that carry none.

    ``created_at`` contributes its epoch second, not its rendered form: a
    timestamp cast to string formats in the session timezone, which would
    make this persisted key a function of a cluster setting. ``event_url``
    is included because the four other fields collided on genuinely
    different events in a real legacy hour (2 pairs in 2,000); it is stable
    content, not a serialization artifact, which is what keeps the rest of
    the payload out of the key.
    """
    parts = (
        F.col("created_at").cast("long"),
        F.col("actor_login"),
        F.col("repo_id"),
        F.col("event_type"),
        F.col("event_url"),
    )
    joined = F.concat_ws("|", *[F.coalesce(p.cast("string"), F.lit(_NULL_SENTINEL)) for p in parts])
    return F.sha2(joined, 256)


def normalize_events(df: DataFrame) -> DataFrame:
    """Map raw event columns onto the canonical Silver shape, per era."""
    created = F.to_timestamp("created_at_raw")

    # From SchemaEra, not string literals: a boundary moved here and not in
    # `era_for` would mislabel history rather than fail.
    era = (
        F.when(created >= F.lit(ERA_BOUNDARY_REDUCED), F.lit(SchemaEra.REDUCED_V3.value))
        .when(created >= F.lit(ERA_BOUNDARY_MODERN), F.lit(SchemaEra.MODERN_V2.value))
        .otherwise(F.lit(SchemaEra.LEGACY_V1.value))
    )

    # `issue.pull_request` does not exist pre-2015 (0 of 194 legacy comments).
    # Null means unknown; false would be a fabricated negative and §5.1 would
    # lose every legacy comment. Gold resolves these by joining (repo_id, number).
    is_pr_comment = F.when(
        (F.col("schema_era") == SchemaEra.LEGACY_V1.value)
        | (F.col("event_type") != "IssueCommentEvent"),
        F.lit(None).cast("boolean"),
    ).otherwise(F.col("issue_is_pr"))

    out = (
        df.withColumn("created_at", created)
        .withColumn("schema_era", era)
        .withColumn("actor_login", F.col("actor_raw"))
        .withColumn("is_pr_comment", is_pr_comment)
    )

    has_native = F.col("id").isNotNull() & (F.length(F.col("id")) > 0)

    return (
        out.withColumn("event_id", F.when(has_native, F.col("id")).otherwise(_content_hash()))
        .withColumn(
            "event_id_source",
            F.when(has_native, F.lit("native")).otherwise(F.lit("content_hash")),
        )
        .select(*SILVER_COLUMNS)
    )
