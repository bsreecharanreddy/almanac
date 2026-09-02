"""Normalize the three schema eras into one shape. Pure; no I/O.

Every branch here exists because Phase 0 measured a difference, not because
a difference was expected. See docs/findings/2026-09-01-schema-eras.md and
docs/findings/2026-09-01-third-schema-era.md.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from almanac.explore.schema import (
    ERA_BOUNDARY_MODERN,
    ERA_BOUNDARY_REDUCED,
    SchemaEra,
)

# The canonical Silver shape. Declared here rather than left implicit so
# that the raw input columns cannot leak downstream -- carrying `id`
# alongside `event_id`, null on every legacy row, is exactly the shape a
# later dedup is most likely to key on by mistake.
#
# `ingested_at` is carried through rather than dropped: it is the
# provenance a determination has to be reproducible against, and it is the
# tiebreak `deduplicate` orders on when two copies share an event time.
# Omitting it made the declared Silver order -- normalize, then dedup --
# impossible to wire, which nothing noticed until Task 6 tried.
SILVER_COLUMNS = (
    "event_id",
    "event_id_source",
    "actor_login",
    "created_at",
    "repo_id",
    "repo_name",
    "event_type",
    "schema_era",
    "ingested_at",
)

# Written into the hash input wherever a field is null, so that a null
# occupies its position instead of vanishing. `concat_ws` skips nulls, so
# without this ("a", null, "c") and ("a", "c", null) hash identically --
# two genuinely different events collapsing onto one id, and for legacy
# events there is no native id to fall back on.
_NULL_SENTINEL = "\\N"


def _content_hash() -> Column:
    """A deterministic surrogate id for events that carry none.

    ``created_at`` contributes its **epoch second**, not its rendered form.
    Casting a timestamp straight to string formats it in the session
    timezone, which would make this key -- persisted, and the only id a
    legacy event will ever have -- a function of a cluster setting.
    Measured 2026-09-01: one event under ``UTC``, ``America/New_York`` and
    ``Asia/Kolkata`` hashed three different ways. A backfill and a later
    incremental run under different session timezones would re-ingest all
    of legacy history as new rows, and dedup would not notice.

    ``event_url`` is in the key because the four fields around it are not
    enough, which was measured rather than assumed: run against a real
    legacy hour, ``(created_at, actor_login, repo_id, event_type)`` gave two
    colliding pairs in 2,000 events, and both pairs were **genuinely
    different events** -- one actor pushing two distinct commit ranges in
    the same second, another opening two distinct issues in the same second.
    Dedup was deleting one of each, silently, at roughly 1 in 1,000 legacy
    events. Phase 0's measured duplicate ratio of 1 in 6.0M could not have
    caught this: it was taken on modern data, which carries native ids and
    never reaches this hash. Adding the url takes the same hour to zero
    collisions. It is stable content -- a canonical URL naming the specific
    commit range or issue -- not a serialization artifact, which is the line
    that keeps the rest of the payload out of the key.
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
    """Map raw event columns onto the canonical Silver shape.

    Three era differences are handled, each measured in Phase 0:

    1. Legacy ``created_at`` carries a ``-07:00`` offset. ``to_timestamp``
       on an offset-aware string converts correctly; a naive parse would
       shift every pre-2015 event by seven hours, silently, past every
       schema check that exists.
    2. Legacy events carry no ``id`` at all (0 of 2,000 sampled), so
       ``event_id`` falls back to a deterministic content hash and
       ``event_id_source`` records which mechanism produced it.
    3. ``actor`` is a bare string in legacy and an object in modern -- a
       type change. The caller flattens it to ``actor_raw`` before this
       function sees it, keeping the type-dispatch at the read boundary.
    """
    created = F.to_timestamp("created_at_raw")

    # Era labels come from `SchemaEra` rather than string literals: this is
    # the second implementation of a rule `era_for` already owns in Python,
    # and a boundary moved in one and not the other mislabels history
    # instead of failing.
    era = (
        F.when(created >= F.lit(ERA_BOUNDARY_REDUCED), F.lit(SchemaEra.REDUCED_V3.value))
        .when(created >= F.lit(ERA_BOUNDARY_MODERN), F.lit(SchemaEra.MODERN_V2.value))
        .otherwise(F.lit(SchemaEra.LEGACY_V1.value))
    )

    out = (
        df.withColumn("created_at", created)
        .withColumn("schema_era", era)
        .withColumn("actor_login", F.col("actor_raw"))
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
