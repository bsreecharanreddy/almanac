"""Point-in-time-correct nearest-neighbor retrieval (design doc §8.3a) --
a second leakage axis on top of `join.as_of_join`'s: a neighbor's own
outcome is only knowable once the neighbor itself resolved, strictly
before *this* spine row's own as_of_timestamp, not merely because Gold has
since resolved it by the time this pipeline runs.

Deliberately not a FeatureTableSpec / run_features entry (features/runner.py):
this queries an external ANN index per spine row rather than transforming
one DataFrame into another, so it gets its own small runner (Task 7)
instead of being forced into that shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import NamedTuple, Protocol

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StructField, StructType, TimestampType

# Explicit, not inferred: an issue neighbor's neighbor_repo_id/pr_number are
# null by construction (only a "pr" neighbor can resolve), and a single-row
# batch where those columns are all-null defeats createDataFrame's own
# schema inference (CANNOT_DETERMINE_TYPE).
_NEIGHBOR_PAIR_SCHEMA = StructType(
    [
        StructField("repo_id", LongType()),
        StructField("pr_number", LongType()),
        StructField("as_of_timestamp", TimestampType()),
        StructField("neighbor_repo_id", LongType()),
        StructField("neighbor_pr_number", LongType()),
    ]
)

_SPINE_KEY_SCHEMA = StructType(
    [StructField("repo_id", LongType()), StructField("pr_number", LongType())]
)


class Neighbor(NamedTuple):
    entity_key: str
    event_time: datetime
    score: float


class SimilarityIndex(Protocol):
    """What compute_pr_similarity needs -- the real Databricks-backed index
    (embed/query.py, Task 5) and a small brute-force test double both
    satisfy this with a handful of lines, the same shape as embed.pipeline's
    TextEncoder.
    """

    def query(self, vector: list[float], *, as_of: datetime, k: int) -> list[Neighbor]:
        """`as_of` and `event_time` are UTC-aware -- `_neighbor_rows` normalizes first."""
        ...


def query_similar(
    index: SimilarityIndex, vector: list[float], as_of: datetime, *, k: int = 10
) -> list[Neighbor]:
    """The as_of filter is passed to the index's own query, not applied
    after -- the same "excluded, not filtered post-hoc" discipline
    `as_of_join`'s own docstring states. A brute-force post-filter here
    would silently reintroduce the O(n^2) scan Task 3's index exists to
    avoid, and would still leak: the index never returned the future
    neighbor's vector in the first place, so there is nothing to filter.
    """
    return index.query(vector, as_of=as_of, k=k)


def _parse_entity_key(entity_key: str) -> tuple[str, int, int] | None:
    """`<kind>:<repo_id>:<number>`, or None if the key is malformed. Task 4's
    first real run (2026-09-06) crashed here on `issue:<repo_id>` keys: Task
    2's extract_texts read `$.payload.number` for issues too, but an
    IssuesEvent carries its number under `payload.issue`, and `concat_ws`
    silently drops the null. A neighbor that names no specific entity can't
    be resolved, the same as a well-formed issue neighbor."""
    kind, _, rest = entity_key.partition(":")
    repo_id, _, number = rest.partition(":")
    if not (repo_id.isdigit() and number.isdigit()):
        return None
    return kind, int(repo_id), int(number)


_NeighborPair = tuple[int, int, datetime, int | None, int | None]


def _neighbor_rows(spine_row: Row, index: SimilarityIndex, *, k: int) -> list[_NeighborPair]:
    """One row per (spine PR, candidate neighbor) pair, or none if the spine
    PR itself was never embedded (Task 2's extract_texts drops rows missing
    title/body) -- there is no vector to query with, so similarity is
    unknown for it, not zero.
    """
    if spine_row["embedding"] is None:
        return []
    # NOT spine_row["as_of_timestamp"].replace(tzinfo=UTC): a collected
    # TIMESTAMP comes back naive in the *driver JVM's* default timezone,
    # not necessarily UTC (tests/helpers.py's epoch_of hit this same trap;
    # verified directly here too -- under TZ=America/New_York this project's
    # own local_session still collected a UTC noon instant as naive 08:00).
    # as_of_epoch is computed by unix_timestamp() under Spark's *session*
    # timeZone (explicitly "UTC", almanac.spark.local_session), which is
    # unambiguous regardless of the machine it runs on.
    as_of = datetime.fromtimestamp(spine_row["as_of_epoch"], tz=UTC)
    neighbors = query_similar(index, spine_row["embedding"], as_of, k=k)
    rows = []
    for neighbor in neighbors:
        parsed = _parse_entity_key(neighbor.entity_key)
        # Only a well-formed "pr" neighbor can ever resolve against
        # `resolutions` (issues carry no SLA-breach outcome; a degenerate
        # key names no specific entity) -- kept null rather than joined on
        # repo_id/number alone, which an issue and an unrelated PR in the
        # same repo could collide on.
        pr = parsed[1:] if parsed is not None and parsed[0] == "pr" else (None, None)
        # A plain tuple, not Row(repo_id=..., ...): createDataFrame with an
        # explicit schema maps by *position* even for Row objects (verified
        # directly, not assumed -- Row(b=2, a=1) against schema [a, b] comes
        # back a=2, b=1), the same landmine Task 2 hit for pandas. Order
        # here must match _NEIGHBOR_PAIR_SCHEMA's field order exactly.
        rows.append(
            (
                spine_row["repo_id"],
                spine_row["pr_number"],
                spine_row["as_of_timestamp"],
                pr[0],
                pr[1],
            )
        )
    return rows


def compute_pr_similarity(
    spine: DataFrame, index: SimilarityIndex, resolutions: DataFrame, *, k: int = 10
) -> DataFrame:
    """For each spine row -- (repo_id, pr_number, as_of_timestamp, embedding)
    -- query_similar's k nearest already-opened candidates, then rate how
    many had breached strictly before this spine row's own as_of arrived.

    `resolutions` is (repo_id, pr_number, closed_at, breach): every PR with
    a defined breach outcome, the same population `join_breach_label`
    trains on (`model/dataset.py`) -- reused, not re-derived, since it is
    the one already-tested definition of "resolved" this project has.

    The query itself runs one call per spine row, driver-side: the real
    spine is on the order of the quarter's PR-opened population against a
    millisecond-latency endpoint, not a Spark-scale join, so collecting
    first is the deliberate boundary here -- never a substitute for the
    real ANN index, which is still what answers each individual query.
    """
    # Collected once, and every later reference reads THIS list rather than
    # the DataFrame again. `run_similarity` samples with
    # `orderBy(rand(seed)).limit(n)`, and Spark classifies `rand` as
    # nondeterministic -- a second evaluation may draw a different sample.
    # Task 4's 2026-09-06 real run did exactly that: ~62,000 neighbor pairs
    # computed against sample A, then joined back against sample B, leaving
    # only the 44 of 10,000 rows the two samples happened to share. It also
    # saves a second full scan of Silver.
    spine_rows = spine.select(
        "repo_id",
        "pr_number",
        "as_of_timestamp",
        "embedding",
        F.unix_timestamp("as_of_timestamp").alias("as_of_epoch"),
    ).collect()
    spine_keys = spine.sparkSession.createDataFrame(
        [(row["repo_id"], row["pr_number"]) for row in spine_rows], schema=_SPINE_KEY_SCHEMA
    )
    pairs = [row for spine_row in spine_rows for row in _neighbor_rows(spine_row, index, k=k)]
    if not pairs:
        return spine_keys.withColumn(
            "similar_neighbor_count", F.lit(0).cast(LongType())
        ).withColumn("similar_prior_breach_rate", F.lit(None).cast(DoubleType()))

    neighbor_pairs = spine.sparkSession.createDataFrame(pairs, schema=_NEIGHBOR_PAIR_SCHEMA)
    resolved = resolutions.select(
        F.col("repo_id").alias("neighbor_repo_id"),
        F.col("pr_number").alias("neighbor_pr_number"),
        "closed_at",
        "breach",
    )
    joined = neighbor_pairs.join(
        resolved, on=["neighbor_repo_id", "neighbor_pr_number"], how="left"
    )
    # A neighbor counts toward the rate only if IT closed strictly before
    # THIS spine row's own as_of -- the leakage axis this function exists
    # for. Not yet closed (or never matched at all, e.g. an issue neighbor)
    # stays null: `F.avg` already ignores nulls, which is exactly "excluded
    # from the numerator", and an all-null group returns null rather than
    # 0.0 for free.
    resolved_breach = F.when(F.col("closed_at") < F.col("as_of_timestamp"), F.col("breach"))

    per_spine = joined.groupBy("repo_id", "pr_number").agg(
        F.count(F.lit(1)).alias("similar_neighbor_count"),
        F.avg(resolved_breach.cast(DoubleType())).alias("similar_prior_breach_rate"),
    )
    # Left join back onto the full spine: a spine row with no embedding
    # produced zero pairs above and would otherwise vanish rather than
    # surface as "unknown" (every as_of_join in this package makes the same
    # choice -- every spine row survives).
    return spine_keys.join(per_spine, on=["repo_id", "pr_number"], how="left").withColumn(
        "similar_neighbor_count", F.coalesce(F.col("similar_neighbor_count"), F.lit(0))
    )
