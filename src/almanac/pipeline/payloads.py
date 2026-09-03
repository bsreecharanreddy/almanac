"""Parse Bronze's raw JSON into the columns Silver normalizes. Pure; no I/O.

One explicit schema for all three eras -- explicit because `spark.read.json`
inferred different schemas per hour and Delta rejected the second write
(Task 7). Where eras disagree, both spellings are declared and coalesced;
`actor` is the exception (bare string pre-2015, object after), read with
`get_json_object`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Only fields something downstream consumes -- declaring the full payload
# would be a second record of GH Archive's shape that goes stale silently.
_PULL_REQUEST = StructType(
    [
        StructField("number", LongType()),
        StructField("merged", BooleanType()),
        StructField("draft", BooleanType()),
    ]
)

_ISSUE = StructType(
    [
        StructField("number", LongType()),
        # Presence is the signal: an issue carrying a pull_request object is a PR.
        StructField("pull_request", StructType([StructField("url", StringType())])),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        # Legacy-only, and load-bearing there: the field that stops the
        # content-hash key merging two different events (eras._content_hash).
        StructField("url", StringType()),
        StructField(
            "repo", StructType([StructField("id", LongType()), StructField("name", StringType())])
        ),
        StructField(
            "repository",
            StructType(
                [
                    StructField("id", LongType()),
                    StructField("name", StringType()),
                    StructField("owner", StringType()),
                ]
            ),
        ),
        StructField(
            "payload",
            StructType(
                [
                    StructField("action", StringType()),
                    StructField("number", LongType()),
                    StructField("pull_request", _PULL_REQUEST),
                    StructField("issue", _ISSUE),
                    # PushEvent volume. Never size(commits) -- the array caps
                    # at 20 (§12 trap 3). distinct_size is modern-only.
                    StructField("size", LongType()),
                    StructField("distinct_size", LongType()),
                ]
            ),
        ),
    ]
)

# The contract eras.normalize_events consumes -- named because Phase 1 lost
# a day to two modules disagreeing about exactly this implicit contract.
RAW_COLUMNS = (
    "created_at_raw",
    "actor_raw",
    "repo_id",
    "repo_name",
    "event_type",
    "event_action",
    "id",
    "event_url",
    "pr_number",
    "pr_merged",
    "pr_draft",
    "issue_is_pr",
    "push_size",
    "push_distinct_size",
)


def _actor() -> Column:
    """The actor's login, across a field whose type changed in 2015.

    Legacy ``actor`` is the login string; modern ``actor`` is an object with
    ``login``. Modern path first; legacy answers only when it returns nothing.
    """
    return F.coalesce(
        F.get_json_object("raw_json", "$.actor.login"),
        F.get_json_object("raw_json", "$.actor"),
    )


def _repo_name() -> Column:
    """``owner/repo``, reconstructed for legacy so both eras mean one thing.

    ``concat`` (null-propagating), not ``concat_ws``: a missing owner must
    yield no name, never a bare unqualified one. Leaving legacy unqualified
    would make Gold's SCD2 read the era boundary as a mass rename.
    """
    return F.coalesce(
        F.col("e.repo.name"),
        F.concat(F.col("e.repository.owner"), F.lit("/"), F.col("e.repository.name")),
    )


def parse_events(df: DataFrame, *, json_column: str = "raw_json") -> DataFrame:
    """Bronze rows in, the raw Silver contract out; every other column survives.

    Passthrough of ``ingested_at`` / ``source_file`` / ``event_date`` /
    ``event_hour`` matters as much as extraction -- dropping them made Phase
    1's Tasks 4-5 impossible to compose. An unparseable record yields nulls,
    fails ``event_type_present``, and lands in quarantine.
    """
    passthrough = [c for c in df.columns if c != json_column]
    parsed = df.withColumn("e", F.from_json(F.col(json_column), EVENT_SCHEMA))

    return parsed.select(
        *passthrough,
        F.col("e.created_at").alias("created_at_raw"),
        _actor().alias("actor_raw"),
        F.coalesce(F.col("e.repo.id"), F.col("e.repository.id")).alias("repo_id"),
        _repo_name().alias("repo_name"),
        F.col("e.type").alias("event_type"),
        F.col("e.payload.action").alias("event_action"),
        F.col("e.id").cast("string").alias("id"),
        F.col("e.url").alias("event_url"),
        # GitHub numbers issues and PRs from one per-repo sequence, so these
        # never disagree; the three spellings cover PR / review / comment events.
        F.coalesce(
            F.col("e.payload.number"),
            F.col("e.payload.pull_request.number"),
            F.col("e.payload.issue.number"),
        ).alias("pr_number"),
        F.col("e.payload.pull_request.merged").alias("pr_merged"),
        F.col("e.payload.pull_request.draft").alias("pr_draft"),
        F.col("e.payload.issue.pull_request").isNotNull().alias("issue_is_pr"),
        F.col("e.payload.size").alias("push_size"),
        F.col("e.payload.distinct_size").alias("push_distinct_size"),
    )
