"""Parse Bronze's raw JSON into the columns Silver normalizes. Pure; no I/O.

This is where the medallion's one legal parse happens. Bronze stores each
record as an unparsed string, so every field Silver and Gold depend on is
extracted here, once, against an **explicit** schema.

The schema is explicit rather than inferred for a measured reason: Task 7's
calibration failed when `spark.read.json` inferred different schemas for
different hours of the same day and Delta rejected the second write. An
explicit schema also means an unrecognised payload shape yields nulls in
the columns it lacks instead of breaking ingestion, which is the rule a new
GitHub event type must never be able to violate.

One schema covers all three eras. Where the eras disagree, both spellings
are declared and coalesced -- `repo` vs `repository`, and so on -- so there
is no era branch here at all. The single exception is `actor`, which is a
bare string before 2015 and an object after; a struct field cannot be both,
so it is read with `get_json_object` instead.
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

# Only the fields something downstream actually consumes. Declaring the
# whole 48-key payload would make this schema a second, competing record of
# what GH Archive contains -- and one that goes stale silently.
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
        # Presence is the whole signal: an issue carrying a `pull_request`
        # object *is* a PR. Only `url` is declared because nothing reads the
        # rest, and a struct is needed for `IS NOT NULL` to mean anything.
        StructField("pull_request", StructType([StructField("url", StringType())])),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        # Top-level `url` is legacy-only, and it is load-bearing there: it is
        # the field that stops the content-hash surrogate key merging two
        # genuinely different events (see `eras._content_hash`).
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
                ]
            ),
        ),
    ]
)

# The contract `eras.normalize_events` consumes. Named here because it is
# the seam between parsing and era normalization, and Phase 1 lost a day to
# two modules disagreeing about exactly this kind of implicit contract.
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
)


def _actor() -> Column:
    """The actor's login, across a field whose *type* changed in 2015.

    Legacy `actor` is the login string itself; modern `actor` is an object
    carrying `login`. `get_json_object` sidesteps the type conflict a single
    struct schema cannot express: the modern path is tried first, and the
    legacy path only answers when it returns nothing.

    Measured on the committed fixtures: `$.actor.login` resolves on 2,000 of
    2,000 modern records and 0 of 2,000 legacy ones.
    """
    return F.coalesce(
        F.get_json_object("raw_json", "$.actor.login"),
        F.get_json_object("raw_json", "$.actor"),
    )


def _repo_name() -> Column:
    """`owner/repo`, reconstructed for legacy so both eras mean one thing.

    Modern events carry `repo.name` already qualified. Legacy splits it
    across `repository.owner` and `repository.name`, and joining them with
    `concat` -- which propagates nulls -- rather than `concat_ws` -- which
    skips them -- is deliberate: a missing owner must yield no name at all,
    never a bare unqualified name masquerading as a valid repo.

    Leaving legacy unqualified would make the column mean two different
    things either side of 2015, and Gold's SCD2 would read the era boundary
    as a mass rename of every repo that survived it.
    """
    return F.coalesce(
        F.col("e.repo.name"),
        F.concat(F.col("e.repository.owner"), F.lit("/"), F.col("e.repository.name")),
    )


def parse_events(df: DataFrame, *, json_column: str = "raw_json") -> DataFrame:
    """Bronze rows in, the raw Silver contract out. Every other column survives.

    Passthrough matters as much as extraction: `ingested_at`, `source_file`,
    `event_date` and `event_hour` are Bronze's provenance and Silver's
    partitioning, and dropping them here is what made Phase 1's Task 4 and
    Task 5 impossible to compose.

    A record that will not parse yields nulls rather than disappearing. It
    then fails `event_type_present` and lands in quarantine, which is where
    an unreadable record belongs -- not in neither table.
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
        # GitHub numbers issues and pull requests from a single per-repo
        # sequence, so these three never disagree about what a number means.
        # `payload.number` is the PullRequestEvent spelling, the other two
        # cover review, review-comment and issue-comment events.
        F.coalesce(
            F.col("e.payload.number"),
            F.col("e.payload.pull_request.number"),
            F.col("e.payload.issue.number"),
        ).alias("pr_number"),
        F.col("e.payload.pull_request.merged").alias("pr_merged"),
        F.col("e.payload.pull_request.draft").alias("pr_draft"),
        F.col("e.payload.issue.pull_request").isNotNull().alias("issue_is_pr"),
    )
