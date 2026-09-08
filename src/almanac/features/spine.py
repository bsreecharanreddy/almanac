"""The PR-opened event population -- the entity list every v1 feature
group's as-of join runs against.

Re-derived from Silver directly, not read from `fact_pull_request`: §3.1
makes the feature platform a peer of Gold, not a consumer of it, and
reading Gold's own fact here would turn that boundary into a claim rather
than a structural fact (design doc §4.4a).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Ordered so the earliest event wins, then deterministically among ties.
# Byte-for-byte reproducibility is the governing invariant, so the winner of
# a `created_at` tie cannot be left to Spark's row order.
_EARLIEST_FIRST: tuple[str, ...] = ("created_at", "actor_login", "pr_draft")


def opened_predicate() -> Column:
    """A fresh Column each call: `F.col(...)` asserts an active SparkContext,
    which does not exist yet at import time.
    """
    return (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "opened")


def earliest_opened_events(events: DataFrame) -> DataFrame:
    """One row per PR -- its earliest `opened` event -- not one per event.

    GH Archive carries two distinct `opened` events for the same PR (different
    `event_id`s, so Silver is right to keep both): 25 PRs in the measured
    quarter. Emitting per event let `assemble_training_set`'s equi-join
    multiply them 2x2 into 4 rows each, 75 rows over the 7,320,121-PR
    population (docs/findings/2026-09-08-pr-opened-spine-fanout.md).

    Earliest wins, matching the accumulating snapshot's own out-of-order rule.
    Shared with `compute_pr_static` rather than fixed in each: both joined on
    the same key, so a dedup in only one of them still fans out.
    """
    ranked = events.where(opened_predicate()).withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("repo_id", "pr_number").orderBy(
                *(F.col(c).asc_nulls_last() for c in _EARLIEST_FIRST)
            )
        ),
    )
    return ranked.where(F.col("_rank") == 1).drop("_rank")


def build_pr_opened_spine(events: DataFrame) -> DataFrame:
    """One row per PR: who opened it, and when.

    `as_of_timestamp` is the opening event's own `created_at` -- the
    instant every as-of join in this package treats as "now" for that PR.
    """
    return earliest_opened_events(events).select(
        "repo_id",
        "pr_number",
        F.col("actor_login").alias("author_login"),
        F.col("created_at").alias("as_of_timestamp"),
    )
