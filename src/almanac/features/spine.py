"""The PR-opened event population -- the entity list every v1 feature
group's as-of join runs against.

Re-derived from Silver directly, not read from `fact_pull_request`: §3.1
makes the feature platform a peer of Gold, not a consumer of it, and
reading Gold's own fact here would turn that boundary into a claim rather
than a structural fact (design doc §4.4a).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_pr_opened_spine(events: DataFrame) -> DataFrame:
    """One row per PR-opened event: who opened it, and when.

    `as_of_timestamp` is the opening event's own `created_at` -- the
    instant every as-of join in this package treats as "now" for that PR.
    """
    return events.where(
        (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "opened")
    ).select(
        "repo_id",
        "pr_number",
        F.col("actor_login").alias("author_login"),
        F.col("created_at").alias("as_of_timestamp"),
    )
