"""The v1 feature groups, each Silver-native (§3.1). `author_activity`
and `repo_activity` are event logs an as-of join reads through; `pr_static`
is already known at PR-open time and is joined directly (assemble.py),
never through as_of_join.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from almanac.features.bot import is_bot_column


def _opened() -> Column:
    """A fresh Column each call, not a module-level constant: `F.col(...)`
    asserts an active SparkContext, which does not exist yet at import time.
    """
    return (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "opened")


def _closed() -> Column:
    return (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "closed")


def compute_repo_activity(events: DataFrame) -> DataFrame:
    """One row per Silver event carrying a repo_id: that repo's cumulative
    event volume, bot share, and PR-open count, inclusive of this event's
    own timestamp -- `as_of_join`'s strict `<` is what excludes an event
    from seeing its own contribution when it is itself a spine cutoff.
    """
    window = (
        Window.partitionBy("repo_id")
        .orderBy("created_at")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    scoped = (
        events.where(F.col("repo_id").isNotNull())
        .withColumn(
            "_is_bot", F.coalesce(is_bot_column(F.col("actor_login")), F.lit(False)).cast("int")
        )
        .withColumn("_is_pr_open", _opened().cast("int"))
    )

    events_total = F.count(F.lit(1)).over(window)
    bot_events = F.sum("_is_bot").over(window)
    prs_opened = F.sum("_is_pr_open").over(window)

    return scoped.select(
        "repo_id",
        F.col("created_at").alias("event_time"),
        events_total.alias("events_total_to_date"),
        bot_events.alias("bot_events_to_date"),
        prs_opened.alias("prs_opened_to_date"),
        (bot_events / events_total).alias("bot_share_to_date"),
    )


def compute_pr_static(events: DataFrame) -> DataFrame:
    """Attributes already known the instant a PR opens -- assemble.py
    joins this on (repo_id, pr_number) directly, not through as_of_join,
    since there is nothing temporal to look up.
    """
    return events.where(_opened()).select(
        "repo_id",
        "pr_number",
        F.col("pr_draft").alias("is_draft"),
        is_bot_column(F.col("actor_login")).alias("is_bot_author"),
        F.dayofweek("created_at").alias("opened_day_of_week"),
        F.hour("created_at").alias("opened_hour"),
    )


def compute_author_activity(events: DataFrame) -> DataFrame:
    """One row per PR ever opened: the author's prior PR count and merge
    rate, as known strictly before this PR's own open time.

    A prior PR counts as "known" only if it had already closed before this
    PR opened (`prior.closed_at < this.opened_at`) -- an open or
    not-yet-observed prior PR is not "not merged", it is unknown, and
    folding it into the denominator would teach the model an outcome it
    could not have had (this task's worked example).
    """
    opened = events.where(_opened()).select(
        "repo_id",
        "pr_number",
        F.col("actor_login").alias("author_login"),
        F.col("created_at").alias("opened_at"),
    )
    closed = events.where(_closed()).select(
        "repo_id",
        "pr_number",
        F.col("created_at").alias("closed_at"),
        F.col("pr_merged").alias("merged"),
    )
    prs = opened.join(closed, on=["repo_id", "pr_number"], how="left")

    this, prior = prs.alias("this"), prs.alias("prior")
    known_priors = this.join(
        prior,
        on=(
            (F.col("this.author_login") == F.col("prior.author_login"))
            & (F.col("prior.opened_at") < F.col("this.opened_at"))
            & F.col("prior.closed_at").isNotNull()
            & (F.col("prior.closed_at") < F.col("this.opened_at"))
        ),
        how="left",
    )

    return known_priors.groupBy(
        F.col("this.author_login").alias("author_login"),
        F.col("this.opened_at").alias("event_time"),
    ).agg(
        F.count(F.col("prior.pr_number")).alias("prior_pr_count"),
        F.avg(
            F.when(
                F.col("prior.pr_number").isNotNull(),
                F.when(F.col("prior.merged"), 1.0).otherwise(0.0),
            )
        ).alias("prior_merge_rate"),
    )
