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
    PR opened -- an open or not-yet-observed prior PR is not "not merged",
    it is unknown, and folding it into the denominator would teach the
    model an outcome it could not have had (`test_features_groups.py`'s
    worked example).

    A running aggregate over each author's own open- and close-events, not
    a `this join prior` self-join: a handful of bot logins open a large
    enough share of every PR that the self-join is a multi-terabyte
    cartesian blow-up on those keys, and `spark.sql.adaptive.skewJoin`
    cannot split a self-join (2026-09-04,
    docs/findings/2026-09-04-author-activity-self-join.md).
    `compute_repo_activity` already uses this same running-window shape.
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

    resolutions = opened.join(closed, on=["repo_id", "pr_number"], how="inner").select(
        "author_login",
        F.col("closed_at").alias("event_ts"),
        # A resolution sorts *after* a query at the same instant, so a prior
        # closing at exactly this PR's open time is not yet visible to it.
        F.lit(1).alias("tie_rank"),
        F.lit(True).alias("is_resolution"),
        F.when(F.col("merged"), F.lit(1.0)).otherwise(F.lit(0.0)).alias("merged_value"),
    )
    queries = opened.select(
        "author_login",
        F.col("opened_at").alias("event_ts"),
        F.lit(0).alias("tie_rank"),
        F.lit(False).alias("is_resolution"),
        F.lit(None).cast("double").alias("merged_value"),
    )

    to_date = (
        Window.partitionBy("author_login")
        .orderBy("event_ts", "tie_rank")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    running = resolutions.unionByName(queries).select(
        "author_login",
        "event_ts",
        "is_resolution",
        F.sum(F.col("is_resolution").cast("int")).over(to_date).alias("prior_pr_count"),
        F.sum("merged_value").over(to_date).alias("prior_merged"),
    )

    return (
        running.where(~F.col("is_resolution"))
        .select(
            "author_login",
            F.col("event_ts").alias("event_time"),
            "prior_pr_count",
            F.when(
                F.col("prior_pr_count") > 0, F.col("prior_merged") / F.col("prior_pr_count")
            ).alias("prior_merge_rate"),
        )
        .dropDuplicates(["author_login", "event_time"])
    )
