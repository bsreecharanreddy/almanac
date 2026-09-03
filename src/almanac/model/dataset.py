"""Attaches Gold's label onto a point-in-time-correct training frame.

Reads Gold, unlike almanac/features/: a label is supervision about the
future outcome, not a point-in-time feature, so §3.1's never-read-Gold
boundary does not extend to it (design doc §5.2).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def join_label(training_frame: DataFrame, fact_pull_request: DataFrame) -> DataFrame:
    """Inner-join Gold's label on, keeping only rows with a defined outcome."""
    label = fact_pull_request.select(
        "repo_id", "pr_number", "time_to_first_response_seconds", "label_exclusion"
    )
    joined = training_frame.join(label, on=["repo_id", "pr_number"], how="inner")
    return joined.where(F.col("label_exclusion").isNull()).drop("label_exclusion")
