"""join_label: attaches Gold's label onto a point-in-time-correct
training frame -- a label is supervision, not a feature, so this lives
outside almanac/features/ (which never reads Gold, §3.1) without
extending that same boundary to the label (§5.2).
"""

import pytest
from pyspark.sql import SparkSession

from almanac.model.dataset import join_label

pytestmark = pytest.mark.spark


def test_joins_the_label_and_drops_rows_with_no_defined_outcome(spark: SparkSession) -> None:
    training_frame = spark.createDataFrame(
        [(1, 5, "alice"), (1, 6, "bob")],
        "repo_id long, pr_number long, author_login string",
    )
    fact_pull_request = spark.createDataFrame(
        [
            (1, 5, 3600, None),
            (1, 6, None, "right_censored"),
        ],
        "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string",
    )

    result = join_label(training_frame, fact_pull_request).collect()

    assert len(result) == 1
    assert result[0]["pr_number"] == 5
    assert result[0]["time_to_first_response_seconds"] == 3600
