"""One wide, point-in-time-correct training frame: the as_of demo §9's
Phase 3 gate names. `pr_static` is joined directly -- it is not a
temporal lookup, so routing it through as_of_join would only add an
unnecessary time comparison against a spine row's own PR.
"""

from pyspark.sql import DataFrame

from almanac.features.join import as_of_join


def assemble_training_set(
    spine: DataFrame,
    *,
    author_activity: DataFrame,
    repo_activity: DataFrame,
    pr_static: DataFrame,
) -> DataFrame:
    result = as_of_join(spine, author_activity, on=["author_login"])
    result = as_of_join(result, repo_activity, on=["repo_id"])
    return result.join(pr_static, on=["repo_id", "pr_number"], how="left")
