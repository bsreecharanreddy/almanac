"""`dbt/macros/almanac_is_bot.sql` must not drift from `classify_bot`."""

import re
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.explore.measure import BOT_REGEX, BotMatch, classify_bot

pytestmark = pytest.mark.spark

MACRO = Path("dbt/macros/almanac_is_bot.sql")

# Every shape the finding calls out: [bot] suffix, curated list, real CI
# accounts, and human surnames a bare `ci$` wrongly matched.
LOGINS = [
    "dependabot[bot]",
    "renovate[bot]",
    "github-actions[bot]",
    "dependabot",
    "renovate",
    "k8s-ci-robot",
    "facebook-github-bot",
    "some-automation",
    "swift-ci",
    "aws-sdk-rust-ci",
    "VenlyCI",
    "CheckmkCI",
    "BarisYazici",
    "AlexandruPopovici",
    "AlperenYABACI",
    "AitanaESCI",
    "cw-circleci",
    "octocat",
    "torvalds",
    "robotframework",
    "Abbott",
]


def _regex_in_macro() -> str:
    text = MACRO.read_text()
    match = re.search(r"rlike '([^']*)'", text)
    assert match, "no rlike literal found in the macro"
    return match.group(1)


def test_macro_regex_is_byte_identical_to_the_python_one() -> None:
    assert _regex_in_macro() == BOT_REGEX.pattern


def test_macro_and_classify_bot_agree_on_real_logins(spark: SparkSession) -> None:
    regex = _regex_in_macro()
    df = spark.createDataFrame([(login,) for login in LOGINS], "login string")
    sql_flag = df.selectExpr(
        "login",
        "("
        "endswith(login, '[bot]') "
        "or lower(login) in ('dependabot', 'renovate', 'github-actions') "
        f"or login rlike '{regex}'"
        ") as is_bot",
    )
    got = {row["login"]: row["is_bot"] for row in sql_flag.collect()}
    expected = {login: classify_bot(login) is not BotMatch.NONE for login in LOGINS}
    assert got == expected
