"""The bot heuristic as a Spark column expression -- the third of three
required-identical restatements alongside explore.measure.classify_bot
(Python) and the almanac_is_bot dbt macro (SQL). Reuses the same pattern
object rather than a fourth hand-copied regex string; test_bot_macro.py
already proves Java `rlike` and Python `re` agree on this exact pattern.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from almanac.explore.measure import BOT_REGEX, CURATED_BOTS


def is_bot_column(login: Column) -> Column:
    return (
        login.endswith("[bot]")
        | F.lower(login).isin(*CURATED_BOTS)
        | login.rlike(BOT_REGEX.pattern)
    )
