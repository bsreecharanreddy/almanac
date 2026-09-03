"""Quality rules and the clean/quarantine split. Pure; no I/O."""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from almanac.pipeline.source import QualityRule, Severity

FAILED_RULES = "_failed_rules"
REJECT_RULES = "_reject_rules"


def apply_rules(df: DataFrame, rules: Sequence[QualityRule]) -> DataFrame:
    """Annotate each row with the names of the rules it failed (an array, not a bool)."""

    def _failed(rule: QualityRule) -> Column:
        # coalesce(expr, False): under three-valued logic NULL > 5 is NULL,
        # and an un-coalesced rule passes the malformed rows it exists to catch.
        return F.when(~F.coalesce(F.expr(rule.expression), F.lit(False)), F.lit(rule.name))

    empty = F.array().cast("array<string>")

    def _names(subset: Sequence[QualityRule]) -> Column:
        if not subset:
            return empty
        return F.array_compact(F.array(*[_failed(r) for r in subset]))

    rejects = [r for r in rules if r.severity is Severity.REJECT]
    return df.withColumn(FAILED_RULES, _names(rules)).withColumn(REJECT_RULES, _names(rejects))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition into (clean, quarantined). Only REJECT failures quarantine.

    A WARN is recorded in ``_failed_rules`` and the row stays clean -- used
    where a field is legitimately absent in one era. Nothing is dropped;
    every input row appears in exactly one output.
    """
    has_reject = F.size(F.col(REJECT_RULES)) > 0
    return (
        df.filter(~has_reject).drop(REJECT_RULES),
        df.filter(has_reject).drop(REJECT_RULES),
    )
