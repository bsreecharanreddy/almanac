"""Quality rules and the clean/quarantine split. Pure; no I/O."""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from almanac.pipeline.source import QualityRule, Severity

FAILED_RULES = "_failed_rules"
REJECT_RULES = "_reject_rules"


def apply_rules(df: DataFrame, rules: Sequence[QualityRule]) -> DataFrame:
    """Annotate each row with the names of the rules it failed.

    An **array of rule names**, not a boolean: a boolean says a record is
    bad, the array says why, which is what makes quality analyzable instead
    of a dead-letter bin (design doc §4.2).

    Every predicate is wrapped in ``coalesce(expr, False)``. Spark uses
    three-valued logic, so ``NULL > 5`` is ``NULL`` rather than ``False``,
    and an un-coalesced rule silently passes exactly the malformed rows it
    was written to catch.
    """

    def _failed(rule: QualityRule) -> Column:
        return F.when(~F.coalesce(F.expr(rule.expression), F.lit(False)), F.lit(rule.name))

    empty = F.array().cast("array<string>")

    def _names(subset: Sequence[QualityRule]) -> Column:
        if not subset:
            return empty
        # array() keeps a null per passing rule; array_compact drops them.
        # Confirmed available in Spark 4.2.0 during Phase 0 Task 2.
        return F.array_compact(F.array(*[_failed(r) for r in subset]))

    rejects = [r for r in rules if r.severity is Severity.REJECT]
    return df.withColumn(FAILED_RULES, _names(rules)).withColumn(REJECT_RULES, _names(rejects))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition into ``(clean, quarantined)``.

    Only ``REJECT`` failures quarantine. A ``WARN`` is recorded in
    ``_failed_rules`` and the row stays in the clean set -- used where a
    field is legitimately absent in one era but required in another. That
    is why severity has to survive into this function rather than being
    collapsed to a single boolean upstream.

    Nothing is dropped: every input row appears in exactly one output, which
    ``test_split_conserves_every_record`` asserts.
    """
    has_reject = F.size(F.col(REJECT_RULES)) > 0
    return (
        df.filter(~has_reject).drop(REJECT_RULES),
        df.filter(has_reject).drop(REJECT_RULES),
    )
