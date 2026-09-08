"""What a consumer of a governed table may rely on, and what enforces it."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

_CONSTRAINT_PREFIX = "delta.constraints."


class ContractError(Exception):
    """Raised before a write, so a breaking shape never reaches the table."""


@dataclass(frozen=True)
class Contract:
    """One governed surface: its shape, its key, and its row-local invariants."""

    surface: str
    columns: dict[str, str]
    keys: tuple[str, ...]
    checks: dict[str, str] = field(default_factory=dict)


def schema_violations(schema: StructType, contract: Contract) -> list[str]:
    """Every way `schema` departs from the contract, named so a reader can act on it."""
    actual = {f.name: f.dataType.simpleString() for f in schema.fields}
    missing = [
        f"{contract.surface}.{name} is missing" for name in contract.columns.keys() - actual.keys()
    ]
    extra = [
        f"{contract.surface}.{name} is not in the contract"
        for name in actual.keys() - contract.columns.keys()
    ]
    mistyped = [
        f"{contract.surface}.{name} is {actual[name]}, contracted as {declared}"
        for name, declared in contract.columns.items()
        if name in actual and actual[name] != declared
    ]
    return sorted(missing + extra + mistyped)


def enforce(df: DataFrame, contract: Contract) -> None:
    """Refuse a frame whose shape a consumer could not have expected.

    Delta's own schema enforcement does not cover this. Measured 2026-09-07 on
    delta-spark 4.4.0: `mode("overwrite")` **accepts** a frame missing a column
    and nulls that column for every row, leaving the table's schema intact --
    the data is gone and nothing raises. (A widened type it does reject.) So a
    dropped column is caught here or not at all.
    """
    problems = schema_violations(df.schema, contract)
    if problems:
        raise ContractError(f"{contract.surface} breaks its contract: {'; '.join(problems)}")


def check_expressions(contract: Contract) -> dict[str, str]:
    """Every row-local rule the table can enforce itself: non-null keys, then the declared checks.

    Key nullability is a CHECK rather than `ALTER COLUMN ... SET NOT NULL`
    because open-source Delta refuses the latter on a populated table, which is
    why `features.registration.not_null_key_sql` never runs outside Databricks.
    A CHECK constraint is accepted by both.
    """
    keys = {f"{key}_not_null": f"{key} IS NOT NULL" for key in contract.keys}
    return {**keys, **contract.checks}


def _normalized(expression: str) -> str:
    """Delta stores the expression it reparsed, so compare on whitespace and case alone."""
    return " ".join(expression.split()).lower()


def apply_constraints(spark: SparkSession, path: str, contract: Contract) -> None:
    """Make the table's CHECK constraints match the contract -- adding, replacing, dropping.

    Applied by path, so it needs no metastore and runs identically local and on
    Databricks. Constraints survive `mode("overwrite")` (measured 2026-09-07),
    so once applied they bind every later writer, not only this one.
    """
    target = f"delta.`{path}`"
    wanted = check_expressions(contract)
    existing = {
        str(row[0]).removeprefix(_CONSTRAINT_PREFIX): str(row[1])
        for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()
        if str(row[0]).startswith(_CONSTRAINT_PREFIX)
    }

    for name in existing.keys() - wanted.keys():
        spark.sql(f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS {name}")
    for name, expression in wanted.items():
        # ADD CONSTRAINT validates every existing row, so an unchanged
        # constraint is left alone rather than rescanning the table each run.
        if _normalized(existing.get(name, "")) == _normalized(expression):
            continue
        spark.sql(f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {target} ADD CONSTRAINT {name} CHECK ({expression})")


def duplicate_keys(df: DataFrame, contract: Contract) -> int:
    """Rows beyond one per key. Not a CHECK constraint -- uniqueness is not row-local."""
    return df.count() - df.select(*contract.keys).distinct().count()
