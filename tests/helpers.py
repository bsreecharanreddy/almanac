"""Shared assertions for the Spark test modules."""

from pyspark.sql import DataFrame, Row


def one(df: DataFrame) -> Row:
    """``first()`` is typed ``Row | None``; every call here expects a row."""
    row = df.first()
    assert row is not None
    return row


def epoch_of(df: DataFrame, column: str) -> int:
    """A timestamp column's stored instant, as an epoch second.

    No test asserts on a *collected* datetime. PySpark hands one back naive,
    converted to the driver's timezone, so a datetime comparison passes or
    fails depending on the machine running it -- and passes on a UTC CI
    runner either way (Task 2's finding, STATUS.md 2026-09-01). An epoch is
    an instant and carries no timezone ambiguity at all.
    """
    return int(one(df.selectExpr(f"unix_timestamp({column}) AS epoch"))["epoch"])
