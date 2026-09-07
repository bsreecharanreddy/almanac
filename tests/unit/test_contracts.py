"""The contract checker itself, and the one place two column lists have to agree."""

import pytest
from pyspark.sql.types import DataType, LongType, StringType, StructField, StructType

from almanac.contracts import Contract, check_expressions, schema_violations
from almanac.features.runner import FEATURE_TABLES, FeatureTableSpec
from almanac.pipeline.eras import SILVER_COLUMNS
from almanac.stream.ingest import STREAM_SILVER_CONTRACT
from almanac.stream.runner import STREAM_FEATURE_TABLES

CONTRACT = Contract(
    surface="t",
    columns={"k": "bigint", "v": "string"},
    keys=("k",),
    checks={"v_present": "v IS NOT NULL"},
)

_TYPES: dict[str, DataType] = {"bigint": LongType(), "string": StringType()}


def _schema(**columns: str) -> StructType:
    return StructType([StructField(name, _TYPES[t]) for name, t in columns.items()])


def test_a_matching_schema_has_nothing_to_report() -> None:
    assert schema_violations(_schema(k="bigint", v="string"), CONTRACT) == []


def test_a_dropped_column_is_reported() -> None:
    """The breach Delta itself accepts: an overwrite missing a column nulls it silently."""
    assert schema_violations(_schema(k="bigint"), CONTRACT) == ["t.v is missing"]


def test_a_column_nobody_contracted_is_reported() -> None:
    """A consumer cannot rely on a column the contract never promised."""
    problems = schema_violations(_schema(k="bigint", v="string", x="string"), CONTRACT)

    assert problems == ["t.x is not in the contract"]


def test_a_widened_type_is_reported_with_both_types() -> None:
    assert schema_violations(_schema(k="string", v="string"), CONTRACT) == [
        "t.k is string, contracted as bigint"
    ]


def test_every_problem_is_named_at_once() -> None:
    """One failure must not hide the next, so a caller fixes the shape in one pass."""
    problems = schema_violations(_schema(k="string", x="string"), CONTRACT)

    assert problems == sorted(problems), "a stable order keeps the message diffable"
    assert problems == [
        "t.k is string, contracted as bigint",
        "t.v is missing",
        "t.x is not in the contract",
    ]


def test_the_keys_become_enforceable_not_null_checks() -> None:
    """Open-source Delta refuses SET NOT NULL on a populated table; a CHECK it accepts."""
    assert check_expressions(CONTRACT) == {
        "k_not_null": "k IS NOT NULL",
        "v_present": "v IS NOT NULL",
    }


def test_the_stream_silver_contract_covers_exactly_the_silver_columns() -> None:
    """Two lists, one fact. A column added to Silver has to reach the contract too."""
    assert set(STREAM_SILVER_CONTRACT.columns) == set(SILVER_COLUMNS) | {"is_late"}


@pytest.mark.parametrize("spec", FEATURE_TABLES + STREAM_FEATURE_TABLES, ids=lambda s: s.name)
def test_every_feature_table_keys_its_contract_on_contracted_columns(
    spec: FeatureTableSpec,
) -> None:
    """The contract key is built from the same two fields as the UC primary key."""
    contract = spec.contract

    assert contract.keys, f"{contract.surface} promises no key"
    assert set(contract.keys) <= set(contract.columns), "a key must be a contracted column"
