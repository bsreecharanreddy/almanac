"""The feature tier and streaming Silver, watched refusing a real breaking change.

`test_gold_contracts.py`'s discipline, one layer down: a contract nobody has
seen fail is a document. Every breach here is applied for real and the write is
confirmed refused, rather than asserted to be refusable.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.contracts import (
    Contract,
    ContractError,
    apply_constraints,
    check_expressions,
    duplicate_keys,
    schema_violations,
)
from almanac.features.runner import (
    FEATURE_TABLES,
    FeatureTableSpec,
    run_features,
    write_and_register,
)
from almanac.stream.ingest import STREAM_SILVER_CONTRACT, stream_events, write_stream_silver
from almanac.stream.runner import STREAM_FEATURE_TABLES, run_features_stage
from tests.helpers import land_poll, run_streaming_query

pytestmark = [pytest.mark.spark, pytest.mark.integration]

# `event_id` beyond the batch-only schema the neighbouring feature tests use:
# `stream.features` breaks its window ties on it, so the streaming builders
# cannot run against Silver without it.
_SCHEMA = (
    "event_id string, repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)

T = datetime(2025, 8, 13, 9, tzinfo=UTC)
GOVERNED = FEATURE_TABLES + STREAM_FEATURE_TABLES

_EVENT: dict[str, object] = {
    "id": "1",
    "type": "PushEvent",
    "created_at": "2026-09-06T12:00:00Z",
    "repo": {"id": 42, "name": "acme/almanac"},
}


_Row = tuple[
    str, int, int | None, datetime, str, str | None, str, bool | None, bool | None, None, datetime
]


def _pr(
    event_id: str,
    hours: int,
    action: str,
    actor: str,
    *,
    repo_id: int = 1,
    pr_number: int = 5,
    merged: bool | None = None,
    draft: bool = False,
) -> _Row:
    return (
        event_id,
        repo_id,
        pr_number,
        T + timedelta(hours=hours),
        "PullRequestEvent",
        action,
        actor,
        merged,
        draft,
        None,
        T,
    )


def _silver_rows(spark: SparkSession) -> DataFrame:
    """Enough Silver to exercise every check: a merged PR, a later one by the
    same author, a bot, and two events for one repo at the same instant."""
    rows: list[_Row] = [
        _pr("e1", 0, "opened", "alice"),
        # Same repo, same instant as the row above -- the tie that gave
        # `repo_activity` two rows under one primary key until 2026-09-07.
        ("e2", 1, None, T, "WatchEvent", None, "dependabot[bot]", None, None, None, T),
        _pr("e3", 3, "closed", "alice", merged=True),
        _pr("e4", 6, "opened", "alice", pr_number=6),
        _pr("e5", 9, "opened", "carol", repo_id=2, pr_number=9, draft=True),
    ]
    return spark.createDataFrame(rows, _SCHEMA)


@pytest.fixture(scope="module")
def built(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Every governed feature table, built once by the shipped runners over real Delta.

    Module-scoped because building all five is most of this file's runtime and
    the tests below only read: the two writes they attempt are the ones the
    contract refuses, so nothing lands to leak into the next test.
    """
    root = tmp_path_factory.mktemp("governed")
    silver = root / "silver"
    _silver_rows(spark).write.format("delta").save(str(silver / "clean"))

    features = root / "features"
    run_features(spark, silver_path=str(silver), features_path=str(features), register=False)
    run_features_stage(
        spark,
        silver_path=str(silver / "clean"),
        features_path=str(features),
        register=False,
        schema="features",
    )
    return root


def _constraints(spark: SparkSession, path: Path) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`").collect()
    return {
        str(r[0]).removeprefix("delta.constraints."): str(r[1])
        for r in rows
        if str(r[0]).startswith("delta.constraints.")
    }


@pytest.mark.parametrize("spec", GOVERNED, ids=lambda s: s.name)
def test_the_shipped_builder_satisfies_its_own_contract(
    built: Path, spark: SparkSession, spec: FeatureTableSpec
) -> None:
    """The baseline every breach below is measured against."""
    written = spark.read.format("delta").load(str(built / "features" / spec.name))

    assert schema_violations(written.schema, spec.contract) == []
    assert duplicate_keys(written, spec.contract) == 0, "the UC primary key is not unique"
    assert written.count() > 0, "an empty table satisfies any contract"


@pytest.mark.parametrize("spec", GOVERNED, ids=lambda s: s.name)
def test_every_declared_check_reaches_the_written_table(
    built: Path, spark: SparkSession, spec: FeatureTableSpec
) -> None:
    """Declaring a check and never applying it is the failure mode this catches."""
    written = built / "features" / spec.name

    assert set(_constraints(spark, written)) == set(check_expressions(spec.contract))


def test_the_constraints_survive_a_full_recompute(built: Path, spark: SparkSession) -> None:
    """`run_features` overwrites every table every run, so a first-run-only
    constraint would quietly stop binding on run two."""
    features = built / "features"
    before = _constraints(spark, features / "repo_activity")

    run_features(
        spark, silver_path=str(built / "silver"), features_path=str(features), register=False
    )

    assert _constraints(spark, features / "repo_activity") == before


def test_dropping_a_contracted_column_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    """The plan's own example breach, applied for real against the shipped writer."""
    spec = next(s for s in FEATURE_TABLES if s.name == "repo_activity")
    broken = replace(spec, compute=lambda events: spec.compute(events).drop("bot_share_to_date"))

    with pytest.raises(ContractError, match=r"repo_activity.bot_share_to_date is missing"):
        write_and_register(
            spark,
            broken,
            _silver_rows(spark),
            features_path=str(tmp_path / "features"),
            register=False,
            schema="features",
        )


def test_delta_alone_would_have_accepted_that_drop(spark: SparkSession, tmp_path: Path) -> None:
    """Why the check above is not redundant with Delta's schema enforcement.

    Measured on delta-spark 4.4.0: an overwrite missing a column is accepted,
    the table keeps the column, and every row's value becomes null. The schema
    still looks right; the data is gone; nothing raises.
    """
    path = str(tmp_path / "unguarded")
    spec = next(s for s in FEATURE_TABLES if s.name == "repo_activity")
    full = spec.compute(_silver_rows(spark))
    full.write.format("delta").save(path)

    full.drop("bot_share_to_date").write.format("delta").mode("overwrite").save(path)

    reread = spark.read.format("delta").load(path)
    assert "bot_share_to_date" in reread.columns, "the column is still in the schema"
    assert reread.where(F.col("bot_share_to_date").isNotNull()).count() == 0, "and always null"


def test_widening_a_key_type_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    spec = next(s for s in FEATURE_TABLES if s.name == "repo_activity")
    broken = replace(
        spec,
        compute=lambda events: spec.compute(events).withColumn(
            "repo_id", F.col("repo_id").cast("string")
        ),
    )

    with pytest.raises(ContractError, match="repo_id is string, contracted as bigint"):
        write_and_register(
            spark,
            broken,
            _silver_rows(spark),
            features_path=str(tmp_path / "features"),
            register=False,
            schema="features",
        )


def test_a_null_key_is_refused_by_the_written_table(built: Path, spark: SparkSession) -> None:
    """The rule `ALTER COLUMN ... SET NOT NULL` cannot express here, enforced anyway."""
    path = str(built / "features" / "repo_activity")
    row = spark.read.format("delta").load(path).limit(1)

    with pytest.raises(Exception, match="repo_id_not_null"):
        row.withColumn("repo_id", F.lit(None).cast("bigint")).write.format("delta").mode(
            "append"
        ).save(path)


def test_a_backwards_elapsed_time_is_refused_by_the_written_table(
    built: Path, spark: SparkSession
) -> None:
    """Point-in-time discipline as a table constraint, not only as a code comment."""
    path = str(built / "features" / "repo_stream_activity")
    row = (
        spark.read.format("delta")
        .load(path)
        .where(F.col("secs_since_last_event").isNotNull())
        .limit(1)
    )
    assert row.count() == 1, "the fixture must contain a repo with prior history"

    with pytest.raises(Exception, match="elapsed_never_negative"):
        row.withColumn("secs_since_last_event", F.lit(-1.0)).write.format("delta").mode(
            "append"
        ).save(path)


def test_a_check_retired_from_the_contract_is_removed_from_the_table(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A rule nobody enforces any more must not linger as one the table still applies.

    On its own table rather than a shared one: this is the only test here that
    changes what it reads.
    """
    path = str(tmp_path / "retiring")
    contract = Contract(surface="retiring", columns={"k": "bigint"}, keys=("k",))
    spark.createDataFrame([(1,)], "k long").write.format("delta").save(path)

    apply_constraints(spark, path, replace(contract, checks={"temporary": "k > 0"}))
    assert "temporary" in _constraints(spark, Path(path))

    apply_constraints(spark, path, contract)
    assert "temporary" not in _constraints(spark, Path(path))
    assert "k_not_null" in _constraints(spark, Path(path)), "the key rule stays"


def test_streaming_silver_refuses_a_frame_missing_a_silver_column(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Checked before the query starts, so a breach leaves no checkpoint behind."""
    landing = tmp_path / "landing"
    landing.mkdir()
    land_poll(landing, [_EVENT], polled_at="2026-09-06T12:00:20Z", name="poll_0")
    checkpoint = tmp_path / "checkpoint"

    stream = stream_events(spark, str(landing)).drop("is_late")

    with pytest.raises(ContractError, match=r"stream_silver.is_late is missing"):
        write_stream_silver(stream, str(tmp_path / "dest"), str(checkpoint))
    assert not checkpoint.exists(), "a refused write must not leave a checkpoint"


def test_the_live_silver_table_carries_its_contract(spark: SparkSession, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    land_poll(landing, [_EVENT], polled_at="2026-09-06T12:00:20Z", name="poll_0")
    dest = tmp_path / "dest"

    run_streaming_query(
        write_stream_silver(
            stream_events(spark, str(landing)),
            str(dest),
            str(tmp_path / "checkpoint"),
            available_now=True,
        )
    )

    assert set(_constraints(spark, dest)) == set(check_expressions(STREAM_SILVER_CONTRACT))
