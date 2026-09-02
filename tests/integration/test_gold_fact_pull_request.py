"""`fact_pull_request`'s accumulating-snapshot behaviour, across real dbt runs.

Same shape as `test_gold_dim_repo.py`: several separate processes against
one persistent warehouse/metastore, each appending more of the event
stream to `silver.events` and rerunning `dbt build`. A snapshot's premise
is that every run reconciles against whatever the source now holds, so the
test has to actually rerun it rather than inspecting one build.

The defining property is that **out-of-order arrival preserves the
earliest timestamp** -- the `opened` event for a PR can land in a later
file than its `closed` event, and a plain `MERGE ... UPDATE SET` would
overwrite the real `opened_at` with a null. `least()` over the batch and
the stored row is what makes that safe, and the mutation that removes it
reddens `test_out_of_order_open_after_close_keeps_both_timestamps`.

Only the handful of Silver columns the model selects are written here;
`test_pipeline.py` already covers Silver deriving them.
"""

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "ingested_at timestamp"
)

_Event = tuple[int, int, datetime, str, str | None, str | None, bool | None, bool | None, datetime]


def _event(
    repo_id: int,
    pr_number: int,
    created_at: datetime,
    ingested_at: datetime,
    *,
    event_type: str,
    action: str | None = None,
    actor: str | None = None,
    merged: bool | None = None,
    draft: bool | None = None,
) -> _Event:
    """One Silver row in the shape `fact_pull_request` reads."""
    return (
        repo_id,
        pr_number,
        created_at,
        event_type,
        action,
        actor,
        merged,
        draft,
        ingested_at,
    )


def _append_silver(spark: SparkSession, path: Path, rows: list[_Event]) -> None:
    """More of the archive landing -- `append`, never a replacement."""
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").mode("append").save(str(path))


def _build_gold(paths: dict[str, Path]) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "almanac.gold.runner",
            "--warehouse",
            str(paths["warehouse"]),
            "--metastore",
            str(paths["metastore"]),
            "--target-path",
            str(paths["target_path"]),
            "--silver-path",
            str(paths["silver_path"]),
            "build",
            "--select",
            "fact_pull_request",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}\n{result.stderr}"


_TS_COLUMNS = ("opened_at", "closed_at", "first_review_at")


def _fact(spark: SparkSession, warehouse: Path) -> DataFrame:
    """The fact, with every timestamp column projected to an epoch second.

    No test in this repo asserts on a collected datetime: PySpark hands one
    back naive in the driver's timezone, so the comparison passes or fails
    by which machine runs it (Phase 1 Task 2, `tests/helpers.epoch_of`). An
    epoch is an instant and carries no ambiguity.
    """
    raw = spark.read.format("delta").load(str(warehouse / "gold.db" / "fact_pull_request"))
    ts = [f"unix_timestamp({c}) as {c}" for c in _TS_COLUMNS]
    others = [c for c in raw.columns if c not in _TS_COLUMNS]
    return raw.selectExpr(*others, *ts)


def epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def _one(df: DataFrame, repo_id: int, pr_number: int) -> Row:
    rows = df.filter((df.repo_id == repo_id) & (df.pr_number == pr_number)).collect()
    assert len(rows) == 1, f"expected exactly one row for ({repo_id}, {pr_number}), got {rows}"
    return rows[0]


@pytest.fixture
def gold_paths(tmp_path: Path) -> dict[str, Path]:
    silver = tmp_path / "silver"
    return {
        "warehouse": tmp_path / "warehouse",
        "metastore": tmp_path / "metastore",
        "target_path": tmp_path / "target",
        "silver_path": silver,
        "clean_path": silver / "clean",
        "quarantine_path": silver / "quarantine",
    }


@pytest.fixture
def empty_quarantine(spark: SparkSession, gold_paths: dict[str, Path]) -> None:
    """`register_silver_sources` registers both tables together; the fact
    reads only `clean`, but the quarantine location still has to exist."""
    spark.createDataFrame([], _SILVER_SCHEMA).write.format("delta").save(
        str(gold_paths["quarantine_path"])
    )


T0 = datetime(2025, 8, 13, 0, 0, tzinfo=UTC)
T1 = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
T2 = datetime(2025, 8, 15, 0, 0, tzinfo=UTC)


def test_out_of_order_open_after_close_keeps_both_timestamps(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """The `opened` event arrives in a later batch than `closed`, at an
    earlier event time. Both timestamps must survive."""
    c_close = datetime(2025, 8, 13, 15, 0, tzinfo=UTC)
    c_open = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [_event(700, 1, c_close, T0, event_type="PullRequestEvent", action="closed", merged=True)],
    )
    _build_gold(gold_paths)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [_event(700, 1, c_open, T1, event_type="PullRequestEvent", action="opened", actor="alice")],
    )
    _build_gold(gold_paths)

    row = _one(_fact(spark, gold_paths["warehouse"]), 700, 1)
    assert row["opened_at"] == epoch(c_open)
    assert row["closed_at"] == epoch(c_close), "a later null batch must not clobber closed_at"
    assert row["author_login"] == "alice"
    assert row["merged"] is True


def test_one_row_per_pr_accumulates_every_lifecycle_column(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """opened, first review, and closed each land in a different run; the PR
    ends as exactly one row carrying all three."""
    c_open = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)
    c_review_late = datetime(2025, 8, 13, 18, 0, tzinfo=UTC)
    c_review_early = datetime(2025, 8, 13, 12, 0, tzinfo=UTC)
    c_close = datetime(2025, 8, 14, 10, 0, tzinfo=UTC)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(
                701,
                5,
                c_open,
                T0,
                event_type="PullRequestEvent",
                action="opened",
                actor="bob",
                draft=False,
            ),
            _event(701, 5, c_review_late, T0, event_type="PullRequestReviewEvent", actor="carol"),
        ],
    )
    _build_gold(gold_paths)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(701, 5, c_review_early, T1, event_type="PullRequestReviewEvent", actor="dave"),
            _event(
                701, 5, c_close, T1, event_type="PullRequestEvent", action="closed", merged=True
            ),
        ],
    )
    _build_gold(gold_paths)

    fact = _fact(spark, gold_paths["warehouse"])
    assert fact.filter((fact.repo_id == 701) & (fact.pr_number == 5)).count() == 1
    row = _one(fact, 701, 5)
    assert row["opened_at"] == epoch(c_open)
    assert row["first_review_at"] == epoch(c_review_early), "earliest review across all batches"
    assert row["closed_at"] == epoch(c_close)
    assert row["author_login"] == "bob"
    assert row["draft"] is False
    assert row["is_censored"] is False


def test_a_pr_open_at_the_window_edge_is_flagged_censored(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """No terminal event observed -> `is_censored`, never a fabricated close."""
    c_open = datetime(2025, 8, 15, 9, 0, tzinfo=UTC)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(
                702, 9, c_open, T2, event_type="PullRequestEvent", action="opened", actor="erin"
            ),
        ],
    )
    _build_gold(gold_paths)

    row = _one(_fact(spark, gold_paths["warehouse"]), 702, 9)
    assert row["closed_at"] is None
    assert row["is_censored"] is True


def test_second_build_merges_rather_than_rebuilding(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """Phase 2 exit gate: a Gold model's second run is a `MERGE`, read from
    the Delta commit log -- not the row counts, which stayed correct in the
    silent-rebuild failure the Task 2 spike found."""
    c_open = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)
    row = [_event(703, 2, c_open, T0, event_type="PullRequestEvent", action="opened", actor="ivan")]

    _append_silver(spark, gold_paths["clean_path"], row)
    _build_gold(gold_paths)
    _append_silver(spark, gold_paths["clean_path"], row)  # same event again, later batch
    _build_gold(gold_paths)

    table = str(gold_paths["warehouse"] / "gold.db" / "fact_pull_request")
    ops = [
        r["operation"]
        for r in spark.sql(f"describe history delta.`{table}`").orderBy("version").collect()
    ]
    assert ops[0] == "CREATE OR REPLACE TABLE AS SELECT"
    assert "MERGE" in ops[1:], f"second build did not merge; ops were {ops}"
    assert _fact(spark, gold_paths["warehouse"]).filter(F.col("repo_id") == 703).count() == 1
