"""`agg_repo_daily` -- daily activity per repo, across real dbt runs."""

import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, created_at timestamp, ingested_at timestamp, actor_login string, "
    "event_type string, event_action string, push_size long, push_distinct_size long"
)

_Event = tuple[int, datetime, datetime, str | None, str, str | None, int | None, int | None]


def _event(
    repo_id: int,
    created_at: datetime,
    ingested_at: datetime,
    *,
    event_type: str,
    action: str | None = None,
    actor: str | None = "human",
    push_size: int | None = None,
    push_distinct_size: int | None = None,
) -> _Event:
    return (
        repo_id,
        created_at,
        ingested_at,
        actor,
        event_type,
        action,
        push_size,
        push_distinct_size,
    )


def _append_silver(spark: SparkSession, path: Path, rows: list[_Event]) -> None:
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
            "agg_repo_daily",
            # cautious: don't run the fact -> dim_repo relationships test,
            # whose other parent this partial build does not include.
            "--indirect-selection",
            "cautious",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}\n{result.stderr}"


def _agg(spark: SparkSession, warehouse: Path) -> DataFrame:
    return spark.read.format("delta").load(str(warehouse / "gold.db" / "agg_repo_daily"))


def _one(df: DataFrame, repo_id: int, activity_date: date) -> Row:
    rows = df.filter((df.repo_id == repo_id) & (df.activity_date == activity_date)).collect()
    assert len(rows) == 1, f"expected one row for ({repo_id}, {activity_date}), got {rows}"
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
    spark.createDataFrame([], _SILVER_SCHEMA).write.format("delta").save(
        str(gold_paths["quarantine_path"])
    )


DAY = datetime(2025, 8, 13, 12, 0, tzinfo=UTC)
NEXT_DAY = datetime(2025, 8, 14, 12, 0, tzinfo=UTC)
T0 = datetime(2025, 8, 13, 0, 0, tzinfo=UTC)
T1 = datetime(2025, 8, 15, 0, 0, tzinfo=UTC)


def test_stars_are_gained_not_totalled_and_pushes_use_payload_size(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(10, DAY, T0, event_type="WatchEvent"),
            _event(10, DAY, T0, event_type="WatchEvent"),
            _event(10, DAY, T0, event_type="ForkEvent"),
            # One push of 1,000 commits: the `commits` array would cap at 20.
            _event(10, DAY, T0, event_type="PushEvent", push_size=1000, push_distinct_size=950),
            _event(10, DAY, T0, event_type="PushEvent", push_size=3, push_distinct_size=3),
            _event(10, DAY, T0, event_type="PullRequestEvent", action="opened"),
            _event(10, DAY, T0, event_type="ForkEvent", actor="renovate[bot]"),
        ],
    )
    _build_gold(gold_paths)

    row = _one(_agg(spark, gold_paths["warehouse"]), 10, date(2025, 8, 13))
    assert row["stars_gained"] == 2
    assert row["forks"] == 2
    assert row["prs_opened"] == 1
    assert row["pushes"] == 2
    assert row["commits_pushed"] == 1003, "sum(payload.size), not size(commits)"
    assert row["distinct_commits_pushed"] == 953
    assert row["bot_events"] == 1
    assert row["events_total"] == 7


def test_daily_grain_and_out_of_order_events_recompute_not_double_count(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    # Batch 1: one day.
    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(20, DAY, T0, event_type="WatchEvent"),
            _event(20, NEXT_DAY, T0, event_type="ForkEvent"),
        ],
    )
    _build_gold(gold_paths)

    # Batch 2: a late star for day 1, plus activity on a third day.
    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(20, DAY, T1, event_type="WatchEvent"),
            _event(20, datetime(2025, 8, 16, 9, 0, tzinfo=UTC), T1, event_type="WatchEvent"),
        ],
    )
    _build_gold(gold_paths)

    agg = _agg(spark, gold_paths["warehouse"])
    assert agg.filter(agg.repo_id == 20).count() == 3, "one row per repo per date"
    assert _one(agg, 20, date(2025, 8, 13))["stars_gained"] == 2, "late event corrected day 1"
    assert _one(agg, 20, date(2025, 8, 14))["forks"] == 1, "untouched day unchanged"
    assert _one(agg, 20, date(2025, 8, 16))["stars_gained"] == 1
