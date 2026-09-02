"""`dim_repo`'s SCD2 lifecycle, across real dbt invocations. No network.

Three separate processes against the same persistent warehouse/metastore,
each appending more rows to a real Silver Delta table and rerunning
`dbt build`. That is deliberate, not convenient: a snapshot's whole
premise is that each invocation sees whatever the source currently holds,
so the test has to actually rerun it against changed data rather than
asserting on one build's output.

This one test proves all four of Task 3's invariants at once, because
they are one scenario, not four:
  1. a rename closes the old row and opens exactly one current row
  2. exactly one `dbt_valid_to IS NULL` per `repo_id`, always
  3. a case-only rename (`GLB` -> `glb`) is detected
  4. a repo renamed twice yields three versions, not two

Invariant 2 is also enforced by `dbt/tests/assert_dim_repo_scd2.sql` as
part of `build` itself -- a real regression there fails the subprocess
(nonzero exit) before this test ever inspects a row, which is the primary
safety net. The row-level assertions below exist so a failure says *what*
went wrong, not just *that* something did.

Gold's own tests exercise the Gold/Silver *contract* -- the handful of
named columns `dim_repo` actually selects -- rather than re-deriving
Silver's own pipeline, which `test_pipeline.py` already covers. Only those
columns are written here.
"""

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = "repo_id long, repo_name string, created_at timestamp, event_id string"

RENAMED_REPO_ID = 501
CONTROL_REPO_ID = 502


_Row = tuple[int, str, datetime, str]


def _write_silver_events(spark: SparkSession, path: Path, rows: list[_Row]) -> None:
    """Append rows to Gold's view of `silver.events`.

    `append`, always: each call simulates more of the archive landing,
    never a replacement of what a previous run already saw.
    """
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").mode("append").save(str(path))


def _build_gold(*, warehouse: Path, metastore: Path, target_path: Path, silver_path: Path) -> None:
    """One `dbt build`, in its own process, against the real (not a copy)
    `dbt/` project -- unlike the Task 2 canary, this test is exercising
    our actual `dim_repo` snapshot and our actual singular test, both of
    which live there.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "almanac.gold.runner",
            "--warehouse",
            str(warehouse),
            "--metastore",
            str(metastore),
            "--target-path",
            str(target_path),
            "--silver-path",
            str(silver_path),
            "build",
            "--select",
            "dim_repo+",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}\n{result.stderr}"


def _current_row(df: DataFrame, repo_id: int) -> Row:
    rows = df.filter((df.repo_id == repo_id) & df.dbt_valid_to.isNull()).collect()
    assert len(rows) == 1, f"expected exactly one current row for repo {repo_id}, got {rows}"
    return rows[0]


def _all_rows(df: DataFrame, repo_id: int) -> list[Row]:
    return df.filter(df.repo_id == repo_id).collect()


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


def test_dim_repo_scd2_lifecycle_across_reruns(
    spark: SparkSession, gold_paths: dict[str, Path]
) -> None:
    t0 = datetime(2025, 8, 13, 0, 0, tzinfo=UTC)
    t1 = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 8, 15, 0, 0, tzinfo=UTC)

    # An empty but valid Delta table: `register_silver_sources` registers
    # both tables together, and dim_repo reads neither of the columns
    # below, but the quarantine location still has to exist.
    _write_silver_events(spark, gold_paths["quarantine_path"], [])

    # --- Run 1: initial state -- two repos, neither renamed yet.
    _write_silver_events(
        spark,
        gold_paths["clean_path"],
        [
            (RENAMED_REPO_ID, "acme/glb", t0, "e1"),
            (CONTROL_REPO_ID, "acme/other", t0, "e2"),
        ],
    )
    _build_gold(
        warehouse=gold_paths["warehouse"],
        metastore=gold_paths["metastore"],
        target_path=gold_paths["target_path"],
        silver_path=gold_paths["silver_path"],
    )

    dim_repo = spark.read.format("delta").load(
        str(gold_paths["warehouse"] / "gold.db" / "dim_repo")
    )
    renamed = _current_row(dim_repo, RENAMED_REPO_ID)
    assert renamed["repo_name"] == "acme/glb"
    control = _current_row(dim_repo, CONTROL_REPO_ID)
    assert control["repo_name"] == "acme/other"

    # --- Run 2: a case-only rename on one repo, the other untouched.
    _write_silver_events(spark, gold_paths["clean_path"], [(RENAMED_REPO_ID, "acme/GLB", t1, "e3")])
    _build_gold(
        warehouse=gold_paths["warehouse"],
        metastore=gold_paths["metastore"],
        target_path=gold_paths["target_path"],
        silver_path=gold_paths["silver_path"],
    )

    dim_repo = spark.read.format("delta").load(
        str(gold_paths["warehouse"] / "gold.db" / "dim_repo")
    )
    renamed_versions = _all_rows(dim_repo, RENAMED_REPO_ID)
    assert len(renamed_versions) == 2, "a rename must add a version, not overwrite one"
    closed = [r for r in renamed_versions if r["dbt_valid_to"] is not None]
    assert len(closed) == 1
    assert closed[0]["repo_name"] == "acme/glb", "the old row must keep its original case"
    current = _current_row(dim_repo, RENAMED_REPO_ID)
    assert current["repo_name"] == "acme/GLB", "case-only rename must be detected, not folded away"

    # The untouched repo must still have exactly one current row, unchanged.
    control = _current_row(dim_repo, CONTROL_REPO_ID)
    assert control["repo_name"] == "acme/other"
    assert len(_all_rows(dim_repo, CONTROL_REPO_ID)) == 1

    # --- Run 3: a second rename on the same repo.
    _write_silver_events(
        spark, gold_paths["clean_path"], [(RENAMED_REPO_ID, "acme/final", t2, "e4")]
    )
    _build_gold(
        warehouse=gold_paths["warehouse"],
        metastore=gold_paths["metastore"],
        target_path=gold_paths["target_path"],
        silver_path=gold_paths["silver_path"],
    )

    dim_repo = spark.read.format("delta").load(
        str(gold_paths["warehouse"] / "gold.db" / "dim_repo")
    )
    renamed_versions = _all_rows(dim_repo, RENAMED_REPO_ID)
    assert len(renamed_versions) == 3, "a double rename must yield three versions, not two"
    current = _current_row(dim_repo, RENAMED_REPO_ID)
    assert current["repo_name"] == "acme/final"
    assert len(_all_rows(dim_repo, CONTROL_REPO_ID)) == 1, "unrelated repo must stay untouched"
