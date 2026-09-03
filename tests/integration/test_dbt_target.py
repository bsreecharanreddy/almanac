"""The local dbt target is a correctness requirement, not configuration."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

CANARY_MODEL = "canary_incremental"


def _invoke_dbt(project_dir: Path, *, warehouse: Path, metastore: Path) -> None:
    """One `dbt run`, in its own process, through the shipped entry point."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "almanac.gold.runner",
            "--warehouse",
            str(warehouse),
            "--metastore",
            str(metastore),
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(Path("dbt").resolve()),
            "run",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"


def _delta_operations(spark: SparkSession, table_path: Path) -> list[str]:
    """Every operation in the Delta commit log, oldest commit first."""
    history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").orderBy("version")
    return [row["operation"] for row in history.collect()]


@pytest.fixture
def canary_project(tmp_path: Path) -> Path:
    """The canary dbt project, copied so dbt's artifacts land in tmp."""
    destination = tmp_path / "canary"
    shutil.copytree(Path("tests/fixtures/dbt_canary"), destination)
    return destination


@pytest.mark.spark
@pytest.mark.integration
def test_gold_model_merges_on_second_run_rather_than_rebuilding(
    spark: SparkSession, canary_project: Path, tmp_path: Path
) -> None:
    """A silent CREATE OR REPLACE is the failure this test exists for."""
    warehouse, metastore = tmp_path / "warehouse", tmp_path / "metastore"

    _invoke_dbt(canary_project, warehouse=warehouse, metastore=metastore)
    _invoke_dbt(canary_project, warehouse=warehouse, metastore=metastore)

    operations = _delta_operations(spark, warehouse / "gold.db" / CANARY_MODEL)

    assert operations[0] == "CREATE OR REPLACE TABLE AS SELECT"
    assert "MERGE" in operations[1:], (
        f"second run did not merge; operations were {operations}. "
        "An ephemeral metastore makes dbt rebuild the model in silence."
    )


@pytest.mark.integration
def test_the_shipped_dbt_project_parses_under_the_session_target(tmp_path: Path) -> None:
    """`dbt parse` resolves the real project, profile, and source declarations."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "almanac.gold.runner",
            "--warehouse",
            str(tmp_path / "warehouse"),
            "--metastore",
            str(tmp_path / "metastore"),
            "--target-path",
            str(tmp_path / "target"),
            "parse",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"dbt parse failed:\n{result.stdout}\n{result.stderr}"

    manifest = json.loads((tmp_path / "target" / "manifest.json").read_text())
    assert "source.almanac.silver.events" in manifest["sources"]
