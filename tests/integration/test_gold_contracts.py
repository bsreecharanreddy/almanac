"""Gold's contracts and referential tests fail the *build*, not a document."""

import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, repo_name string, event_id string, pr_number long, "
    "created_at timestamp, actor_login string, ingested_at timestamp, "
    "event_type string, event_action string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, push_size long, push_distinct_size long"
)

T = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)


def _silver_rows(spark: SparkSession, clean: Path, quarantine: Path) -> None:
    spark.createDataFrame([], _SILVER_SCHEMA).write.format("delta").save(str(quarantine))
    rows = [
        (
            5,
            "o/r",
            "e1",
            1,
            T,
            "ann",
            T,
            "PullRequestEvent",
            "opened",
            None,
            False,
            None,
            None,
            None,
        ),
        (
            5,
            "o/r",
            "e2",
            1,
            datetime(2025, 8, 13, 12, 0, tzinfo=UTC),
            "bo",
            T,
            "PullRequestReviewEvent",
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (5, "o/r", "e3", 0, T, "ann", T, "WatchEvent", None, None, None, None, None, None),
    ]
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").mode("append").save(
        str(clean)
    )


def _build(project: Path, tmp_path: Path, silver: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "almanac.gold.runner",
            "--warehouse",
            str(tmp_path / "warehouse"),
            "--metastore",
            str(tmp_path / "metastore"),
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            str(tmp_path / "target"),
            "--silver-path",
            str(silver),
            "build",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def project_and_silver(spark: SparkSession, tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "dbt"
    shutil.copytree("dbt", project)
    silver = tmp_path / "silver"
    _silver_rows(spark, silver / "clean", silver / "quarantine")
    return project, silver


def test_the_shipped_project_builds_clean(
    project_and_silver: tuple[Path, Path], tmp_path: Path
) -> None:
    """The baseline the two break-it tests below are measured against."""
    project, silver = project_and_silver
    result = _build(project, tmp_path, silver)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"


def test_a_wrong_output_type_fails_the_contract(
    project_and_silver: tuple[Path, Path], tmp_path: Path
) -> None:
    """`repo_id` is contracted as `bigint`; emitting a string fails the build."""
    project, silver = project_and_silver
    model = project / "models" / "gold" / "fact_pull_request.sql"
    broken = model.read_text().replace(
        "        assembled.repo_id,\n        assembled.pr_number,\n        assembled.opened_at,",
        "        cast(assembled.repo_id as string) as repo_id,\n"
        "        assembled.pr_number,\n        assembled.opened_at,",
        1,
    )
    assert broken != model.read_text(), "mutation anchor not found"
    model.write_text(broken)

    result = _build(project, tmp_path, silver)
    assert result.returncode != 0, "a contract violation must redden the build"
    assert "contract" in (result.stdout + result.stderr).lower()


def test_a_fact_row_with_no_dim_repo_fails_relationships(
    project_and_silver: tuple[Path, Path], tmp_path: Path
) -> None:
    """A `fact_pull_request` row with no matching `dim_repo` fails the relationships test."""
    project, silver = project_and_silver
    snapshot = project / "snapshots" / "dim_repo.sql"
    snapshot.write_text(
        snapshot.read_text().replace(
            "where repo_id is not null", "where repo_id is not null and repo_id <> 5"
        )
    )

    result = _build(project, tmp_path, silver)
    assert result.returncode != 0, "a dangling fact reference must redden the build"
    assert "relationships" in (result.stdout + result.stderr).lower()
