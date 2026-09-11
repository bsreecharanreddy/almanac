"""The demo build step: fixtures in, committed artifacts out."""

import json
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from almanac.demo.build import ERAS, build_medallion, build_queue
from almanac.model.train import FEATURE_COLUMNS

pytestmark = pytest.mark.spark

_IDENTITY_COLUMNS = ("actor_login", "author_login", "repo_name", "repo_full_name", "org_login")


def test_it_lands_all_three_schema_eras(spark: SparkSession, tmp_path: Path) -> None:
    """Three eras, because the schema break is what the medallion panel exists to show."""
    summary = build_medallion(spark, out_dir=tmp_path)
    assert len(ERAS) == 3
    assert {era["event_date"] for era in summary["eras"]} == {e[1] for e in ERAS}


def test_the_split_is_asserted_never_assumed(spark: SparkSession, tmp_path: Path) -> None:
    """valid + quarantine == scored, the repo's standing rule for every quality split."""
    summary = build_medallion(spark, out_dir=tmp_path)
    for era in summary["eras"]:
        assert era["silver_rows"] + era["quarantine_rows"] == era["scored_rows"]


def test_it_writes_a_readable_artifact(spark: SparkSession, tmp_path: Path) -> None:
    build_medallion(spark, out_dir=tmp_path)
    payload = json.loads((tmp_path / "medallion.json").read_text())
    assert payload["scale"]["hours_per_era"] == 1
    assert payload["eras"]


def test_the_queue_carries_no_identity_column(spark: SparkSession, tmp_path: Path) -> None:
    """docs/pseudonymization.md: logins and owner/repo are never published."""
    build_medallion(spark, out_dir=tmp_path)
    build_queue(spark, out_dir=tmp_path)
    frame = pd.read_parquet(tmp_path / "queue.parquet")
    for column in _IDENTITY_COLUMNS:
        assert column not in frame.columns, f"{column} must not reach a published artifact"


def test_every_row_is_scored_and_the_score_is_a_probability(
    spark: SparkSession, tmp_path: Path
) -> None:
    build_medallion(spark, out_dir=tmp_path)
    build_queue(spark, out_dir=tmp_path)
    frame = pd.read_parquet(tmp_path / "queue.parquet")
    assert len(frame) > 0
    assert frame["breach_risk"].between(0.0, 1.0).all()


def test_coverage_reports_a_null_count_for_every_feature(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The sparsity is the point-in-time invariant made visible, so it is measured."""
    build_medallion(spark, out_dir=tmp_path)
    build_queue(spark, out_dir=tmp_path)
    coverage = json.loads((tmp_path / "coverage.json").read_text())
    assert set(coverage["features"]) == set(FEATURE_COLUMNS)
    assert coverage["total_rows"] > 0
    for column in FEATURE_COLUMNS:
        assert 0 <= coverage["features"][column]["non_null"] <= coverage["total_rows"]
