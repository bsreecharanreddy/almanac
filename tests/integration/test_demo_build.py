"""The demo build step: fixtures in, committed artifacts out."""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.demo.build import ERAS, build_medallion

pytestmark = pytest.mark.spark


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
