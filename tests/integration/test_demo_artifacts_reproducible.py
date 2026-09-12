"""The committed artifacts regenerate byte-identically, or the suite fails.

The same invariant the feature platform is built on: same inputs, same Delta
version, byte-identical output, a year later. Applied here so a stale demo
artifact fails the build instead of shipping quietly -- this repo has nine
recorded instances of a hand-maintained record outliving its truth.
"""

import hashlib
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.demo.artifacts import DEMO_DATA_DIR
from almanac.demo.build import build_medallion, build_queue

pytestmark = pytest.mark.spark

_COMMITTED = ("medallion.json", "coverage.json", "queue.parquet")


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_a_rebuild_reproduces_every_committed_artifact(spark: SparkSession, tmp_path: Path) -> None:
    build_medallion(spark, out_dir=tmp_path)
    build_queue(spark, out_dir=tmp_path)
    for name in _COMMITTED:
        committed = DEMO_DATA_DIR / name
        assert committed.is_file(), f"{name} is not committed; run `make demo-build`"
        assert _digest(tmp_path / name) == _digest(committed), (
            f"{name} differs from the committed copy -- rerun `make demo-build` "
            f"and commit the result, or explain why the inputs changed"
        )
