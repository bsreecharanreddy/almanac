import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.config import Settings
from almanac.spark import local_session

# MLflow 3.x refuses the local file store unless this is set. Phase 4's
# tests exercise the logging *contract* against a hermetic file:// store
# (per-test tmp_path); the real backend is Databricks, reached via the
# runner's `--tracking-uri databricks` and exercised only in Task 9's
# cloud step. sqlite would buy nothing here but an artifact-root to manage.
os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One session for the whole test run.

    Creating a session per test is the single most common reason a Spark
    suite becomes unusably slow.
    """
    session = local_session("almanac-tests")
    yield session
    session.stop()


def _one_fixture(pattern: str) -> Path:
    matches = sorted(Settings().fixture_dir.glob(pattern))
    if not matches:
        pytest.skip(f"no fixture matching {pattern}; run `make fixtures`")
    return matches[0]


@pytest.fixture(scope="session")
def modern_events_path() -> Path:
    return _one_fixture("modern-*.jsonl.gz")


@pytest.fixture(scope="session")
def legacy_events_path() -> Path:
    return _one_fixture("legacy-*.jsonl.gz")


@pytest.fixture(scope="session")
def reduced_events_path() -> Path:
    """Post-2025-10-15 payload reduction (SchemaEra.REDUCED_V3) -- the only
    committed fixture streaming's own era guard can accept."""
    return _one_fixture("reduced-*.jsonl.gz")
