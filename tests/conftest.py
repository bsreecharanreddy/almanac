from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.config import Settings
from almanac.spark import local_session


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
