from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

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
