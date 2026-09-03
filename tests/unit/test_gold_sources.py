"""``table_location``: a LOCATION is a URI, not a filesystem path."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from pyspark.sql import SparkSession

from almanac.gold.sources import register_silver_sources, table_location

# Every scheme the lake actually addresses, plus one it does not, so a future
# reader can see the rule is "has a scheme", not "starts with abfss".
URIS = [
    "abfss://silver@almanaclake.dfs.core.windows.net/events/clean",
    "s3://bucket/events/clean",
    "gs://bucket/events/clean",
]


@pytest.mark.parametrize("uri", URIS)
def test_a_storage_uri_is_returned_untouched(uri: str) -> None:
    """The defect: Path collapsed '//' and Spark answered 'Missing cloud file
    system scheme', killing the A/B arm at the Gold step -- measured 2026-09-02."""
    assert table_location(uri) == uri


@pytest.mark.parametrize("uri", URIS)
def test_pathlib_would_have_corrupted_each_of_them(uri: str) -> None:
    """Pins *why* the str route exists, so nobody reintroduces Path here."""
    assert str(Path(uri)) != uri


def test_a_relative_local_path_is_made_absolute() -> None:
    """The invariant the old resolve() protected: a relative LOCATION on a
    non-default schema nests under that schema's managed dir and points at nothing."""
    assert Path(table_location("data/warehouse")).is_absolute()


def test_an_absolute_local_path_survives(tmp_path: Path) -> None:
    assert table_location(str(tmp_path)) == str(tmp_path)


def test_a_fuse_volume_path_survives() -> None:
    """/Volumes/... has no '://' but is already absolute, so it must pass through."""
    assert table_location("/Volumes/almanac_dbx/burn/staging") == (
        "/Volumes/almanac_dbx/burn/staging"
    )


class _SqlSpy:
    """Records statements instead of executing them; no cluster, no credentials."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_the_registered_location_keeps_the_scheme_intact() -> None:
    """The end-to-end shape of the defect, asserted on the SQL Spark would receive."""
    spy = _SqlSpy()
    base = "abfss://silver@almanaclake.dfs.core.windows.net/events"

    register_silver_sources(
        cast(SparkSession, spy), clean_path=f"{base}/clean", quarantine_path=f"{base}/quarantine"
    )

    locations = [s for s in spy.statements if "LOCATION" in s]
    assert len(locations) == 2
    assert f"LOCATION '{base}/clean'" in locations[0]
    assert f"LOCATION '{base}/quarantine'" in locations[1]
    assert "abfss:/silver" not in " ".join(locations)
