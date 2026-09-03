"""``spark_path``: which scheme Spark needs for a staged file, by where it lives."""

from __future__ import annotations

from pathlib import Path

from almanac.burn.day import spark_path


def test_a_unity_catalog_volume_path_stays_bare() -> None:
    """Spark resolves /Volumes through the Unity Catalog filesystem. file:/ sends
    it to the driver's local root instead, where the file the fetch just wrote
    through the FUSE mount is invisible to the JVM -- the real failure on
    2026-09-02, after moving staging off local disk fixed the previous one."""
    volume_file = Path("/Volumes/almanac_dbx/burn/staging/2025-07-01-0.json.gz")

    assert spark_path(volume_file) == "/Volumes/almanac_dbx/burn/staging/2025-07-01-0.json.gz"


def test_a_real_local_disk_path_gets_a_file_uri() -> None:
    """The inverse failure, measured on the first real burn run: bare, this
    resolves against fs.defaultFS -- dbfs:/ on a cluster -- not the ephemeral
    disk the file was actually downloaded to."""
    local_file = Path("/local_disk0/staging/2025-07-01-0.json.gz")

    assert spark_path(local_file) == "file:///local_disk0/staging/2025-07-01-0.json.gz"


def test_a_directory_merely_named_like_the_volume_root_is_not_treated_as_one() -> None:
    """is_relative_to, not startswith: /Volumes_backup is a local directory."""
    lookalike = Path("/Volumes_backup/staging/2025-07-01-0.json.gz")

    assert spark_path(lookalike) == "file:///Volumes_backup/staging/2025-07-01-0.json.gz"
