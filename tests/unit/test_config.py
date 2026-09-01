from pathlib import Path

from almanac.config import Settings


def test_settings_have_archive_base_url() -> None:
    s = Settings()
    assert s.archive_base_url == "https://data.gharchive.org"


def test_settings_paths_are_paths() -> None:
    s = Settings()
    assert isinstance(s.data_dir, Path)
    assert isinstance(s.fixture_dir, Path)


def test_fetch_attempts_is_at_least_one() -> None:
    # A zero here would silently disable fetching altogether.
    assert Settings().max_fetch_attempts >= 1
