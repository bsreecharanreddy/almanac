"""``GoldTarget.cli_flags``: the defaults a job cluster cannot resolve."""

from __future__ import annotations

from pathlib import Path

import pytest

from almanac.gold.runner import (
    DEFAULT_PROJECT_DIR,
    SILVER_SCHEMA_ENV,
    GoldTarget,
    default_source_schema,
)


def _flag(flags: list[str], name: str) -> str:
    return flags[flags.index(name) + 1]


def test_profiles_dir_does_not_follow_project_dir() -> None:
    """The footgun: setting only project_dir leaves --profiles-dir on the
    relative default, which resolves against a job task's cwd -- not the repo
    root. Same shape as defect #3's --source-config. Callers pass both."""
    flags = GoldTarget(
        warehouse="/w", metastore=Path("/m"), project_dir=Path("/workspace/dbt")
    ).cli_flags()

    assert _flag(flags, "--project-dir") == "/workspace/dbt"
    assert _flag(flags, "--profiles-dir") == str(DEFAULT_PROJECT_DIR)


def test_both_dirs_are_absolute_when_both_are_passed() -> None:
    """What every job task must actually do."""
    flags = GoldTarget(
        warehouse="/w",
        metastore=Path("/m"),
        project_dir=Path("/workspace/dbt"),
        profiles_dir=Path("/workspace/dbt"),
    ).cli_flags()

    assert Path(_flag(flags, "--project-dir")).is_absolute()
    assert Path(_flag(flags, "--profiles-dir")).is_absolute()


def test_the_default_project_dir_is_relative() -> None:
    """Pins why the above matters -- correct locally, unresolvable on a cluster."""
    assert not DEFAULT_PROJECT_DIR.is_absolute()


def test_the_source_schema_defaults_to_silver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every local and CI run must keep the plain name."""
    monkeypatch.delenv(SILVER_SCHEMA_ENV, raising=False)
    assert default_source_schema() == "silver"
    assert GoldTarget(warehouse="/w", metastore=Path("/m")).source_schema == "silver"


def test_the_source_schema_follows_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two concurrent A/B arms isolate by setting this one variable; dbt's
    sources.yml reads the same one, so the name is stated once."""
    monkeypatch.setenv(SILVER_SCHEMA_ENV, "ab_photon_silver")
    assert GoldTarget(warehouse="/w", metastore=Path("/m")).source_schema == "ab_photon_silver"


def test_an_explicit_source_schema_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """The env var is a default, not an override."""
    monkeypatch.setenv(SILVER_SCHEMA_ENV, "ab_photon_silver")
    target = GoldTarget(warehouse="/w", metastore=Path("/m"), source_schema="explicit")
    assert target.source_schema == "explicit"
