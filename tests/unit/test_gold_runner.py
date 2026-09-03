"""``GoldTarget.cli_flags``: the defaults a job cluster cannot resolve."""

from __future__ import annotations

from pathlib import Path

from almanac.gold.runner import DEFAULT_PROJECT_DIR, GoldTarget


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
