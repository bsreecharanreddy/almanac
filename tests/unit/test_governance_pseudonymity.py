"""§10: no actor identity in any published artifact -- enforced by this file.

The repo-wide scan at the bottom is the actual gate. The unit tests above it
exist so a change that quietly stops the scan from catching anything fails
loudly, rather than passing because it found nothing.
"""

import subprocess
from pathlib import Path

from almanac.governance.pseudonymity import (
    PUBLISHED_GLOBS,
    local_identifiers,
    published_files,
    scan,
    scan_text,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _planted(text: str) -> str:
    """Assemble a fixture address at runtime.

    This file became a scanned surface on 2026-09-09, so a literal address in
    it would be caught by the gate at the bottom -- correctly. Machine names
    below stay literal: that rule reads the running host, so a synthetic name
    can never match it.
    """
    return text.replace("(at)", "@")


def test_a_planted_email_is_caught() -> None:
    found = scan_text(_planted("contact me at real.person(at)gmail.com please"), identifiers=[])

    assert [(f.rule, f.match) for f in found] == [("email", _planted("real.person(at)gmail.com"))]


def test_a_planted_machine_identifier_is_caught() -> None:
    """The exact shape that cost a 126-commit history rewrite in Phase 6."""
    found = scan_text(
        _planted("Ada <ada(at)Adas-MacBook-Pro.local>"), identifiers=["Adas-MacBook-Pro"]
    )

    assert any(f.rule == "local-identifier" for f in found)


def test_a_storage_uri_is_not_mistaken_for_an_email() -> None:
    """`abfss://bronze@account.dfs.core.windows.net` matches an email regex and
    is not an identity. A check that cries wolf is a check people skip.
    """
    text = "abfss://bronze@almanaclake.dfs.core.windows.net/events"

    assert scan_text(text, identifiers=[]) == []


def test_the_pseudonymous_commit_address_is_allowed() -> None:
    """This project rewrote its history *to* this address; flagging it would
    ask it to undo the fix.
    """
    text = "283869510+someone@users.noreply.github.com"

    assert scan_text(text, identifiers=[]) == []


def test_an_identifier_only_matches_a_whole_word() -> None:
    """`ada` must not fire on `adamant` or `Adamson`."""
    assert scan_text("the adamant Adamson", identifiers=["ada"]) == []
    assert scan_text('owner = "ada"', identifiers=["ada"]) != []


def test_the_published_surfaces_include_the_readme_and_docs() -> None:
    """A scan that silently stops finding files would pass forever."""
    names = {p.name for p in published_files(REPO_ROOT)}

    assert "README.md" in names
    assert "STATUS.md" in names
    assert len(published_files(REPO_ROOT)) > 40


def test_the_published_surfaces_include_source_and_tests() -> None:
    """A public repo publishes its code, and the globs stopped short of it.

    A machine name sat in `pseudonymity.py`'s own docstring and another in this
    file, both invisible to the gate below because the surfaces ended at docs
    and terraform -- the same miss as the .gitignore comment, one layer up, in
    the module that exists to prevent it.
    """
    names = {p.relative_to(REPO_ROOT).as_posix() for p in published_files(REPO_ROOT)}

    assert "src/almanac/governance/pseudonymity.py" in names
    assert "tests/unit/test_governance_pseudonymity.py" in names


def test_the_published_surfaces_include_the_agent_tooling() -> None:
    """`.claude/` is tracked and public, and reads like a private notebook.

    Added the same day as the `src/`+`tests/` widening: enumerating surfaces
    by hand is exactly the mechanism that missed those two, and `.claude/`
    was the next directory the same argument reached. Clean when checked --
    this closes the gap before it costs something, not after.
    """
    names = {p.relative_to(REPO_ROOT).as_posix() for p in published_files(REPO_ROOT)}

    assert "CLAUDE.md" in names
    assert any(n.startswith(".claude/skills/") for n in names), "skills unscanned"
    assert any(n.startswith(".claude/hooks/") for n in names), "hooks unscanned"


def test_a_ci_service_account_is_not_treated_as_an_identity() -> None:
    """CI is where this check matters most, and it broke there first.

    GitHub Actions runs as the login `runner`, which is also an ordinary word
    in this repo -- job runner, features runner, `runner.py`. Reading it as a
    personal identifier failed 6 files on the first CI run this check ever saw
    (2026-09-08), on a repo with no leak in it.
    """
    assert local_identifiers(hostname="fv-az1234-567", login="runner") == ["fv-az1234-567"]


def test_a_personal_login_is_still_an_identity() -> None:
    """The exclusion must not swallow the case the check exists for."""
    assert "ada" in local_identifiers(hostname="host-abc", login="ada")


def test_no_published_artifact_carries_an_identity() -> None:
    """The gate. Runs against this machine's own identifiers, so a leak from
    whoever is working is caught on their next run rather than at review time.
    """
    findings = scan(REPO_ROOT, identifiers=local_identifiers())

    assert not findings, "identity published in:\n" + "\n".join(str(f) for f in findings)


def test_the_demo_surface_is_covered() -> None:
    """A public demo is the highest-exposure artifact this repo ships.

    The control that once passed cleanly while aimed at the wrong files is the
    reason this is a test and not a note.
    """
    for glob in ("demo/**/*.py", "demo/**/*.json", "demo/**/*.md", "demo/requirements.txt"):
        assert glob in PUBLISHED_GLOBS, f"{glob} is published and unscanned"


def test_published_files_excludes_gitignored_matches(tmp_path: Path) -> None:
    """A glob is a surface a stranger reads. A gitignored match under it is not
    that -- found 2026-09-11 when the demo build step's own scratch Delta lake,
    gitignored, sat inside demo/**, newly scanned by Task 6, and failed the
    gate on a local absolute path that would never actually be published.
    """
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "demo" / "data" / "lake").mkdir(parents=True)
    (tmp_path / ".gitignore").write_text("demo/data/lake/\n")
    (tmp_path / "demo" / "data" / "lake" / "scratch.json").write_text("{}")
    (tmp_path / "demo" / "data" / "coverage.json").write_text("{}")

    found = {p.name for p in published_files(tmp_path, globs=("demo/**/*.json",))}

    assert found == {"coverage.json"}
