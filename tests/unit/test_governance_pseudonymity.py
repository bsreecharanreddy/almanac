"""§10: no actor identity in any published artifact -- enforced by this file.

The repo-wide scan at the bottom is the actual gate. The unit tests above it
exist so a change that quietly stops the scan from catching anything fails
loudly, rather than passing because it found nothing.
"""

from pathlib import Path

from almanac.governance.pseudonymity import (
    local_identifiers,
    published_files,
    scan,
    scan_text,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_a_planted_email_is_caught() -> None:
    found = scan_text("contact me at real.person@gmail.com please", identifiers=[])

    assert [(f.rule, f.match) for f in found] == [("email", "real.person@gmail.com")]


def test_a_planted_machine_identifier_is_caught() -> None:
    """The exact shape that cost a 126-commit history rewrite in Phase 6."""
    found = scan_text("Sree <sree@Chris-MacBook-Pro.local>", identifiers=["Chris-MacBook-Pro"])

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
    """`sree` must not fire on `streets` or `Sreenivasan`."""
    assert scan_text("the streets of Sreenivasan", identifiers=["sree"]) == []
    assert scan_text('owner = "sree"', identifiers=["sree"]) != []


def test_the_published_surfaces_include_the_readme_and_docs() -> None:
    """A scan that silently stops finding files would pass forever."""
    names = {p.name for p in published_files(REPO_ROOT)}

    assert "README.md" in names
    assert "STATUS.md" in names
    assert len(published_files(REPO_ROOT)) > 40


def test_no_published_artifact_carries_an_identity() -> None:
    """The gate. Runs against this machine's own identifiers, so a leak from
    whoever is working is caught on their next run rather than at review time.
    """
    findings = scan(REPO_ROOT, identifiers=local_identifiers())

    assert not findings, "identity published in:\n" + "\n".join(str(f) for f in findings)
