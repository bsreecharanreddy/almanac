"""No actor identity in a published artifact -- checked on every run, not remembered."""

from __future__ import annotations

import re
import socket
import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

# The surfaces a stranger reads. Terraform is in here because this repo goes
# public at v1.0 and a `tags` value is as visible as a README line.
PUBLISHED_GLOBS: tuple[str, ...] = (
    "README.md",
    "CLAUDE.md",
    "docs/**/*.md",
    "docs/**/*.json",
    "dashboards/**/*.json",
    "infra/**/*.tf",
    # A public repo publishes its dotfiles too. Added 2026-09-08 after the
    # first run of this check found a hostname in a .gitignore comment written
    # the same day -- in the rule that existed to stop that hostname leaking.
    ".gitignore",
)

# Two characters is not an identity, it is a false-positive generator: a
# hostname like "m1" would match half the prose in docs/.
_MIN_IDENTIFIER_LEN = 3

# A build agent's login names a machine, not a person, and `runner` is also an
# ordinary word here -- job runner, features runner, `runner.py`. Reading it as
# an identity failed 6 published files on the first CI run this check ever saw
# (2026-09-08), on a repo with nothing leaked in it. Same reasoning as
# _NOT_AN_EMAIL below: a check that cries wolf is a check people learn to skip.
_SERVICE_ACCOUNTS = frozenset(
    {"runner", "runneradmin", "root", "admin", "ubuntu", "vsts", "jenkins", "circleci"}
)

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# `abfss://bronze@account.dfs.core.windows.net` is a storage URI that happens to
# match an email; `…@users.noreply.github.com` is the pseudonymous address this
# project deliberately rewrote its history to use. Neither is an identity leak,
# and a check that cries wolf on them is a check people learn to skip.
_NOT_AN_EMAIL = (
    ".dfs.core.windows.net",
    ".blob.core.windows.net",
    "users.noreply.github.com",
    "@example.com",
)


@dataclass(frozen=True)
class Finding:
    """One identifier, where it is published, and which rule caught it."""

    path: str
    line: int
    rule: str
    match: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line} [{self.rule}] {self.match}"


def local_identifiers(*, hostname: str | None = None, login: str | None = None) -> list[str]:
    """Identity strings belonging to the machine this runs on.

    Derived, never committed. A denylist of personal identifiers would have to
    contain the very strings it exists to keep out of the repo, so this reads
    them from the host instead: the hostname (which cost this project a
    126-commit history rewrite when `Nymishas-MacBook-Pro` reached a commit
    trailer) and the OS login.

    `hostname` / `login` are injected by tests only, so the exclusion below can
    be exercised without a second machine -- the same reason `RestSession`
    carries an injectable clock.

    Deliberately excluded: `git config user.name` and `user.email`, which are
    already the public GitHub handle and its noreply address; and the service
    logins in `_SERVICE_ACCOUNTS`, which name a build agent rather than a person.
    """
    host = socket.gethostname() if hostname is None else hostname
    names = {host, host.removesuffix(".local"), _os_login() if login is None else login}
    return sorted(
        name
        for name in names
        if len(name) >= _MIN_IDENTIFIER_LEN and name.lower() not in _SERVICE_ACCOUNTS
    )


def _os_login() -> str:
    try:
        return subprocess.run(
            ["id", "-un"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):  # pragma: no cover - platform guard
        return ""


def _emails(line: str) -> list[str]:
    return [m for m in _EMAIL.findall(line) if not any(s in m for s in _NOT_AN_EMAIL)]


def scan_text(text: str, *, identifiers: Sequence[str], path: str = "<text>") -> list[Finding]:
    """Every published identity in `text`. Pure: no reads, no host lookups."""
    found: list[Finding] = []
    for number, line in enumerate(text.splitlines(), start=1):
        found += [Finding(path, number, "email", match) for match in _emails(line)]
        found += [
            Finding(path, number, "local-identifier", identifier)
            for identifier in identifiers
            if re.search(rf"\b{re.escape(identifier)}\b", line)
        ]
    return found


def published_files(root: Path, globs: Iterable[str] = PUBLISHED_GLOBS) -> list[Path]:
    """Every published artifact under `root`, in a stable order."""
    return sorted({path for glob in globs for path in root.glob(glob) if path.is_file()})


def scan(root: Path, *, identifiers: Sequence[str]) -> list[Finding]:
    """The I/O edge: read every published artifact and scan it."""
    return [
        finding
        for path in published_files(root)
        for finding in scan_text(
            path.read_text(encoding="utf-8", errors="replace"),
            identifiers=identifiers,
            path=str(path.relative_to(root)),
        )
    ]
