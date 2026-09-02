"""Pure counters over parsed events. No I/O."""

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from itertools import pairwise

# Curated exact logins, extended as real ones are found.
CURATED_BOTS = frozenset({"dependabot", "renovate", "github-actions"})

# Own clause so its false positives stay countable (§12 trap 6). A bare
# `ci$` matched 869 of 1,002 human surnames; the ci clauses require a
# separator or a case-sensitive camelCase suffix
# (docs/findings/2026-09-01-bot-classification.md).
BOT_REGEX = re.compile(r"(?i:(bot|automation)$)|(?i:[-_.]ci$)|[a-z0-9]CI$")


class BotMatch(StrEnum):
    SUFFIX = "suffix"  # [bot] suffix -- authoritative
    CURATED = "curated"  # exact match on a known list
    REGEX = "regex"  # heuristic; has false positives
    NONE = "none"


@dataclass(frozen=True)
class RepoRename:
    repo_id: int
    from_name: str
    to_name: str
    observed_at: datetime


def duplicate_ratio(event_ids: Iterable[str]) -> float:
    """Fraction of the stream that is a redundant copy (0.0 when all distinct)."""
    ids = list(event_ids)
    if not ids:
        return 0.0
    return (len(ids) - len(set(ids))) / len(ids)


def classify_bot(login: str) -> BotMatch:
    """Which clause, if any, marks this login as a bot."""
    if login.endswith("[bot]"):
        return BotMatch.SUFFIX
    if login.lower() in CURATED_BOTS:
        return BotMatch.CURATED
    if BOT_REGEX.search(login):
        return BotMatch.REGEX
    return BotMatch.NONE


def rename_events(
    observations: Iterable[tuple[int, str, datetime]],
) -> list[RepoRename]:
    """Name changes per repo id from ``(repo_id, repo_name, observed_at)`` tuples.

    Sorted by time first -- unsorted input would fabricate reversed renames.
    """
    by_repo: dict[int, list[tuple[datetime, str]]] = defaultdict(list)
    for repo_id, name, at in observations:
        by_repo[repo_id].append((at, name))

    renames: list[RepoRename] = []
    for repo_id, seen in by_repo.items():
        seen.sort()
        for (_, previous), (at, current) in pairwise(seen):
            if previous != current:
                renames.append(RepoRename(repo_id, previous, current, at))
    return renames
