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

# Its own clause so its false positives stay countable (design doc §12,
# trap 6). The shape below is not the obvious one, and the difference is
# measured, not stylistic -- see docs/findings/2026-09-01-bot-classification.md.
#
# A bare `ci$` clause was tried first and is unusable: of 1,002 distinct
# logins ending in "ci" across 6.0M real events, 869 (86.7%) have no
# separator and are overwhelmingly human surnames -- Turkish (Akinci,
# Yazici, Ekinci, Avci) and Italian (Federici, Popovici, Falcucci). Real
# CI accounts almost always carry a separator (swift-ci, aws-sdk-rust-ci,
# LinuxServer-CI) or camelCase the suffix (VenlyCI, CheckmkCI).
#
# The third alternative is case-SENSITIVE on purpose: `[a-z0-9]CI$` needs a
# literal uppercase CI after a lowercase char, which admits VenlyCI while
# still rejecting all-caps human names like AlperenYABACI and AitanaESCI.
BOT_REGEX = re.compile(r"(?i:(bot|automation)$)|(?i:[-_.]ci$)|[a-z0-9]CI$")


class BotMatch(StrEnum):
    SUFFIX = "suffix"  # login ends with [bot] -- authoritative
    CURATED = "curated"  # exact match on a known list
    REGEX = "regex"  # heuristic; HAS false positives
    NONE = "none"


@dataclass(frozen=True)
class RepoRename:
    repo_id: int
    from_name: str
    to_name: str
    observed_at: datetime


def duplicate_ratio(event_ids: Iterable[str]) -> float:
    """Fraction of the stream that is a redundant copy.

    Counts extra copies, so a stream with no duplicates scores 0.0.
    """
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
    """Name changes per repo id, in observation order.

    Input is ``(repo_id, repo_name, observed_at)``. Observations are sorted
    by time first: unsorted input would fabricate reversed renames.
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
