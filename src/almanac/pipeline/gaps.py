"""Hour-level completeness. Pure; no I/O.

A missing hour is reported, never hidden: §12 trap 5 -- a PR's first
response can land in a skipped hour, fabricating an SLA breach.
"""

from collections.abc import Iterable, Set
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Self

from almanac.extract.urls import hours_in_range


def expected_hours(start: date, end: date) -> list[datetime]:
    """Every hour from ``start`` to ``end`` inclusive, as UTC datetimes.

    Timezone-aware once, here: aware and naive datetimes never compare or
    hash alike, so a single naive side would make every hour look absent.
    """
    return [
        datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
        for day, hour in hours_in_range(start, end)
    ]


def missing_hours(expected: Iterable[datetime], present: Set[datetime]) -> list[datetime]:
    """Expected hours absent from ``present``, sorted ascending."""
    return sorted(h for h in expected if h not in present)


@dataclass(frozen=True, slots=True)
class GapReport:
    expected: int
    present: int
    missing: list[datetime]

    @property
    def is_complete(self) -> bool:
        return not self.missing

    @classmethod
    def build(cls, expected: Iterable[datetime], present: Set[datetime]) -> Self:
        exp = list(expected)
        gaps = missing_hours(exp, present)
        # present may carry hours outside the range; count from exp so a
        # report cannot claim more coverage than the range has.
        return cls(expected=len(exp), present=len(exp) - len(gaps), missing=gaps)
