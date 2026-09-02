"""Hour-level completeness. Pure; no I/O.

A missing hour is a defect this pipeline reports, never one it hides.
Design doc §12 trap 5: a PR's first response can land in a skipped hour,
and losing it fabricates an SLA breach that never occurred.
"""

from collections.abc import Iterable, Set
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Self

from almanac.extract.urls import hours_in_range


def expected_hours(start: date, end: date) -> list[datetime]:
    """Every hour from ``start`` to ``end`` inclusive, as UTC datetimes.

    The bridge between ``hours_in_range``'s ``(date, hour)`` pairs and the
    ``datetime`` keys the rest of this module compares. It exists so the
    conversion is written once, and is timezone-aware once: aware and naive
    datetimes are never equal and never hash alike, so a single naive side
    makes every hour look absent and ``missing_hours`` reports a total
    outage instead of raising. Sibling of the Task 2 finding in
    ``bronze.add_ingestion_metadata`` -- same hazard, comparison side
    rather than storage side.
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
        # Derived from `exp`, not `len(present)`: `present` may carry hours
        # outside the range, and counting it directly would let a report
        # claim more coverage than the range actually has.
        return cls(expected=len(exp), present=len(exp) - len(gaps), missing=gaps)
