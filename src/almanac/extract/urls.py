"""Pure URL construction for GH Archive. No I/O."""

from collections.abc import Iterator
from datetime import date, timedelta

DEFAULT_BASE_URL = "https://data.gharchive.org"
HOURS_PER_DAY = 24


def archive_url(day: date, hour: int, base_url: str = DEFAULT_BASE_URL) -> str:
    """URL for one hourly archive file.

    The hour component is UNPADDED -- ``3``, never ``03``. The date
    component IS zero-padded. That asymmetry belongs to the scheme itself,
    and "fixing" it produces a 404 for ten of every twenty-four files.
    """
    if not 0 <= hour < HOURS_PER_DAY:
        raise ValueError(f"hour must be 0..23, got {hour}")
    return f"{base_url}/{day:%Y-%m-%d}-{hour}.json.gz"


def hours_in_range(start: date, end: date) -> Iterator[tuple[date, int]]:
    """Every ``(day, hour)`` from ``start`` to ``end`` inclusive, in order."""
    if end < start:
        raise ValueError(f"end {end} must not be before start {start}")
    day = start
    while day <= end:
        for hour in range(HOURS_PER_DAY):
            yield day, hour
        day += timedelta(days=1)
