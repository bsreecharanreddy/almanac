"""The I/O edge for GH Archive. All decisions live in classify_response."""

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import httpx

from almanac.config import Settings
from almanac.extract.outcome import FetchResult, FetchStatus
from almanac.extract.urls import archive_url

_HTTP_OK = 200
_HTTP_NOT_FOUND = 404


def classify_response(status_code: int, content_length: int) -> FetchStatus:
    """Pure. Which of the four outcomes a response represents."""
    if status_code == _HTTP_NOT_FOUND:
        return FetchStatus.ABSENT
    if status_code == _HTTP_OK:
        return FetchStatus.OK if content_length > 0 else FetchStatus.EMPTY
    return FetchStatus.FAILED


def fetch_hour(
    day: date,
    hour: int,
    *,
    client: httpx.Client,
    dest_dir: Path,
    settings: Settings,
) -> FetchResult:
    """Fetch one hourly archive file. Retries only genuinely transient failures."""
    url = archive_url(day, hour, settings.archive_base_url)
    dest_dir.mkdir(parents=True, exist_ok=True)
    final = dest_dir / url.rsplit("/", 1)[-1]
    tmp = final.with_suffix(final.suffix + ".part")

    last_error: str | None = None
    attempt = 0
    for attempt in range(1, settings.max_fetch_attempts + 1):
        try:
            response = client.get(url, timeout=settings.http_timeout_seconds)
        except httpx.HTTPError as exc:
            last_error = str(exc)
            tmp.unlink(missing_ok=True)
            continue

        status = classify_response(response.status_code, len(response.content))

        # A 404 or an empty file is a fact about the data. Retrying either
        # wastes time and cannot change the answer.
        if status in (FetchStatus.ABSENT, FetchStatus.EMPTY):
            return FetchResult(url, status, None, 0, attempt)

        if status is FetchStatus.OK:
            # Write to .part then rename: a crash mid-write must never leave
            # a truncated file that a later run mistakes for complete.
            tmp.write_bytes(response.content)
            tmp.replace(final)
            return FetchResult(url, status, final, len(response.content), attempt)

        last_error = f"HTTP {response.status_code}"

    tmp.unlink(missing_ok=True)
    return FetchResult(url, FetchStatus.FAILED, None, 0, attempt, last_error)


def fetch_hours(
    hours: Iterable[tuple[date, int]],
    *,
    client: httpx.Client,
    dest_dir: Path,
    settings: Settings,
) -> list[FetchResult]:
    """Fetch many hourly files concurrently; results stay in ``hours`` order.

    ``fetch_hour`` absorbs every ``httpx`` error into a ``FAILED``/``ABSENT``
    result, so one missing hour never aborts the batch. A non-network fault
    (a full disk) still propagates, for the day-level checkpoint to catch.
    """
    ordered = list(hours)
    if not ordered:
        return []

    dest_dir.mkdir(parents=True, exist_ok=True)

    def _fetch(pair: tuple[date, int]) -> FetchResult:
        day, hour = pair
        return fetch_hour(day, hour, client=client, dest_dir=dest_dir, settings=settings)

    with ThreadPoolExecutor(max_workers=min(settings.fetch_concurrency, len(ordered))) as pool:
        return list(pool.map(_fetch, ordered))
