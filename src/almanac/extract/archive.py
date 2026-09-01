"""The I/O edge for GH Archive. All decisions live in classify_response."""

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
