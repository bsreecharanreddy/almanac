"""Poll the GitHub Events API and land raw events, one file per poll.

Measured 2026-09-06 (docs/findings/2026-09-06-events-api-and-online-store-
rates.md): the retrievable window turns over completely between polls --
zero event-id overlap across 30 consecutive polls -- so this is a sampled
tail, not a mirror. A conditional request never returned 304 in five
attempts either; ``If-None-Match`` is still sent because it is correct
client behaviour and costs nothing, not because 304 is load-bearing here.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from almanac.extract.rest import next_link
from almanac.pipeline.source import EventStreamConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from almanac.pipeline.source import AuthConfig

# 429 is unambiguous -- GitHub only returns it for rate/abuse limiting.
# 403 is not: GitHub also returns 403 for a real auth failure, distinguished
# only by whether the budget is actually exhausted.
_UNCONDITIONAL_BACKOFF = HTTPStatus.TOO_MANY_REQUESTS


def _utcnow() -> datetime:
    return datetime.now(UTC)


def resolve_token(auth: AuthConfig) -> str:
    """Read the token from the env var the config names. Never a literal in code or config."""
    token = os.environ.get(auth.token_env)
    if not token:
        raise RuntimeError(f"environment variable {auth.token_env!r} is not set")
    return token


@dataclass(frozen=True)
class PollerSession:
    """Client, config and token composed into one object per poll run.

    Same shape as ``RestSession`` in ``extract.rest``: an injectable
    ``sleep``/``now`` so tests hit neither the clock nor the network.
    """

    client: httpx.Client
    config: EventStreamConfig
    token: str
    sleep: Callable[[float], object] = time.sleep
    now: Callable[[], datetime] = field(default=_utcnow)

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


@dataclass(frozen=True)
class PollResult:
    events: tuple[dict[str, object], ...]
    etag: str | None
    poll_interval: float
    rate_limit_remaining: int | None


@dataclass(frozen=True)
class PollStats:
    polls: int
    events_written: int


def _is_exhausted(response: httpx.Response, config: EventStreamConfig) -> bool:
    remaining = response.headers.get(config.rate_limit.header_remaining)
    return remaining is not None and int(remaining) <= 0


def _should_back_off(response: httpx.Response, config: EventStreamConfig) -> bool:
    if response.status_code == _UNCONDITIONAL_BACKOFF:
        return True
    return response.status_code == HTTPStatus.FORBIDDEN and _is_exhausted(response, config)


def _retry_wait(response: httpx.Response, session: PollerSession) -> float:
    """Seconds to wait before retrying.

    ``Retry-After`` (seconds, GitHub's secondary/abuse limit) takes
    precedence over the reset epoch (the primary rate limit) -- the two
    are different mechanisms and only one may be present.
    """
    retry_after = response.headers.get("retry-after")
    if retry_after is not None:
        return float(retry_after)
    reset = response.headers.get(session.config.rate_limit.header_reset)
    if reset is None:
        return 1.0
    return max(0.0, float(int(reset) - session.now().timestamp())) + 1.0


def _get_with_backoff(session: PollerSession, url: str, headers: dict[str, str]) -> httpx.Response:
    for _ in range(session.config.max_attempts):
        response = session.client.get(url, headers=headers)
        if _should_back_off(response, session.config):
            session.sleep(_retry_wait(response, session))
            continue
        return response
    raise RuntimeError(f"rate limited for all {session.config.max_attempts} attempts")


def _poll_interval(response: httpx.Response, config: EventStreamConfig) -> float:
    return float(response.headers.get(config.poll_interval_header, 60))


def _rate_remaining(response: httpx.Response, config: EventStreamConfig) -> int | None:
    value = response.headers.get(config.rate_limit.header_remaining)
    return int(value) if value is not None else None


def poll_once(session: PollerSession, *, etag: str | None = None) -> PollResult:
    """One poll: page 1 conditional on ``etag``, then up to ``pages_per_poll`` pages total.

    A 304 on page 1 means nothing new since the last poll and returns no
    events -- a correctness path, not an error, even though it was never
    observed live in the measurement this config's rates are drawn from.
    """
    url: str | None = session.config.url
    events: dict[object, dict[str, object]] = {}
    new_etag = etag
    interval = 60.0
    remaining: int | None = None
    page = 0

    while url is not None and page < session.config.pages_per_poll:
        headers = dict(session.headers)
        if page == 0 and etag:
            headers["If-None-Match"] = etag

        response = _get_with_backoff(session, url, headers)

        if page == 0 and response.status_code == HTTPStatus.NOT_MODIFIED:
            return PollResult(
                events=(),
                etag=etag,
                poll_interval=_poll_interval(response, session.config),
                rate_limit_remaining=_rate_remaining(response, session.config),
            )

        response.raise_for_status()
        for raw_event in response.json():
            events[raw_event["id"]] = raw_event
        if page == 0:
            new_etag = response.headers.get("etag", etag)
        interval = _poll_interval(response, session.config)
        remaining = _rate_remaining(response, session.config)

        url = next_link(response.headers.get("link", ""))
        page += 1

    return PollResult(
        events=tuple(events.values()),
        etag=new_etag,
        poll_interval=interval,
        rate_limit_remaining=remaining,
    )


def _write_poll(
    events: tuple[dict[str, object], ...], dest: Path, polled_at: datetime, poll_index: int
) -> Path:
    """One file per poll, atomically. ``.part`` then rename: a crash mid-write
    must not leave a file a later run mistakes for complete (the same
    convention ``extract.archive.fetch_hour`` uses).

    Each line wraps the raw event with the poll's own timestamp, kept
    separate from the event's own ``created_at`` -- Task 3's watermark
    measures exactly this divergence, and conflating the two would erase
    the thing being measured. ``poll_index`` disambiguates the filename
    when ``now`` repeats (a fixed clock in a test, or Task 4's replay
    harness) -- without it, ``Path.replace`` would silently overwrite an
    earlier poll's file, an unsignalled data-loss bug on a live clock this
    is cheap enough to rule out entirely.

    ``event`` is embedded as a JSON-encoded *string*, not a nested object:
    a nested object forces the landing zone's own read schema to type it,
    and ``payloads.EVENT_SCHEMA`` deliberately omits ``actor`` (its type
    varies pre/post 2015), which would silently null every ``actor_login``
    in the stream before ``parse_events`` ever saw the event -- caught for
    real by Task 4's batch-equality gate, 2026-09-06. A string round-trips
    exactly what GitHub sent, matching Bronze's own raw-string contract.
    """
    final = dest / f"events_{polled_at:%Y%m%dT%H%M%S%f}_{poll_index:06d}.jsonl"
    tmp = final.with_suffix(final.suffix + ".part")
    lines = (
        json.dumps({"polled_at": polled_at.isoformat(), "event": json.dumps(e)}) for e in events
    )
    tmp.write_text("\n".join(lines) + "\n")
    tmp.replace(final)
    return final


def run_poller(session: PollerSession, dest: Path, *, max_polls: int | None = None) -> PollStats:
    """Poll in a loop, landing each non-empty poll and sleeping the server's own interval.

    ``max_polls=None`` runs until stopped externally -- the real Task 9
    shape, bounded instead by a job timeout. Tests always pass ``max_polls``.
    """
    dest.mkdir(parents=True, exist_ok=True)
    etag: str | None = None
    polls = 0
    events_written = 0

    while max_polls is None or polls < max_polls:
        polled_at = session.now()
        result = poll_once(session, etag=etag)
        if result.events:
            _write_poll(result.events, dest, polled_at, polls)
            events_written += len(result.events)
        etag = result.etag
        polls += 1
        if max_polls is not None and polls >= max_polls:
            break
        session.sleep(result.poll_interval)

    return PollStats(polls=polls, events_written=events_written)
