"""The GitHub REST API second source -- and the falsification of §4.5a's
"zero new Python" claim (docs/findings/2026-09-02-second-source.md).

Rate-limit waiting and pagination are the two behaviours a file fetch never
needs; ``RestSession`` carries injectable ``sleep`` / ``now`` so tests hit
neither the clock nor the network.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

import httpx

from almanac.pipeline.source import RestSourceConfig

_RATE_LIMITED = (403, 429)


@dataclass(frozen=True)
class RestSession:
    """Client, config and token composed into one object per enrichment run."""

    client: httpx.Client
    config: RestSourceConfig
    token: str
    sleep: Callable[[float], object] = time.sleep
    now: Callable[[], float] = field(default=time.time)

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


@dataclass(frozen=True)
class PrEnrichment:
    """The fields the October 2025 firehose reduction dropped (§12 trap 12)."""

    owner: str
    repo: str
    number: int
    merged: bool | None
    draft: bool | None
    title: str | None
    additions: int | None
    deletions: int | None
    changed_files: int | None


def _is_exhausted(response: httpx.Response, config: RestSourceConfig) -> bool:
    remaining = response.headers.get(config.rate_limit.header_remaining)
    return remaining is not None and int(remaining) <= 0


def _reset_wait(response: httpx.Response, session: RestSession) -> float:
    """Seconds until the rate-limit window resets, from the response's own header."""
    reset = response.headers.get(session.config.rate_limit.header_reset)
    if reset is None:
        return 1.0
    return max(0.0, float(int(reset) - session.now())) + 1.0


def _wait_if_exhausted(response: httpx.Response, session: RestSession) -> None:
    if _is_exhausted(response, session.config):
        session.sleep(_reset_wait(response, session))


def next_link(link_header: str) -> str | None:
    """The `rel="next"` URL from an RFC 8288 `Link` header, if any."""
    for part in link_header.split(","):
        segments = [s.strip() for s in part.split(";")]
        if 'rel="next"' in segments and segments[0].startswith("<") and segments[0].endswith(">"):
            return segments[0][1:-1]
    return None


def fetch_pr(session: RestSession, owner: str, repo: str, number: int) -> PrEnrichment:
    """One PR's enrichment fields, waiting out a rate-limit response; other non-200 raises."""
    url = session.config.url_template.format(owner=owner, repo=repo, number=number)

    for _ in range(session.config.max_attempts):
        response = session.client.get(url, headers=session.headers)
        if response.status_code in _RATE_LIMITED and _is_exhausted(response, session.config):
            session.sleep(_reset_wait(response, session))
            continue
        response.raise_for_status()
        _wait_if_exhausted(response, session)  # make the next caller in the batch wait
        return _to_enrichment(owner, repo, number, response.json())

    raise RuntimeError(f"rate limited for all {session.config.max_attempts} attempts on {url}")


def list_pr_numbers(session: RestSession, owner: str, repo: str) -> Iterator[int]:
    """Every PR number in a repo, following the ``Link`` header across pages."""
    url: str | None = session.config.list_url_template.format(owner=owner, repo=repo)

    while url:
        response = session.client.get(url, headers=session.headers)
        response.raise_for_status()
        for pr in response.json():
            yield int(pr["number"])
        _wait_if_exhausted(response, session)
        url = next_link(response.headers.get("Link", ""))


def _to_enrichment(owner: str, repo: str, number: int, payload: dict[str, object]) -> PrEnrichment:
    return PrEnrichment(
        owner=owner,
        repo=repo,
        number=number,
        merged=payload.get("merged"),  # type: ignore[arg-type]
        draft=payload.get("draft"),  # type: ignore[arg-type]
        title=payload.get("title"),  # type: ignore[arg-type]
        additions=payload.get("additions"),  # type: ignore[arg-type]
        deletions=payload.get("deletions"),  # type: ignore[arg-type]
        changed_files=payload.get("changed_files"),  # type: ignore[arg-type]
    )
