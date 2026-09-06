"""Measure what fraction of the GitHub event stream a polling client can see."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from http import HTTPStatus

URL = "https://api.github.com/events?per_page=100"

# Archive-measured 2026 firehose volume (design doc 12). The capture fraction
# is observed rate over this, so it is a ratio of two independently sourced
# numbers -- not a direct count of what was missed, which is unobservable.
ARCHIVE_EVENTS_PER_HOUR = (155_000, 162_000)


def fetch(
    url: str, token: str, etag: str | None = None
) -> tuple[int, dict[str, str], list[dict] | None]:
    """One request. Returns status, lowercased headers, and the decoded body."""
    request = urllib.request.Request(url)
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("User-Agent", "almanac-events-measurement")
    if etag:
        request.add_header("If-None-Match", etag)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            headers = {k.lower(): v for k, v in response.headers.items()}
            return response.status, headers, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, None


def poll_window(token: str) -> tuple[dict[str, datetime], dict[str, str]]:
    """Every page the Link header exposes, deduplicated by event id."""
    events: dict[str, datetime] = {}
    headers: dict[str, str] = {}
    url: str | None = URL
    while url:
        status, headers, body = fetch(url, token)
        if status != HTTPStatus.OK or body is None:
            break
        for event in body:
            events[event["id"]] = datetime.fromisoformat(event["created_at"].replace("Z", "+00:00"))
        url = next(
            (
                part.split(";")[0].strip().strip("<>")
                for part in headers.get("link", "").split(",")
                if 'rel="next"' in part
            ),
            None,
        )
    return events, headers


def probe_conditional(token: str, attempts: int = 5) -> str:
    """Whether a 304 is reachable at all, and if so whether it costs rate budget."""
    for _ in range(attempts):
        _, first, _ = fetch(URL, token)
        before = int(first.get("x-ratelimit-used", -1))
        status, second, _ = fetch(URL, token, etag=first.get("etag"))
        after = int(second.get("x-ratelimit-used", -1))
        if status == HTTPStatus.NOT_MODIFIED:
            return f"304 reached; used {before}->{after}, exempt={after == before}"
    return f"no 304 in {attempts} attempts -- the feed moves faster than two requests"


def main() -> int:
    token = os.environ.get("GH_TOKEN")
    if not token:
        print("GH_TOKEN is required (try: GH_TOKEN=$(gh auth token))", file=sys.stderr)
        return 2

    target = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    print(f"# conditional-request probe: {probe_conditional(token)}", flush=True)
    print("poll,events,new,overlap,lag_s,interval_s,rl_remaining")

    previous: set[str] = set()
    samples: list[tuple[int, float]] = []
    overlaps: list[int] = []
    for poll in range(target):
        started = time.monotonic()
        events, headers = poll_window(token)
        if not events:
            print(f"{poll},ERROR,no events returned", flush=True)
            time.sleep(60)
            continue

        interval = float(headers.get("x-poll-interval", 60))
        lag = (datetime.now(UTC) - max(events.values())).total_seconds()
        overlap = len(set(events) & previous)
        overlaps.append(overlap)
        samples.append((len(events), interval))
        print(
            f"{poll},{len(events)},{len(set(events) - previous)},{overlap},"
            f"{lag:.0f},{interval:.0f},{headers.get('x-ratelimit-remaining', '?')}",
            flush=True,
        )

        previous = set(events)
        if poll < target - 1:
            time.sleep(max(interval - (time.monotonic() - started), 1))

    if samples:
        n = len(samples)
        mean_events = sum(e for e, _ in samples) / n
        mean_interval = sum(i for _, i in samples) / n
        per_hour = mean_events / mean_interval * 3600
        low, high = ARCHIVE_EVENTS_PER_HOUR
        print(f"\n# n = {n} polls")
        print(f"# mean events per window : {mean_events:.1f}")
        print(f"# observed throughput    : {per_hour:,.0f}/hour")
        print(
            f"# capture fraction       : {per_hour / high * 100:.1f}%-{per_hour / low * 100:.1f}%"
        )
        # The load-bearing number: zero overlap means the window turned over
        # entirely between polls, so events in the gap are unrecoverable.
        print(f"# total overlap          : {sum(overlaps)} across {len(overlaps)} polls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
