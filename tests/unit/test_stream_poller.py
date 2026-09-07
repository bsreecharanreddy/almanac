"""The GitHub Events API poller. Never touches the network."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from almanac.pipeline.source import EventStreamConfig
from almanac.stream.poller import PollerSession, poll_once, resolve_token, run_poller

_ROOT = Path(__file__).resolve().parents[2]
CONF = _ROOT / "conf" / "sources" / "github_events.yml"

_Handler = Callable[[httpx.Request], httpx.Response]

_EVENT_1 = {"id": "1", "type": "PushEvent", "created_at": "2026-09-06T12:00:00Z"}
_EVENT_2 = {"id": "2", "type": "IssuesEvent", "created_at": "2026-09-06T12:00:05Z"}
_EVENT_3 = {"id": "3", "type": "WatchEvent", "created_at": "2026-09-06T12:00:10Z"}


def _session(
    handler: _Handler,
    *,
    token: str = "t0ken",
    sleep: Callable[[float], object] = lambda _: None,
    now: Callable[[], datetime] = lambda: datetime(2026, 9, 6, 12, 5, tzinfo=UTC),
) -> PollerSession:
    return PollerSession(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        config=EventStreamConfig.load(CONF),
        token=token,
        sleep=sleep,
        now=now,
    )


# --- the config layer: a third shape, distinct from the other two sources ---


def test_the_event_stream_config_loads() -> None:
    cfg = EventStreamConfig.load(CONF)
    assert cfg.kind == "event_stream"
    assert cfg.auth.token_env
    assert cfg.pages_per_poll == 3


def test_resolve_token_reads_the_named_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "s3cr3t")
    assert resolve_token(EventStreamConfig.load(CONF).auth) == "s3cr3t"


def test_resolve_token_raises_without_leaking_the_token_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="GITHUB_TOKEN"):
        resolve_token(EventStreamConfig.load(CONF).auth)


# --- one poll: pagination, dedup, conditional requests ---


def test_poll_once_follows_pagination_up_to_pages_per_poll() -> None:
    base = "https://api.github.com/events?per_page=100"
    pages = {
        base: ([_EVENT_1], f'<{base}&page=2>; rel="next"'),
        f"{base}&page=2": ([_EVENT_2], f'<{base}&page=3>; rel="next"'),
        # A 4th page exists but pages_per_poll=3 must never fetch it.
        f"{base}&page=3": ([_EVENT_3], f'<{base}&page=4>; rel="next"'),
    }
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        body, link = pages[str(request.url)]
        return httpx.Response(200, json=body, headers={"Link": link, "X-Poll-Interval": "60"})

    result = poll_once(_session(handler))

    assert calls == [base, f"{base}&page=2", f"{base}&page=3"]
    assert len(result.events) == 3


def test_poll_once_dedups_events_seen_across_pages() -> None:
    base = "https://api.github.com/events?per_page=100"
    pages = {
        base: ([_EVENT_1], f'<{base}&page=2>; rel="next"'),
        f"{base}&page=2": ([_EVENT_1], ""),  # same id again -- must not double-count
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body, link = pages[str(request.url)]
        return httpx.Response(200, json=body, headers={"Link": link, "X-Poll-Interval": "60"})

    result = poll_once(_session(handler))

    assert result.events == (_EVENT_1,)


def test_conditional_request_sends_if_none_match_when_an_etag_is_known() -> None:
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["if_none_match"] = request.headers.get("if-none-match")
        return httpx.Response(200, json=[], headers={"ETag": 'W/"new"', "X-Poll-Interval": "60"})

    poll_once(_session(handler), etag='W/"prev"')

    assert seen["if_none_match"] == 'W/"prev"'


def test_304_returns_no_new_events_not_an_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(304, headers={"X-Poll-Interval": "60"})

    result = poll_once(_session(handler), etag='W/"prev"')

    assert result.events == ()
    assert result.etag == 'W/"prev"'  # preserved, not cleared


# --- backoff: a 429 is a signal to handle, not a limit to raise ---


def test_backs_off_on_429_using_retry_after() -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "30"})
        return httpx.Response(200, json=[_EVENT_1], headers={"X-Poll-Interval": "60"})

    result = poll_once(_session(handler, sleep=slept.append))

    assert calls["n"] == 2, "the 429 was retried after backing off, not raised"
    assert slept == [30.0]
    assert len(result.events) == 1


def test_403_with_an_exhausted_budget_backs_off_and_retries() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                403,
                json={"message": "rate limited"},
                headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1000000000"},
            )
        return httpx.Response(200, json=[_EVENT_1], headers={"X-Poll-Interval": "60"})

    result = poll_once(_session(handler, now=lambda: datetime(2001, 9, 9, tzinfo=UTC)))

    assert calls["n"] == 2, "an exhausted 403 was retried after backing off, not raised"
    assert len(result.events) == 1


def test_403_without_an_exhausted_budget_raises_immediately() -> None:
    """A 403 is not unambiguous -- GitHub also returns it for a real auth failure,
    distinguished only by whether the rate-limit budget is actually exhausted."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"message": "Bad credentials"})

    with pytest.raises(httpx.HTTPStatusError):
        poll_once(_session(handler))


def test_token_never_appears_in_error_messages() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="server error")

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        poll_once(_session(handler, token="s3cr3t-token"))

    assert "s3cr3t-token" not in str(excinfo.value)


# --- run_poller: cadence, landing, and the write's atomicity ---


def test_run_poller_sleeps_for_the_servers_poll_interval_not_a_constant(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[], headers={"X-Poll-Interval": "45"})

    slept: list[float] = []
    session = _session(handler, sleep=slept.append)
    run_poller(session, tmp_path, max_polls=2)

    assert slept == [45.0]


def test_poll_time_is_recorded_separately_from_the_events_created_at(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_EVENT_1], headers={"X-Poll-Interval": "60"})

    fixed_now = datetime(2026, 9, 6, 14, 0, 0, tzinfo=UTC)
    session = _session(handler, now=lambda: fixed_now)

    run_poller(session, tmp_path, max_polls=1)

    [path] = list(tmp_path.glob("*.jsonl"))
    record = json.loads(path.read_text().splitlines()[0])
    event = json.loads(record["event"])
    assert record["polled_at"] == fixed_now.isoformat()
    assert event["created_at"] == _EVENT_1["created_at"]
    assert record["polled_at"] != event["created_at"]


def test_two_polls_under_a_fixed_clock_do_not_overwrite_each_other(tmp_path: Path) -> None:
    """A fixed `now` (a test, or Task 4's replay harness) must not collapse two
    polls into one file -- `Path.replace` overwrites silently, which would be
    unsignalled data loss."""
    responses = [
        httpx.Response(200, json=[_EVENT_1], headers={"X-Poll-Interval": "60"}),
        httpx.Response(200, json=[_EVENT_2], headers={"X-Poll-Interval": "60"}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    fixed_now = datetime(2026, 9, 6, 12, 5, tzinfo=UTC)
    session = _session(handler, now=lambda: fixed_now)

    stats = run_poller(session, tmp_path, max_polls=2)

    files = sorted(tmp_path.glob("*.jsonl"))
    assert len(files) == 2, "both polls' files must survive, not collapse into one"
    assert stats.events_written == 2
    ids_seen = {json.loads(json.loads(f.read_text().splitlines()[0])["event"])["id"] for f in files}
    assert ids_seen == {"1", "2"}


def test_empty_poll_writes_no_file(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[], headers={"X-Poll-Interval": "60"})

    stats = run_poller(_session(handler), tmp_path, max_polls=1)

    assert stats.polls == 1
    assert stats.events_written == 0
    assert list(tmp_path.glob("*.jsonl")) == []


def test_a_write_failure_never_leaves_a_completed_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_EVENT_1], headers={"X-Poll-Interval": "60"})

    real_write_text = Path.write_text

    def boom(self: Path, *args: object, **kwargs: object) -> int:
        if self.suffix == ".part":
            raise OSError("disk full")
        return real_write_text(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "write_text", boom)

    with pytest.raises(OSError, match="disk full"):
        run_poller(_session(handler), tmp_path, max_polls=1)

    assert list(tmp_path.glob("*.jsonl")) == [], "a failed write must not leave a final file"
