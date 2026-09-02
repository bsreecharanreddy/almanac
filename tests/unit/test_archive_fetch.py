import threading
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

import httpx
import pytest

from almanac.config import Settings
from almanac.extract.archive import classify_response, fetch_hour, fetch_hours
from almanac.extract.outcome import FetchStatus

# --- the pure decision, exhaustively ---


def test_200_with_bytes_is_ok() -> None:
    assert classify_response(200, 1024) is FetchStatus.OK


def test_200_with_zero_bytes_is_empty_not_ok() -> None:
    # A published-but-empty hour is a real, different condition from a
    # missing one. Collapsing them hides collector outages.
    assert classify_response(200, 0) is FetchStatus.EMPTY


def test_404_is_absent_not_failed() -> None:
    # Absent hours are expected in this dataset and must NOT be retried
    # or treated as errors.
    assert classify_response(404, 0) is FetchStatus.ABSENT


@pytest.mark.parametrize("code", [500, 502, 503, 504, 429])
def test_server_errors_are_failed(code: int) -> None:
    assert classify_response(code, 0) is FetchStatus.FAILED


# --- the I/O edge, against a mock transport ---


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_writes_file_and_reports_ok(tmp_path: Path) -> None:
    body = b"\x1f\x8b" + b"0" * 100

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    result = fetch_hour(
        date(2025, 3, 15),
        3,
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(),
    )
    assert result.status is FetchStatus.OK
    assert result.bytes_downloaded == len(body)
    assert result.path is not None
    assert result.path.read_bytes() == body


def test_absent_hour_is_not_retried(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404)

    result = fetch_hour(
        date(2025, 3, 15),
        3,
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(),
    )
    assert result.status is FetchStatus.ABSENT
    assert calls["n"] == 1, "a 404 is a fact about the data, not a transient error"
    assert result.path is None


def test_empty_hour_is_not_retried(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, content=b"")

    result = fetch_hour(
        date(2025, 3, 15),
        3,
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(),
    )
    assert result.status is FetchStatus.EMPTY
    assert calls["n"] == 1


def test_server_error_is_retried_then_reported_failed(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    result = fetch_hour(
        date(2025, 3, 15),
        3,
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(max_fetch_attempts=3),
    )
    assert result.status is FetchStatus.FAILED
    assert calls["n"] == 3
    assert result.attempts == 3


def test_transient_error_then_success(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=b"\x1f\x8bdata")

    result = fetch_hour(
        date(2025, 3, 15),
        3,
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(),
    )
    assert result.status is FetchStatus.OK
    assert result.attempts == 2


def test_partial_download_leaves_no_file(tmp_path: Path) -> None:
    # A half-written file that looks complete is worse than no file: the
    # next run would skip it and Bronze would silently lose events.
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("connection dropped")

    result = fetch_hour(
        date(2025, 3, 15),
        3,
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(),
    )
    assert result.status is FetchStatus.FAILED
    assert list(tmp_path.iterdir()) == []


# --- the concurrent batch, for the backfill ---


def _hours(day: date, count: int) -> list[tuple[date, int]]:
    return [(day, h) for h in range(count)]


def test_fetch_hours_returns_a_result_per_hour_in_input_order(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=request.url.path.encode())

    results = fetch_hours(
        _hours(date(2025, 7, 1), 24),
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(fetch_concurrency=8),
    )
    assert [r.status for r in results] == [FetchStatus.OK] * 24
    assert [r.url for r in results] == [
        f"https://data.gharchive.org/2025-07-01-{h}.json.gz" for h in range(24)
    ]


def test_fetch_hours_actually_runs_in_parallel(tmp_path: Path) -> None:
    live = 0
    peak = 0
    lock = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.05)
        with lock:
            live -= 1
        return httpx.Response(200, content=b"\x1f\x8b")

    fetch_hours(
        _hours(date(2025, 7, 1), 12),
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(fetch_concurrency=6),
    )
    assert peak > 1, "fetch_hours must issue overlapping requests"
    assert peak <= 6, "and never more than fetch_concurrency at once"


def test_fetch_hours_empty_input_is_empty_output(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request should be made for an empty hour list")

    assert fetch_hours([], client=_client(handler), dest_dir=tmp_path, settings=Settings()) == []


def test_fetch_hours_one_bad_hour_does_not_sink_the_batch(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/2025-07-01-3.json.gz":
            return httpx.Response(502)
        return httpx.Response(200, content=b"\x1f\x8b")

    results = fetch_hours(
        _hours(date(2025, 7, 1), 6),
        client=_client(handler),
        dest_dir=tmp_path,
        settings=Settings(max_fetch_attempts=1),
    )
    assert results[3].status is FetchStatus.FAILED
    assert [r.status for i, r in enumerate(results) if i != 3] == [FetchStatus.OK] * 5
