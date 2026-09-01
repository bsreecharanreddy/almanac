from collections.abc import Callable
from datetime import date
from pathlib import Path

import httpx
import pytest

from almanac.config import Settings
from almanac.extract.archive import classify_response, fetch_hour
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
