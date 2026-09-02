"""The GitHub REST API second source. Never touches the network.

This module and its tests exist to *measure* §4.5a's "a new source
onboards via YAML alone, zero new Python" claim, not to assume it. The
finding is written up in `docs/findings/2026-09-02-second-source.md`.
"""

import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from almanac.extract.rest import PrEnrichment, RestSession, fetch_pr, list_pr_numbers
from almanac.pipeline.source import RestSourceConfig, SourceConfig

_ROOT = Path(__file__).resolve().parents[2]
CONF = _ROOT / "conf" / "sources" / "github_rest.yml"
PR_FIXTURE = json.loads((_ROOT / "tests" / "fixtures" / "github_pr_detail.json").read_text())

_Handler = Callable[[httpx.Request], httpx.Response]


def _session(
    handler: _Handler, *, sleep: Callable[[float], object] = lambda _: None
) -> RestSession:
    return RestSession(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        config=RestSourceConfig.load(CONF),
        token="t0ken",
        sleep=sleep,
        now=lambda: 999_999_900.0,
    )


# --- the config layer: the two shapes are genuinely different ---


def test_the_rest_config_loads() -> None:
    cfg = RestSourceConfig.load(CONF)
    assert cfg.kind == "rest_api"
    assert "{number}" in cfg.url_template
    assert cfg.auth.token_env
    assert cfg.rate_limit.requests_per_hour == 5000


def test_the_file_source_config_rejects_the_rest_config() -> None:
    """`SourceConfig` (extra='forbid') cannot absorb the REST fields --
    the first line of the finding: the config model needed new Python,
    not just a new YAML file."""
    with pytest.raises(ValidationError):
        SourceConfig.model_validate(
            {
                "name": "github_rest",
                "url_template": "u",
                "format": "json",
                "partition_by": ["a"],
                "quality_rules": [],
                "kind": "rest_api",
                "auth": {"token_env": "GITHUB_TOKEN"},
            }
        )


# --- fetching one PR's enrichment fields ---


def test_fetch_pr_extracts_the_lossy_fields() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization", "")
        return httpx.Response(200, json=PR_FIXTURE, headers={"X-RateLimit-Remaining": "4999"})

    got = fetch_pr(_session(handler), "acme", "almanac", 42)

    assert seen["url"] == "https://api.github.com/repos/acme/almanac/pulls/42"
    assert seen["auth"] == "Bearer t0ken"
    assert got == PrEnrichment(
        owner="acme",
        repo="almanac",
        number=42,
        merged=True,
        draft=False,
        title="Fix the off-by-one in the hour partitioner",
        additions=37,
        deletions=12,
        changed_files=3,
    )


def test_fetch_pr_backs_off_when_the_budget_is_exhausted() -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                403,
                json={"message": "rate limited"},
                headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1000000000"},
            )
        return httpx.Response(200, json=PR_FIXTURE, headers={"X-RateLimit-Remaining": "4998"})

    got = fetch_pr(_session(handler, sleep=slept.append), "acme", "almanac", 42)

    assert calls["n"] == 2, "a rate-limited response is retried after a wait"
    assert slept and slept[0] >= 100, "waited until the reset, not a fixed guess"
    assert got.merged is True


def test_fetch_pr_raises_on_a_real_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "Not Found"})

    with pytest.raises(httpx.HTTPStatusError):
        fetch_pr(_session(handler), "acme", "almanac", 999)


# --- pagination: the list endpoint needs the Link header ---


def test_list_pr_numbers_follows_the_link_header() -> None:
    base = "https://api.github.com/repos/acme/almanac/pulls?state=all&per_page=100"
    pages = {
        base: ([{"number": 1}, {"number": 2}], f'<{base}&page=2>; rel="next"'),
        f"{base}&page=2": ([{"number": 3}], ""),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body, link = pages[str(request.url)]
        return httpx.Response(
            200, json=body, headers={"Link": link, "X-RateLimit-Remaining": "4000"}
        )

    numbers = list(list_pr_numbers(_session(handler), "acme", "almanac"))
    assert numbers == [1, 2, 3]
