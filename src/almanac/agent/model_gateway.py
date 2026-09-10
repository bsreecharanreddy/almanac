"""One seam in front of the foundation-model endpoints: what each accepts, when a
failure is worth falling back on, and which model actually answered."""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import httpx2
from databricks.sdk.service.serving import EndpointStateReady, ServingEndpoint
from openai import AsyncOpenAI
from pydantic_ai.exceptions import ModelAPIError, ModelHTTPError
from pydantic_ai.messages import ModelResponse
from pydantic_ai.models import Model
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.settings import ModelSettings

from almanac.agent.gateway import AuditLog, AuditRecord

PRIMARY_ENDPOINT = "databricks-claude-sonnet-5"
FALLBACK_ENDPOINT = "databricks-gpt-oss-120b"

# What a model call is called in the audit log Task 8 writes, so a model
# request and a tool call sit in one ordered record of the run.
MODEL_CALL = "model_request"

# Every sampling knob `ModelSettings` carries, named once so the record below
# is a statement about a closed set rather than about whatever was passed.
SAMPLING_PARAMETERS = frozenset({"temperature", "top_p", "top_k"})

# Which of those each endpoint has been *shown* to accept. Default-deny: an
# endpoint missing here has not been checked, and "not checked" must not read
# as "fine" -- the rule `LAKEBASE_REGIONS` states, learned from a region that
# was never checked and cost 44 minutes.
#
# The primary's empty set is a different claim from absence: it means checked,
# and accepts none. `databricks-claude-sonnet-5` returns 400 for all three
# (design doc S9.3 finding 1).
SAMPLING_SUPPORT: Mapping[str, frozenset[str]] = {PRIMARY_ENDPOINT: frozenset()}

DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_RETRIES = 2

# The two HTTP conditions worth a second attempt on another endpoint. Named
# rather than inlined because they *are* the fallback policy, and a policy
# spelled as two bare integers inside a boolean is one nobody reviews.
TOO_MANY_REQUESTS = 429
SERVER_ERROR_FLOOR = 500


class UnsupportedSettingError(ValueError):
    """A request carried a sampling parameter the endpoint has not been shown to accept."""


class EndpointNotReadyError(RuntimeError):
    """A configured model is missing or not serving, found before the first paid call."""


class ServingCatalog(Protocol):
    """The one `WorkspaceClient.serving_endpoints` read preflight makes."""

    def list(self) -> Iterable[ServingEndpoint]: ...


def conform_settings(endpoint: str, settings: ModelSettings | None) -> ModelSettings | None:
    """Refuse a sampling parameter the endpoint rejects, before the request rather than after.

    `contracts.py`'s pattern pointed at a request instead of a frame: refuse a
    shape the consumer could not have expected. Refusing rather than stripping,
    because a silently dropped `temperature` leaves the caller believing it
    applied -- and the 400 it would otherwise cause is the exact error the
    fallback policy below exists to *not* absorb.
    """
    if settings is None:
        return None

    offered = SAMPLING_PARAMETERS.intersection(settings)
    if not offered:
        return settings

    accepted = SAMPLING_SUPPORT.get(endpoint)
    if accepted is None:
        raise UnsupportedSettingError(
            f"{endpoint} has not been checked for {', '.join(sorted(offered))}, and an "
            "unchecked endpoint is not a safe one: an unsupported sampling parameter is a "
            "permanent 400 that no fallback will rescue. Check it, then add it to "
            "SAMPLING_SUPPORT."
        )

    refused = sorted(offered - accepted)
    if refused:
        raise UnsupportedSettingError(
            f"{endpoint} returns 400 for {', '.join(refused)}, so the request would fail "
            f"every time. It accepts: {', '.join(sorted(accepted)) or 'no sampling parameters'}."
        )
    return settings


@dataclass(frozen=True)
class Workspace:
    """How to reach the serving endpoints, and on what budget.

    One record shared by every model in the chain, because "one seam" is the
    claim this module makes: the primary and the fallback differ in their name
    and in nothing else.
    """

    base_url: str
    api_key: str
    timeout: float = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    http_client: httpx2.AsyncClient | None = None


def build_model(
    endpoint: str, workspace: Workspace, *, settings: ModelSettings | None = None
) -> OpenAIChatModel:
    """One endpoint, with its timeout and retry budget named here rather than inherited.

    The Databricks serving API is OpenAI-compatible, so both endpoints reach the
    agent through one client type -- which is what makes the fallback below a
    swap of one endpoint for another instead of two unrelated code paths.
    """
    client = AsyncOpenAI(
        base_url=workspace.base_url,
        api_key=workspace.api_key,
        http_client=workspace.http_client,
        timeout=workspace.timeout,
        max_retries=workspace.max_retries,
    )
    return OpenAIChatModel(
        endpoint,
        provider=OpenAIProvider(openai_client=client),
        settings=conform_settings(endpoint, settings),
    )


def is_transient(exception: Exception) -> bool:
    """Fall back on what a retry could fix, and on nothing else.

    A 4xx that is not 429 is permanent and usually ours -- an unsupported
    sampling parameter, a wrong endpoint name. Falling back on one means the run
    succeeds on a model nobody chose and nothing says so, which is design doc
    S9.3 finding 2 and the reason `fallback_on` is set explicitly rather than
    left at its default of every `ModelAPIError`.
    """
    if isinstance(exception, ModelHTTPError):
        return (
            exception.status_code == TOO_MANY_REQUESTS
            or exception.status_code >= SERVER_ERROR_FLOOR
        )
    # A bare `ModelAPIError` is connection-level -- a timeout, a reset, DNS --
    # and is transient by construction, because it never reached the model.
    return isinstance(exception, ModelAPIError)


def fallback_model(primary: Model, fallback: Model) -> FallbackModel:
    """The two endpoints as one model, falling back only on a transient failure."""
    return FallbackModel(primary, fallback, fallback_on=is_transient)


def answering_model(response: ModelResponse) -> str:
    """Which model answered, per the response itself.

    Read back rather than inferred, the same discipline `versions` applies to
    the registry: with a fallback in the chain the configured primary is a guess
    about which model ran, and Phase 8 found a guess exactly like that wrong on
    the serving endpoint.
    """
    if not response.model_name:
        raise ValueError("the response does not say which model produced it")
    return response.model_name


def record_model_call(
    audit: AuditLog, *, response: ModelResponse, latency_ms: float
) -> AuditRecord:
    """One model call, in the same ordered log as the tool calls."""
    record = AuditRecord(
        tool=MODEL_CALL,
        arguments={},
        outcome="ok",
        latency_ms=latency_ms,
        model=answering_model(response),
    )
    audit.append(record)
    return record


def preflight(
    catalog: ServingCatalog,
    *,
    required: Sequence[str] = (PRIMARY_ENDPOINT, FALLBACK_ENDPOINT),
) -> None:
    """Refuse to run unless every configured endpoint exists and is serving.

    Same shape as `infra.lakebase_window.check_region`: the message names the
    symptom, because the failure otherwise looks like something else. Here the
    symptom is a 404 on the first call inside a paid window.

    The docs are not a safe source for this. The general-purpose table omits
    `databricks-claude-sonnet-5` while the endpoint reports READY in this
    workspace (design doc S9.3 finding 3), so the workspace is asked directly.
    A standing guard rather than a one-time check, because models retire on
    announced dates -- Claude Sonnet 4 already carries 2026-10-09.
    """
    serving = {
        endpoint.name
        for endpoint in catalog.list()
        if endpoint.name is not None
        and endpoint.state is not None
        and endpoint.state.ready == EndpointStateReady.READY
    }
    unusable = [name for name in required if name not in serving]
    if unusable:
        raise EndpointNotReadyError(
            f"{', '.join(unusable)} is not a serving endpoint in this workspace, so the first "
            "call would 404 inside a paid window rather than fail here for free. Serving now: "
            f"{', '.join(sorted(serving)) or 'nothing'}."
        )


def elapsed_ms(started: float) -> float:
    """Wall time since `time.perf_counter()`, for the audit record's latency."""
    return (time.perf_counter() - started) * 1000
