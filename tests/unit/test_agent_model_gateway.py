"""The model gateway: what each endpoint accepts, when a failure is worth
falling back on, and which model actually answered.

Every assertion here is on a request or a stub, so the whole module is free --
no endpoint is called. That is the point of testing finding 1 on the *request*:
the vendor's 400 does not have to be bought to be guarded against.
"""

import json
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import anyio
import httpx2
import pytest
from databricks.sdk.service.serving import EndpointState, EndpointStateReady, ServingEndpoint
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.models import Model, ModelRequestParameters
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import Tool

from almanac.agent.bounded_agent import answer, build_agent
from almanac.agent.gateway import AuditLog
from almanac.agent.model_gateway import (
    FALLBACK_ENDPOINT,
    MODEL_CALL,
    PRIMARY_ENDPOINT,
    AuditedModel,
    EndpointNotReadyError,
    UnsupportedSettingError,
    Workspace,
    answering_model,
    build_model,
    fallback_model,
    preflight,
    record_failed_model_call,
    record_model_call,
)

_BASE_URL = "https://example.invalid/serving-endpoints"

_COMPLETION = {
    "id": "1",
    "object": "chat.completion",
    "created": 0,
    "choices": [
        {"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}
    ],
    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
}


def _answers(model: str) -> Callable[[httpx2.Request], httpx2.Response]:
    """A served response that names the model, the way the real endpoint does."""
    return lambda request: httpx2.Response(200, json={**_COMPLETION, "model": model})


def _fails(status: int) -> Callable[[httpx2.Request], httpx2.Response]:
    return lambda request: httpx2.Response(status, json={"error": {"message": "no"}})


def _times_out(request: httpx2.Request) -> httpx2.Response:
    raise httpx2.ConnectTimeout("the endpoint did not answer")


def _recording(
    handler: Callable[[httpx2.Request], httpx2.Response],
) -> tuple[list[dict[str, Any]], Callable[[httpx2.Request], httpx2.Response]]:
    """The bodies actually put on the wire, so a test can assert on the request."""
    bodies: list[dict[str, Any]] = []

    def record(request: httpx2.Request) -> httpx2.Response:
        bodies.append(json.loads(request.content))
        return handler(request)

    return bodies, record


def _model(
    endpoint: str,
    handler: Callable[[httpx2.Request], httpx2.Response],
    **kwargs: Any,
) -> Model:
    workspace = Workspace(
        base_url=_BASE_URL,
        api_key="not-a-real-key",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
        # Retries are the OpenAI client's own, and a retried 429 would make the
        # fallback tests slow without making them stronger.
        max_retries=0,
    )
    return build_model(endpoint, workspace, **kwargs)


def _ask(model: Model) -> ModelResponse:
    return anyio.run(
        lambda: model.request(
            [ModelRequest(parts=[UserPromptPart(content="which PRs are at risk?")])],
            None,
            ModelRequestParameters(),
        )
    )


def _endpoint(name: str, ready: EndpointStateReady) -> ServingEndpoint:
    return ServingEndpoint(name=name, state=EndpointState(ready=ready))


class _Catalog:
    """`WorkspaceClient.serving_endpoints`' one read, with no workspace behind it."""

    def __init__(self, *endpoints: ServingEndpoint) -> None:
        self._endpoints = endpoints

    def list(self) -> Iterator[ServingEndpoint]:
        return iter(self._endpoints)


def test_the_serialized_body_for_the_primary_endpoint_carries_no_sampling_keys() -> None:
    """Finding 1, asserted on the wire rather than on a comment saying not to."""
    bodies, handler = _recording(_answers(PRIMARY_ENDPOINT))

    _ask(_model(PRIMARY_ENDPOINT, handler))

    assert bodies[0]["model"] == PRIMARY_ENDPOINT
    assert not {"temperature", "top_p", "top_k"} & bodies[0].keys()


def test_a_sampling_parameter_the_primary_endpoint_rejects_is_refused_before_the_request() -> None:
    with pytest.raises(UnsupportedSettingError, match="temperature"):
        _model(PRIMARY_ENDPOINT, _answers(PRIMARY_ENDPOINT), settings={"temperature": 0.5})


def test_an_endpoint_with_no_capability_record_refuses_every_sampling_parameter() -> None:
    """Default-deny: unchecked must not read as fine, the rule LAKEBASE_REGIONS states."""
    with pytest.raises(UnsupportedSettingError, match="has not been checked"):
        _model("databricks-some-new-model", _answers("x"), settings={"top_p": 0.9})


def test_settings_that_are_not_sampling_parameters_reach_the_wire() -> None:
    """The gate is narrow on purpose -- it refuses sampling keys, not every setting.

    Asserted under the name the wire uses: `pydantic-ai` sends `max_tokens` as
    `max_completion_tokens`, so checking the settings dict instead of the body
    would have proved nothing about what the endpoint receives.
    """
    bodies, handler = _recording(_answers(PRIMARY_ENDPOINT))

    _ask(_model(PRIMARY_ENDPOINT, handler, settings={"max_tokens": 64}))

    assert bodies[0]["max_completion_tokens"] == 64


@pytest.mark.parametrize(
    ("label", "handler"),
    [
        ("timeout", _times_out),
        ("429", _fails(429)),
        ("503", _fails(503)),
    ],
)
def test_a_transient_failure_falls_back(
    label: str, handler: Callable[[httpx2.Request], httpx2.Response]
) -> None:
    """Three shapes, not one. Canopica's recorded lesson is a fallback that only
    ever caught a single shape and was therefore never really tested.
    """
    chain = fallback_model(
        _model(PRIMARY_ENDPOINT, handler),
        _model(FALLBACK_ENDPOINT, _answers(FALLBACK_ENDPOINT)),
    )

    assert answering_model(_ask(chain)) == FALLBACK_ENDPOINT


def test_a_400_does_not_fall_back() -> None:
    """Finding 2, the one that matters: a 400 is permanent and usually ours, so
    routing around it would let every answer come from a model nobody chose.
    """
    reached: list[str] = []

    def fallback(request: httpx2.Request) -> httpx2.Response:
        reached.append("fallback")
        return _answers(FALLBACK_ENDPOINT)(request)

    chain = fallback_model(
        _model(PRIMARY_ENDPOINT, _fails(400)), _model(FALLBACK_ENDPOINT, fallback)
    )

    with pytest.raises(ModelHTTPError) as failure:
        _ask(chain)

    assert failure.value.status_code == 400
    assert reached == []


def test_the_answering_model_is_read_back_from_the_response_not_inferred(tmp_path: Any) -> None:
    """With a fallback in the chain the configured primary is a guess about which
    model ran, and Phase 8 already found a guess like that wrong on the endpoint.
    """
    chain = fallback_model(
        _model(PRIMARY_ENDPOINT, _fails(429)),
        _model(FALLBACK_ENDPOINT, _answers(FALLBACK_ENDPOINT)),
    )
    audit = AuditLog(tmp_path / "audit.jsonl")

    record_model_call(audit, response=_ask(chain), latency_ms=12.0)

    record = audit.records()[-1]
    assert record.model == FALLBACK_ENDPOINT
    assert record.model != PRIMARY_ENDPOINT


def test_preflight_refuses_an_endpoint_that_does_not_exist_and_names_what_does() -> None:
    catalog = _Catalog(_endpoint("databricks-gpt-oss-120b", EndpointStateReady.READY))

    with pytest.raises(EndpointNotReadyError) as refusal:
        preflight(catalog, required=["databricks-invented-model", "databricks-gpt-oss-120b"])

    assert "databricks-invented-model" in str(refusal.value)
    assert "databricks-gpt-oss-120b" in str(refusal.value)


def test_preflight_refuses_an_endpoint_that_exists_but_is_not_serving() -> None:
    """Existing and answering are different claims, and only the second is useful."""
    catalog = _Catalog(
        _endpoint(PRIMARY_ENDPOINT, EndpointStateReady.NOT_READY),
        _endpoint(FALLBACK_ENDPOINT, EndpointStateReady.READY),
    )

    with pytest.raises(EndpointNotReadyError, match=PRIMARY_ENDPOINT):
        preflight(catalog)


def test_preflight_passes_when_both_configured_models_are_ready() -> None:
    catalog = _Catalog(
        _endpoint(PRIMARY_ENDPOINT, EndpointStateReady.READY),
        _endpoint(FALLBACK_ENDPOINT, EndpointStateReady.READY),
    )

    preflight(catalog)


# Deliberately not the configured primary: a record that took its model from
# config rather than from the error would otherwise pass by coincidence.
_ASKED = "databricks-claude-opus-5"


def test_a_failed_model_call_is_recorded_with_the_model_that_was_asked(tmp_path: Any) -> None:
    """The live window's first finding: a 403 that left the audit log empty."""
    audit = AuditLog(tmp_path / "audit.jsonl")
    refused = ModelHTTPError(403, _ASKED, {"error_code": "PERMISSION_DENIED"})

    record_failed_model_call(audit, refused, latency_ms=7.0)

    record = audit.records()[-1]
    assert record.tool == MODEL_CALL
    assert record.outcome == "failed"
    assert record.model == _ASKED
    assert "403" in (record.reason or "")


def test_an_agent_run_the_model_refuses_leaves_a_failed_audit_record(tmp_path: Any) -> None:
    """Through `answer` itself, not only the helper: the run raises, and the
    log says which model refused before it does.
    """

    def refuses(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        raise ModelHTTPError(403, _ASKED, {"error_code": "PERMISSION_DENIED"})

    audit = AuditLog(tmp_path / "audit.jsonl")
    agent = build_agent(FunctionModel(refuses), [], audit=audit)

    with pytest.raises(ModelHTTPError):
        anyio.run(lambda: answer(agent, "is anything at risk?"))

    assert [(r.outcome, r.model) for r in audit.records()] == [("failed", _ASKED)]


_PRIMARY_STUB = "primary-stub"
_FALLBACK_STUB = "fallback-stub"
_SLOW_TOOL_SECONDS = 0.3


def _calls_then_answers(tool_name: str) -> FunctionModel:
    """One tool call, then an answer: the smallest run with two model requests."""
    responses = iter(
        [
            ModelResponse(parts=[ToolCallPart(tool_name=tool_name, args={})]),
            ModelResponse(parts=[TextPart(content="done")]),
        ]
    )

    def next_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return next(responses)

    return FunctionModel(next_response, model_name=_PRIMARY_STUB)


def _noop() -> str:
    return "ok"


def test_each_model_request_is_recorded_with_its_own_latency_not_the_runs(tmp_path: Any) -> None:
    """A slow tool must not make the model look slow: the first live answer logged
    two tool calls' time against its one model record.
    """

    async def slow() -> str:
        await anyio.sleep(_SLOW_TOOL_SECONDS)
        return "done"

    audit = AuditLog(tmp_path / "audit.jsonl")
    agent = build_agent(_calls_then_answers("slow"), [Tool(slow)], audit=audit)

    anyio.run(lambda: answer(agent, "is anything at risk?"))

    requests = [r for r in audit.records() if r.tool == MODEL_CALL]
    assert len(requests) == 2
    assert all(r.latency_ms < _SLOW_TOOL_SECONDS * 1000 for r in requests)


def test_a_fallback_that_answered_an_earlier_request_is_on_that_requests_record(
    tmp_path: Any,
) -> None:
    """One record per run names whichever model answered last, so a fallback that
    chose the run's tool call would leave the configured primary on the record.
    """
    attempts: list[str] = []

    def primary(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        attempts.append("primary")
        if len(attempts) == 1:
            raise ModelHTTPError(429, _PRIMARY_STUB, {"error": "busy"})
        return ModelResponse(parts=[TextPart(content="done")])

    def fallback(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(tool_name="noop", args={})])

    chain = fallback_model(
        FunctionModel(primary, model_name=_PRIMARY_STUB),
        FunctionModel(fallback, model_name=_FALLBACK_STUB),
    )
    audit = AuditLog(tmp_path / "audit.jsonl")
    agent = build_agent(chain, [Tool(_noop, name="noop")], audit=audit)

    anyio.run(lambda: answer(agent, "is anything at risk?"))

    assert [(r.outcome, r.model) for r in audit.records() if r.tool == MODEL_CALL] == [
        ("ok", _FALLBACK_STUB),
        ("ok", _PRIMARY_STUB),
    ]


async def _streams(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
    yield "streamed"


def test_a_streamed_request_is_refused_rather_than_left_unaudited(tmp_path: Any) -> None:
    """A stream would reach the model with no record, so it does not reach it at all."""
    audit = AuditLog(tmp_path / "audit.jsonl")
    model = AuditedModel(FunctionModel(stream_function=_streams, model_name=_PRIMARY_STUB), audit)

    async def stream() -> None:
        async with model.request_stream(
            [ModelRequest(parts=[UserPromptPart(content="which PRs are at risk?")])],
            None,
            ModelRequestParameters(),
        ):
            pass

    with pytest.raises(NotImplementedError, match="audit"):
        anyio.run(stream)
    assert audit.records() == []
