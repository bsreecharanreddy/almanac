"""The bounded agent: it answers by calling tools, every call is audited, and a
run that hits the turn bound stops with a structured result rather than a
truncated answer.

Driven by a scripted `FunctionModel` rather than a live endpoint, so the loop,
the gateway, the tools and the Delta tables underneath them are all real and the
only stub is the thing that would cost money.
"""

from pathlib import Path
from typing import Any

import anyio
import pytest
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pyspark.sql import SparkSession

from almanac.agent.bounded_agent import (
    DEFAULT_TURN_LIMIT,
    Answered,
    Incomplete,
    answer,
    build_agent,
    gateway_tools,
    load_transcript,
    replay_model,
    save_transcript,
)
from almanac.agent.gateway import AuditLog, ToolGateway, allow_list
from almanac.agent.mcp_server import TOOL_NAMES, ToolContext, build_server
from almanac.agent.model_gateway import MODEL_CALL
from tests.agent_fixtures import (
    MODEL_NAME,
    TWO_ERA_EVENTS,
    StubRegistry,
    StubScoringModel,
    land_silver_and_features,
    silver_frame,
)

pytestmark = pytest.mark.spark

_STUB_MODEL_NAME = "stub-endpoint"
_QUESTION = "which model version is live?"


def _server(spark: SparkSession, tmp_path: Path) -> Any:
    silver_path, features_path = land_silver_and_features(
        spark, silver_frame(spark, TWO_ERA_EVENTS), tmp_path
    )
    return build_server(
        ToolContext(
            spark=spark,
            model=StubScoringModel(),
            registry=StubRegistry(),
            registered_model_name=MODEL_NAME,
            model_version="2",
            silver_path=silver_path,
            features_path=features_path,
        )
    )


def _wired(spark: SparkSession, tmp_path: Path) -> tuple[Any, ToolGateway, AuditLog]:
    server = _server(spark, tmp_path)
    audit = AuditLog(tmp_path / "audit" / "run.jsonl")
    gateway = ToolGateway(server, anyio.run(lambda: allow_list(server)), audit)
    return server, gateway, audit


def _script(*responses: ModelResponse) -> FunctionModel:
    """A model that says exactly what the test tells it to, in order."""
    remaining = iter(responses)

    def next_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return next(remaining)

    return FunctionModel(next_response, model_name=_STUB_MODEL_NAME)


def _always_calls_a_tool() -> FunctionModel:
    """A model that never stops asking -- the shape the turn bound exists for."""

    def next_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(tool_name="versions", args={})])

    return FunctionModel(next_response, model_name=_STUB_MODEL_NAME)


def _calls_versions_then_answers() -> FunctionModel:
    return _script(
        ModelResponse(parts=[ToolCallPart(tool_name="versions", args={})]),
        ModelResponse(parts=[TextPart(content="Version 2 is live.")]),
    )


def _agent(
    model: Any, spark: SparkSession, tmp_path: Path, label: str = "run"
) -> tuple[Any, AuditLog]:
    """Each agent lands its own tables: two in one directory is a Delta path clash."""
    server, gateway, audit = _wired(spark, tmp_path / label)
    return build_agent(model, anyio.run(lambda: gateway_tools(server, gateway))), audit


def test_the_agent_answers_by_calling_a_tool_and_both_calls_are_audited(
    spark: SparkSession, tmp_path: Path
) -> None:
    agent, audit = _agent(_calls_versions_then_answers(), spark, tmp_path)

    outcome = anyio.run(lambda: answer(agent, _QUESTION, audit=audit))

    assert isinstance(outcome, Answered)
    assert outcome.answer == "Version 2 is live."
    assert outcome.tools_called == ["versions"]
    assert [record.tool for record in audit.records()] == ["versions", MODEL_CALL]


def test_the_tools_the_agent_sees_come_from_the_served_surface(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Derived, not restated: a fourth hand-written copy of the tool list is the
    one that goes stale silently.
    """
    server, gateway, _ = _wired(spark, tmp_path)

    tools = anyio.run(lambda: gateway_tools(server, gateway))

    served = {tool.name: tool for tool in anyio.run(server.list_tools)}
    assert {tool.name for tool in tools} == set(TOOL_NAMES)
    for tool in tools:
        assert tool.description == served[tool.name].description
        assert tool.function_schema.json_schema == served[tool.name].input_schema


def test_a_tool_the_gateway_disallows_is_never_offered_to_the_agent(
    spark: SparkSession, tmp_path: Path
) -> None:
    server, _, audit = _wired(spark, tmp_path)
    narrowed = ToolGateway(server, frozenset({"versions"}), audit)

    tools = anyio.run(lambda: gateway_tools(server, narrowed))

    assert [tool.name for tool in tools] == ["versions"]


def test_a_run_that_hits_the_turn_bound_stops_with_a_structured_incomplete_result(
    spark: SparkSession, tmp_path: Path
) -> None:
    """No truncated answer, and no prose standing in for one that was never reached."""
    agent, audit = _agent(_always_calls_a_tool(), spark, tmp_path)

    outcome = anyio.run(lambda: answer(agent, _QUESTION, audit=audit, turn_limit=3))

    assert isinstance(outcome, Incomplete)
    assert "3" in outcome.reason
    assert outcome.tools_called == ["versions", "versions", "versions"]
    assert not hasattr(outcome, "answer")


def test_the_turn_bound_is_enforced_at_the_boundary(spark: SparkSession, tmp_path: Path) -> None:
    """Two model requests is exactly what one tool call plus an answer costs, so
    the bound is checked where it actually bites rather than far from it.
    """
    at_limit, audit = _agent(_calls_versions_then_answers(), spark, tmp_path, "at-limit")
    below, other_audit = _agent(_calls_versions_then_answers(), spark, tmp_path, "below")

    allowed = anyio.run(lambda: answer(at_limit, _QUESTION, audit=audit, turn_limit=2))
    refused = anyio.run(lambda: answer(below, _QUESTION, audit=other_audit, turn_limit=1))

    assert isinstance(allowed, Answered)
    assert isinstance(refused, Incomplete)


def test_the_answering_model_is_recorded_rather_than_assumed(
    spark: SparkSession, tmp_path: Path
) -> None:
    agent, audit = _agent(_calls_versions_then_answers(), spark, tmp_path)

    outcome = anyio.run(lambda: answer(agent, _QUESTION, audit=audit))

    assert isinstance(outcome, Answered)
    assert outcome.model == _STUB_MODEL_NAME
    assert audit.records()[-1].model == _STUB_MODEL_NAME


def test_a_transcript_round_trips_and_replays_to_the_same_answer(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The first run is the only one that needs a model. Every later one replays,
    deterministically and free, which is what Phase 10's suite inherits.
    """
    recorded, audit = _agent(_calls_versions_then_answers(), spark, tmp_path, "recorded")
    first = anyio.run(lambda: answer(recorded, _QUESTION, audit=audit))
    assert isinstance(first, Answered)

    path = tmp_path / "transcripts" / "versions.json"
    save_transcript(path, first.transcript)

    replayed, replay_audit = _agent(
        replay_model(load_transcript(path)), spark, tmp_path, "replayed"
    )
    second = anyio.run(lambda: answer(replayed, _QUESTION, audit=replay_audit))

    assert isinstance(second, Answered)
    assert second.answer == first.answer
    assert second.model == first.model
    assert second.tools_called == first.tools_called


def test_the_default_turn_bound_is_a_number_not_an_accident() -> None:
    """A bound that can be raised silently is not a bound."""
    assert DEFAULT_TURN_LIMIT > 0
