"""The bounded agent: plan, act, observe, with a hard turn bound and a structured result."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field
from pydantic_ai import Agent, capture_run_messages
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelResponse,
    ToolCallPart,
)
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import Tool
from pydantic_ai.usage import RunUsage, UsageLimits

from almanac.agent.gateway import AuditLog, ToolGateway
from almanac.agent.grounding import GroundingTrace, verify
from almanac.agent.model_gateway import AuditedModel, answering_model
from almanac.agent.schemas import Strict

# Six model requests is room for two or three tool calls and an answer. The
# number matters less than that it is one number, in one place, that a run
# cannot raise on its own.
DEFAULT_TURN_LIMIT = 6

# The rule the whole phase rests on, stated to the model as well as enforced
# around it: an LLM narrates a computed result, it never computes one.
INSTRUCTIONS = (
    "You answer questions about breach risk for work items in a queue, using only "
    "the tools available to you.\n"
    "Every number you state must come from a tool call in this run. Never estimate "
    "one, interpolate between two, or carry one over from another work item.\n"
    "If a tool returns a refusal, say which feature is missing and why, and give no "
    "number in its place.\n"
    "Report the model version and the Delta versions the tools return, so the answer "
    "is traceable to the data that produced it."
)


class Answered(Strict):
    """A question the agent finished, with the evidence it finished it on."""

    status: Literal["answered"] = "answered"
    question: str
    answer: str
    model: str
    tools_called: list[str]
    transcript: list[ModelMessage]


class Incomplete(Strict):
    """The turn bound reached first. No answer, rather than a truncated one."""

    status: Literal["incomplete"] = "incomplete"
    question: str
    reason: str
    tools_called: list[str]
    transcript: list[ModelMessage]


class Ungrounded(Strict):
    """A verified answer that would not ground -- kept for the record, not to rely on.

    `answer_grounded` handed the model the verifier's failed rows and one more
    turn, and the retry still did not verify. `answer` is the last thing it said;
    `grounding` is why it fails. A structured refusal, the shape `predict`'s drift
    refusal already has -- an ungroundable answer is a typed result, not a thing
    that ships.
    """

    status: Literal["ungrounded"] = "ungrounded"
    question: str
    answer: str
    grounding: GroundingTrace
    retries: int
    tools_called: list[str]
    transcript: list[ModelMessage]


AgentOutcome = Annotated[Answered | Incomplete | Ungrounded, Field(discriminator="status")]


async def gateway_tools(server: MCPServer, gateway: ToolGateway) -> list[Tool[Any]]:
    """The agent's tools, derived from the served surface rather than restated.

    Name, description and JSON schema all come from what the server advertises,
    which came from Task 2's models -- so what the agent believes it can call
    cannot drift from what the server serves. A hand-written fourth copy of the
    tool list is the one that would go stale silently.

    Every call routes through the gateway, so nothing reaches a tool unallowed
    or unaudited.
    """
    return [
        Tool.from_schema(
            _through_gateway(gateway, served.name),
            name=served.name,
            description=served.description,
            json_schema=served.input_schema,
        )
        for served in await server.list_tools()
        if served.name in gateway.allowed
    ]


def build_agent(model: Model, tools: Sequence[Tool[Any]], *, audit: AuditLog) -> Agent[None, str]:
    """One agent, its tools bound to the gateway and its model to the audit log."""
    return Agent(
        AuditedModel(model, audit), output_type=str, instructions=INSTRUCTIONS, tools=tools
    )


async def answer(
    agent: Agent[None, str], question: str, *, turn_limit: int = DEFAULT_TURN_LIMIT
) -> AgentOutcome:
    """One bounded run: an answer with its evidence, or a structured stop.

    The bound counts model requests rather than wall time, because the failure
    it guards is a loop that keeps calling tools, not one that runs slowly.
    """
    with capture_run_messages() as captured:
        try:
            result = await agent.run(question, usage_limits=UsageLimits(request_limit=turn_limit))
        except UsageLimitExceeded:
            return Incomplete(
                question=question,
                reason=(
                    f"stopped at the {turn_limit}-request bound with no answer reached; "
                    "a truncated answer would read like a finished one"
                ),
                tools_called=tools_called(captured),
                transcript=list(captured),
            )

    messages = result.all_messages()
    response = _final_response(messages)
    return Answered(
        question=question,
        answer=result.output,
        model=answering_model(response),
        tools_called=tools_called(messages),
        transcript=list(messages),
    )


# Handed to the model verbatim on the one retry, ahead of the verifier's own
# failure rows. It names the move, not the fix: call the tool that would
# substantiate the claim, or drop the number.
_RETRY_PROMPT = (
    "Your previous answer did not verify against the tools called in this run:\n"
    "{failures}\n"
    "You have one more turn. Call the tool that would substantiate the claim, or "
    "restate the answer using only numbers a tool returned. State no number you "
    "cannot point to a tool return for."
)


async def answer_grounded(
    agent: Agent[None, str], question: str, *, turn_limit: int = DEFAULT_TURN_LIMIT
) -> AgentOutcome:
    """A bounded run whose answer is verified against its own tool returns.

    An answer that does not ground gets exactly one more turn, the verifier's
    failed rows handed back as the observation. If it still does not ground the
    run ends `Ungrounded`. The retry spends a budgeted turn -- it is not a free
    one -- so the Phase 9 bound still holds across both attempts.
    """
    first = await answer(agent, question, turn_limit=turn_limit)
    if not isinstance(first, Answered):
        return first
    trace = verify(first.transcript)
    if trace.grounded:
        return first

    try:
        result = await agent.run(
            _retry_prompt(trace),
            message_history=first.transcript,
            usage=RunUsage(requests=_model_requests(first.transcript)),
            usage_limits=UsageLimits(request_limit=turn_limit),
        )
    except UsageLimitExceeded:
        return Ungrounded(
            question=question,
            answer=first.answer,
            grounding=trace,
            retries=1,
            tools_called=first.tools_called,
            transcript=first.transcript,
        )

    messages = result.all_messages()
    retried = verify(messages)
    if retried.grounded:
        return Answered(
            question=question,
            answer=result.output,
            model=answering_model(_final_response(messages)),
            tools_called=tools_called(messages),
            transcript=list(messages),
        )
    return Ungrounded(
        question=question,
        answer=result.output,
        grounding=retried,
        retries=1,
        tools_called=tools_called(messages),
        transcript=list(messages),
    )


def _retry_prompt(trace: GroundingTrace) -> str:
    return _RETRY_PROMPT.format(failures="\n".join(f"- {failure}" for failure in trace.failures))


def _model_requests(transcript: Sequence[ModelMessage]) -> int:
    """Model requests the run has already spent -- one response per request."""
    return sum(isinstance(message, ModelResponse) for message in transcript)


def tools_called(messages: Sequence[ModelMessage]) -> list[str]:
    """Every tool the run asked for, in the order it asked."""
    return [
        part.tool_name
        for message in messages
        if isinstance(message, ModelResponse)
        for part in message.parts
        if isinstance(part, ToolCallPart)
    ]


def save_transcript(path: Path, transcript: Sequence[ModelMessage]) -> None:
    """The run's messages as JSON, so no later run has to pay for the same answer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(ModelMessagesTypeAdapter.dump_json(list(transcript), indent=2))


def load_transcript(path: Path) -> list[ModelMessage]:
    """A recorded run, back as messages."""
    return ModelMessagesTypeAdapter.validate_json(path.read_bytes())


def replay_model(transcript: Sequence[ModelMessage]) -> FunctionModel:
    """The recorded run's responses, in order, with no network and no cost.

    `FunctionModel` stamps its own name on everything it returns, so a naive
    replay reports `function:next_response:` as the model that answered -- which
    would make every replayed run in Phase 10's suite attributable to nothing.
    The name of the response that *ended* the recorded run is carried over
    instead, because that is the one `answering_model` reads.
    """
    responses = [message for message in transcript if isinstance(message, ModelResponse)]
    if not responses:
        raise ValueError("the transcript holds no model responses, so there is nothing to replay")
    remaining = iter(responses)

    def next_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        try:
            return next(remaining)
        except StopIteration:
            raise ValueError(
                "the transcript ran out of responses, so the replayed run asked more of "
                "the model than the recorded one did; they are not the same run"
            ) from None

    return FunctionModel(next_response, model_name=responses[-1].model_name)


def _through_gateway(gateway: ToolGateway, name: str) -> Callable[..., Awaitable[dict[str, Any]]]:
    async def call(**arguments: Any) -> dict[str, Any]:
        return await gateway.call(name, arguments)

    return call


def _final_response(messages: Sequence[ModelMessage]) -> ModelResponse:
    for message in reversed(messages):
        if isinstance(message, ModelResponse):
            return message
    raise ValueError("the run produced no model response")
