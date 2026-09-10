"""The first transcript captured live, replayed through the agent for free.

Recorded in the 2026-09-10 window against `databricks-meta-llama-3-3-70b-instruct`,
standing in for a primary the workspace refused -- see
`docs/findings/2026-09-10-agent-layer-window.md`.
"""

from pathlib import Path
from typing import Any

import anyio
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.tools import Tool

from almanac.agent.bounded_agent import (
    Answered,
    answer,
    build_agent,
    load_transcript,
    replay_model,
    tools_called,
)
from almanac.agent.gateway import AuditLog
from almanac.agent.model_gateway import MODEL_CALL

_LIVE = (
    Path(__file__).parents[1] / "fixtures" / "transcripts" / "2026-09-10-live-predict-explain.json"
)
_ANSWERED_BY = "meta-llama-3.3-70b-instruct-121024"


def _question(transcript: list[ModelMessage]) -> str:
    first = transcript[0]
    assert isinstance(first, ModelRequest)
    prompt = first.parts[0]
    assert isinstance(prompt, UserPromptPart)
    assert isinstance(prompt.content, str)
    return prompt.content


def _recorded_answer(transcript: list[ModelMessage]) -> str:
    final = transcript[-1]
    assert isinstance(final, ModelResponse)
    return "".join(part.content for part in final.parts if isinstance(part, TextPart))


def _tools_answering_from(transcript: list[ModelMessage]) -> list[Tool[Any]]:
    """The tools answer from the recording too, so this replays the loop, not the tools."""
    returns: dict[str, list[Any]] = {}
    for message in transcript:
        if isinstance(message, ModelRequest):
            for part in message.parts:
                if isinstance(part, ToolReturnPart):
                    returns.setdefault(part.tool_name, []).append(part.content)

    def recorded(name: str) -> Tool[Any]:
        remaining = iter(returns[name])

        async def call(**_arguments: Any) -> Any:
            return next(remaining)

        return Tool.from_schema(call, name=name, description=name, json_schema={"type": "object"})

    return [recorded(name) for name in returns]


def test_the_live_transcript_replays_through_the_agent_to_the_answer_it_gave(
    tmp_path: Path,
) -> None:
    """Each replayed request is recorded again, under the model that answered it live."""
    transcript = load_transcript(_LIVE)
    audit = AuditLog(tmp_path / "audit.jsonl")
    agent = build_agent(replay_model(transcript), _tools_answering_from(transcript), audit=audit)

    outcome = anyio.run(lambda: answer(agent, _question(transcript)))

    assert isinstance(outcome, Answered)
    assert outcome.answer == _recorded_answer(transcript)
    assert outcome.model == _ANSWERED_BY
    assert outcome.tools_called == tools_called(transcript) == ["predict", "explain"]
    assert [(r.tool, r.model) for r in audit.records()] == [(MODEL_CALL, _ANSWERED_BY)] * 3
