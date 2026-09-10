"""Retry once, then abstain.

An answer whose numbers do not trace gets exactly one more turn, with the
verifier's failed rows handed back as the observation. If it still does not
ground, the run ends `Ungrounded` -- the last answer kept for the record, never
returned as a number to rely on.
"""

from pathlib import Path
from typing import Any

import anyio
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import Tool

from almanac.agent.bounded_agent import Answered, Ungrounded, answer_grounded, build_agent
from almanac.agent.gateway import AuditLog

_STUB_MODEL_NAME = "stub-endpoint"
_QUESTION = "what is the breach risk for pull request 1 in repository 2?"
_PREDICT_RETURN = {
    "breach_risk": 0.0038260014552166737,
    "provenance": {"model_version": "2", "read_delta_versions": {"events": 92}},
}


def _script(*responses: ModelResponse) -> FunctionModel:
    remaining = iter(responses)

    def next_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return next(remaining)

    return FunctionModel(next_response, model_name=_STUB_MODEL_NAME)


def _predict_tool() -> Tool[Any]:
    async def call(**_arguments: Any) -> dict[str, Any]:
        return _PREDICT_RETURN

    return Tool.from_schema(
        call, name="predict", description="predict", json_schema={"type": "object"}
    )


def _calls_predict() -> ModelResponse:
    return ModelResponse(parts=[ToolCallPart(tool_name="predict", args={})])


def _says(text: str) -> ModelResponse:
    return ModelResponse(parts=[TextPart(content=text)])


def _agent(model: FunctionModel, tmp_path: Path) -> Any:
    return build_agent(model, [_predict_tool()], audit=AuditLog(tmp_path / "audit.jsonl"))


def test_a_first_answer_that_grounds_is_returned_without_a_retry(tmp_path: Path) -> None:
    agent = _agent(_script(_calls_predict(), _says("the breach risk is 0.003826")), tmp_path)

    outcome = anyio.run(lambda: answer_grounded(agent, _QUESTION))

    assert isinstance(outcome, Answered)
    assert outcome.answer == "the breach risk is 0.003826"
    assert outcome.tools_called == ["predict"]


def test_an_ungrounded_answer_that_grounds_on_the_retry_returns_the_second_answer(
    tmp_path: Path,
) -> None:
    agent = _agent(
        _script(
            _calls_predict(),
            _says("the breach risk is 0.5"),
            _says("the breach risk is 0.003826"),
        ),
        tmp_path,
    )

    outcome = anyio.run(lambda: answer_grounded(agent, _QUESTION))

    assert isinstance(outcome, Answered)
    assert outcome.answer == "the breach risk is 0.003826"


def test_an_answer_that_will_not_ground_twice_ends_ungrounded_after_one_retry(
    tmp_path: Path,
) -> None:
    agent = _agent(
        _script(
            _calls_predict(),
            _says("the breach risk is 0.5"),
            _says("the breach risk is 0.9"),
        ),
        tmp_path,
    )

    outcome = anyio.run(lambda: answer_grounded(agent, _QUESTION))

    assert isinstance(outcome, Ungrounded)
    assert outcome.retries == 1
    assert outcome.answer == "the breach risk is 0.9"
    assert not outcome.grounding.grounded
    assert any("0.9" in failure for failure in outcome.grounding.failures)


def test_the_retry_spends_a_budgeted_turn_rather_than_getting_a_free_one(tmp_path: Path) -> None:
    """`turn_limit=2` is exactly the first answer's cost -- one tool call and one
    reply. The retry has no turn left, so the run abstains without asking the
    model again; the two-response script proves the third call never happened.
    """
    agent = _agent(_script(_calls_predict(), _says("the breach risk is 0.5")), tmp_path)

    outcome = anyio.run(lambda: answer_grounded(agent, _QUESTION, turn_limit=2))

    assert isinstance(outcome, Ungrounded)
    assert outcome.retries == 1
    assert outcome.answer == "the breach risk is 0.5"
