"""Mutation testing the verifier -- design S6, "not optional".

A verifier nobody tried to break is a control of unknown scope, which is the
`pseudonymity.py` failure again. Each entry breaks one check on purpose; the
probe is a call whose result must change when it does. A mutation that changes
nothing is a missing test, and this file is where that gets caught.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
import pytest
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
    ToolCallPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import Tool

from almanac.agent import bounded_agent, grounding
from almanac.agent.bounded_agent import (
    Answered,
    Ungrounded,
    answer_grounded,
    build_agent,
    load_transcript,
)
from almanac.agent.gateway import AuditLog
from almanac.agent.grounding import GroundingTrace, check_numbers, verify

_FIXTURES = Path(__file__).parents[1] / "fixtures"
_LIVE = load_transcript(_FIXTURES / "transcripts" / "2026-09-10-live-predict-explain.json")
_GROUNDED = list(
    ModelMessagesTypeAdapter.validate_python(
        json.loads((_FIXTURES / "golden" / "predict-and-explain-ground.json").read_text())[
            "transcript"
        ]
    )
)

Mutate = Callable[[pytest.MonkeyPatch], None]
Probe = Callable[[], bool]


def _numbers_all_trace(answer: str, returns: list[tuple[str, Any]]) -> bool:
    return all(check.source != "unmatched" for check in check_numbers(answer, returns))


def _drop_scientific_notation(mp: pytest.MonkeyPatch) -> None:
    mp.setattr(grounding, "_LITERAL", re.compile(r"(?<![\w.])[-+]?\d[\d,]*(?:\.\d+)?%?"))


def _widen_rounding_to_a_band(mp: pytest.MonkeyPatch) -> None:
    mp.setattr(grounding, "_MIN_SIGNIFICANT_DIGITS_TO_ROUND", 0)


def _delete_the_training_rule(mp: pytest.MonkeyPatch) -> None:
    kept = tuple(rule for rule in grounding._CLAIM_RULES if rule[0] != "training_provenance")
    mp.setattr(grounding, "_CLAIM_RULES", kept)


def _absent_required_field_is_a_pass(mp: pytest.MonkeyPatch) -> None:
    real = grounding.check_claims

    def lenient(answer: str, returns: list[tuple[str, Any]]) -> list[Any]:
        return [
            c.model_copy(update={"traced": True})
            if c.detail and "no tool in this run" in c.detail
            else c
            for c in real(answer, returns)
        ]

    mp.setattr(grounding, "check_claims", lenient)


def _invert_the_direction_comparison(mp: pytest.MonkeyPatch) -> None:
    real = grounding.check_directions

    def flipped(answer: str, returns: list[tuple[str, Any]]) -> list[Any]:
        return [c.model_copy(update={"agrees": not c.agrees}) for c in real(answer, returns)]

    mp.setattr(grounding, "check_directions", flipped)


_CASES: list[tuple[str, Mutate, Probe, bool]] = [
    (
        "scientific notation dropped from number extraction",
        _drop_scientific_notation,
        lambda: _numbers_all_trace("the risk is 1.5e-3", [("predict", {"breach_risk": 0.0015})]),
        True,
    ),
    (
        "rounding widened to a tolerance band",
        _widen_rounding_to_a_band,
        lambda: _numbers_all_trace("the risk is 0.004", [("predict", {"breach_risk": 0.0038})]),
        False,
    ),
    (
        "the training-provenance claim rule deleted",
        _delete_the_training_rule,
        lambda: verify(_LIVE).grounded,
        False,
    ),
    (
        "a required field no tool returned counts as traced",
        _absent_required_field_is_a_pass,
        lambda: verify(_LIVE).grounded,
        False,
    ),
    (
        "the directional sign comparison inverted",
        _invert_the_direction_comparison,
        lambda: verify(_GROUNDED).grounded,
        True,
    ),
]


@pytest.mark.parametrize("name, mutate, probe, correct", _CASES, ids=lambda case: case)
def test_each_verifier_check_has_a_mutation_the_suite_catches(
    monkeypatch: pytest.MonkeyPatch, name: str, mutate: Mutate, probe: Probe, correct: bool
) -> None:
    assert probe() is correct, "the probe does not reflect the real verifier"

    mutate(monkeypatch)

    assert probe() is not correct, f"nothing caught: {name}"


def _script(*texts: str) -> FunctionModel:
    """calls `predict`, then says each of `texts` in turn."""
    turns = iter(
        [
            ModelResponse(parts=[ToolCallPart(tool_name="predict", args={})]),
            *(ModelResponse(parts=[TextPart(content=text)]) for text in texts),
        ]
    )

    def next_turn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return next(turns)

    return FunctionModel(next_turn, model_name="stub")


def _agent(model: FunctionModel, tmp_path: Path) -> Any:
    async def predict(**_arguments: Any) -> dict[str, Any]:
        return {"breach_risk": 0.0038}

    tool = Tool.from_schema(
        predict, name="predict", description="p", json_schema={"type": "object"}
    )
    return build_agent(model, [tool], audit=AuditLog(tmp_path / "audit.jsonl"))


def test_removing_the_abstain_stops_a_failing_run_ending_ungrounded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`answer_grounded` reads `verify().grounded`, so a verifier that never fails
    turns a bad answer into an `Answered`. This is the mutation Task 5's abstain
    exists to make impossible.
    """
    failing = _script("the risk is 0.5", "the risk is 0.9")
    real = anyio.run(lambda: answer_grounded(_agent(failing, tmp_path), "q"))
    assert isinstance(real, Ungrounded)

    monkeypatch.setattr(
        bounded_agent, "verify", lambda _transcript: GroundingTrace(verdict="grounded")
    )
    mutated = anyio.run(lambda: answer_grounded(_agent(_script("the risk is 0.5"), tmp_path), "q"))
    assert isinstance(mutated, Answered)
