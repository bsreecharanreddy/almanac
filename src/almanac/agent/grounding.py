"""Grounding: every number in an answer traces to a tool return, or the run is ungrounded.

Task 2 defines the trace; Task 3 fills its numbers, Task 4 its claims, Task 7 its
directions. The verdict is a decision, not a score -- design doc S6,
"deterministic, not LLM-as-judge" -- because a build gate needs a yes or no.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterator, Sequence
from typing import Literal

from pydantic import Field
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolReturnPart

from almanac.agent.schemas import Strict

NumberSource = Literal["exact", "rounded", "truncated", "unmatched"]
Direction = Literal["increases_risk", "decreases_risk"]

# A quoted span may hold a feature name with a digit ("pr_1"); an echoed
# timestamp is neither computed nor something a rounding rule applies to. Both
# are stripped before literals are pulled from the prose.
# ceiling: a number the model deliberately quotes ("the score is \"0.0038\"")
# is dropped with the span. An answer should not quote its numbers, and none
# of the recorded ones do; widen the strip if that stops being true.
_QUOTED = re.compile(r"\"[^\"]*\"|'[^']*'|`[^`]*`")
_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?"
)
_LITERAL = re.compile(r"(?<![\w.])[-+]?\d[\d,]*(?:\.\d+)?(?:[eE][-+]?\d+)?%?")

# A rounding match needs the literal to say something: "0.004" for a returned
# 0.0038 is one significant digit and too aggressive to call a rounding.
_MIN_SIGNIFICANT_DIGITS_TO_ROUND = 2


class NumberCheck(Strict):
    """One numeric literal from the answer, and the tool value it did or did not trace to."""

    literal: str
    source: NumberSource
    tool: str | None = None
    field_path: str | None = None


class ClaimCheck(Strict):
    """One typed claim, and whether its number traced to the field that claim type requires."""

    claim: str
    claim_type: str
    required_field: str
    traced: bool
    detail: str | None = None


class DirectionCheck(Strict):
    """One directional statement from the answer, checked against the contribution's sign."""

    claim: str
    feature: str
    claimed: Direction
    actual: Direction
    agrees: bool


class GroundingTrace(Strict):
    """Everything the verifier examined for one answer, and its verdict.

    One per agent run, serialized beside its transcript (Task 9). `failures` is
    the human-readable list of what went wrong -- empty exactly when the verdict
    is `grounded`.
    """

    verdict: Literal["grounded", "ungrounded"]
    numbers: list[NumberCheck] = Field(default_factory=list)
    claims: list[ClaimCheck] = Field(default_factory=list)
    directions: list[DirectionCheck] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)

    @property
    def grounded(self) -> bool:
        return self.verdict == "grounded"


def _as_float(value: object) -> float | None:
    """A finite number, or a string that is entirely one -- else None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str):
        try:
            parsed = float(value.replace(",", "").strip())
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def numeric_leaves(content: object, path: str = "") -> Iterator[tuple[str, float]]:
    """Every numeric leaf of a tool return, with its dotted path.

    A numeric string (`model_version` is `"2"`) counts: the model reads the
    JSON as text and states `2`, so the verifier has to match it there.
    """
    if isinstance(content, dict):
        for key, value in content.items():
            yield from numeric_leaves(value, f"{path}.{key}" if path else str(key))
    elif isinstance(content, (list, tuple)):
        for index, value in enumerate(content):
            yield from numeric_leaves(value, f"{path}[{index}]")
    else:
        number = _as_float(content)
        if number is not None:
            yield path, number


def literals(answer: str) -> list[str]:
    """The numeric literals a reader would say the answer states.

    Quoted spans and echoed timestamps come out first -- see `_QUOTED`.
    """
    prose = _TIMESTAMP.sub(" ", _QUOTED.sub(" ", answer))
    return [match.group() for match in _LITERAL.finditer(prose)]


def _decimals(literal: str) -> int:
    return len(literal.split(".", 1)[1]) if "." in literal else 0


def _significant_digits(literal: str) -> int:
    digits = literal.lstrip("+-").replace(".", "").lstrip("0")
    return len(digits) or 1


def _classify(literal: str, value: float) -> NumberSource | None:
    """How `value` accounts for `literal`, or None if it does not.

    `literal` written to `d` decimals is grounded by `value` when they are equal,
    when `value` rounds to it at `d`, or when `value` truncated at `d` is it --
    but a rounding match needs the literal to carry at least two significant
    digits, so an aggressive `0.004` for a returned `0.0038` is unmatched, not
    grounded. Integers never round.
    """
    is_percent = literal.endswith("%")
    bare = literal.rstrip("%")
    target = _as_float(bare)
    if target is None:
        return None
    candidates = [value, value * 100] if is_percent else [value]
    decimals = _decimals(bare)
    may_round = decimals > 0 and _significant_digits(bare) >= _MIN_SIGNIFICANT_DIGITS_TO_ROUND
    for candidate in candidates:
        if candidate == target:
            return "exact"
        if may_round:
            if f"{candidate:.{decimals}f}" == f"{target:.{decimals}f}":
                return "rounded"
            factor = 10**decimals
            truncated = math.trunc(candidate * factor) / factor
            if f"{truncated:.{decimals}f}" == f"{target:.{decimals}f}":
                return "truncated"
    return None


def check_numbers(answer: str, tool_returns: Sequence[tuple[str, object]]) -> list[NumberCheck]:
    """One `NumberCheck` per literal in the answer, in the order the answer states them."""
    values = [
        (tool, path, number)
        for tool, content in tool_returns
        for path, number in numeric_leaves(content, tool)
    ]
    checks: list[NumberCheck] = []
    for literal in literals(answer):
        match = next(
            (
                (tool, path, source)
                for tool, path, number in values
                if (source := _classify(literal, number)) is not None
            ),
            None,
        )
        if match is None:
            checks.append(NumberCheck(literal=literal, source="unmatched"))
        else:
            tool, path, source = match
            checks.append(NumberCheck(literal=literal, source=source, tool=tool, field_path=path))
    return checks


def answer_and_returns(transcript: Sequence[ModelMessage]) -> tuple[str, list[tuple[str, object]]]:
    """The final answer text, and every tool return in the run, in call order."""
    answer = ""
    for message in reversed(transcript):
        if isinstance(message, ModelResponse):
            answer = "".join(p.content for p in message.parts if isinstance(p, TextPart))
            break
    returns = [
        (part.tool_name, part.content)
        for message in transcript
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, ToolReturnPart)
    ]
    return answer, returns


def verify(transcript: Sequence[ModelMessage]) -> GroundingTrace:
    """The run's answer against its tool returns. Ungrounded if any number is unmatched."""
    answer, returns = answer_and_returns(transcript)
    numbers = check_numbers(answer, returns)
    failures = [
        f"{c.literal} appears in no tool return" for c in numbers if c.source == "unmatched"
    ]
    return GroundingTrace(
        verdict="ungrounded" if failures else "grounded",
        numbers=numbers,
        failures=failures,
    )
