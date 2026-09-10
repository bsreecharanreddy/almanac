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


# A closed, small set on purpose. A claim of one of these types must trace to the
# field that type means -- not merely to some field with the right number. The
# window's "trained on Delta versions 92" is the reason: 92 was real, and it
# traced to the versions the tools *read*, not to what the champion trained on.
# An unrecognized claim shape is not failed; its numbers still go through
# `check_numbers`, and the trace records that no relationship rule applied.
_CLAIM_RULES: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    (
        "training_provenance",
        "training_data_delta_versions",
        re.compile(r"train(?:ed|ing)\b[^.]*?\bversions?\s+(\d+)", re.IGNORECASE),
    ),
    (
        "read_provenance",
        "read_delta_versions",
        re.compile(r"\b(?:read|reading|as of)\b[^.]*?\bversions?\s+(\d+)", re.IGNORECASE),
    ),
    (
        "model_version",
        "model_version",
        # "the model ... is version 2" -- but not "the champion was trained on ...
        # version 91", which is the next rule's claim. The `train` between anchor
        # and version is what separates a training claim from a model-version one.
        re.compile(r"\b(?:model|champion)\b(?:(?!\btrain)[^.])*?\bversion\s+(\d+)", re.IGNORECASE),
    ),
    (
        "score",
        "breach_risk",
        re.compile(r"\b(?:breach risk|score)\b[^.]*?\bis\s+(\d*\.?\d+)", re.IGNORECASE),
    ),
    (
        "baseline",
        "baseline",
        re.compile(r"\bbaseline\b[^.]*?\bis\s+(-?\d*\.?\d+)", re.IGNORECASE),
    ),
)

# The tool whose return carries each required field, for the "you never called it"
# message. Only fields that come from exactly one tool need an entry.
_SUBSTANTIATING_TOOL = {
    "training_data_delta_versions": "versions",
    "baseline": "explain",
}


def check_claims(answer: str, tool_returns: Sequence[tuple[str, object]]) -> list[ClaimCheck]:
    """One `ClaimCheck` per typed claim in the answer: did its number trace to the *right* field."""
    values = [
        (path, number)
        for tool, content in tool_returns
        for path, number in numeric_leaves(content, tool)
    ]
    checks: list[ClaimCheck] = []
    for claim_type, required_field, pattern in _CLAIM_RULES:
        for match in pattern.finditer(answer):
            literal = match.group(1)
            on_required = [v for path, v in values if required_field in path]
            traced = any(_classify(literal, v) is not None for v in on_required)
            checks.append(
                ClaimCheck(
                    claim=" ".join(match.group(0).split()),
                    claim_type=claim_type,
                    required_field=required_field,
                    traced=traced,
                    detail=None if traced else _why_untraced(literal, required_field, values),
                )
            )
    return checks


def _why_untraced(literal: str, required_field: str, values: Sequence[tuple[str, float]]) -> str:
    if not any(required_field in path for path, _ in values):
        tool = _SUBSTANTIATING_TOOL.get(required_field)
        called = f" -- call `{tool}`" if tool else ""
        return f"no tool in this run returned `{required_field}`{called}"
    elsewhere = next((path for path, v in values if _classify(literal, v) is not None), None)
    return (
        f"{literal} traced to `{elsewhere}`, not `{required_field}`"
        if elsewhere
        else f"{literal} traced to nothing"
    )


# Directional language, split by which way it moves the risk. A sentence naming a
# feature and exactly one of these says which direction the answer claims for it;
# a sentence with both (or neither) is ambiguous and not judged.
_RAISES = re.compile(
    r"\b(?:increase\w*|raise\w*|elevat\w*|heighten\w*|driv\w+ up|push\w* up|higher)\b",
    re.IGNORECASE,
)
_LOWERS = re.compile(
    r"\b(?:decreas\w*|lower\w*|reduc\w*|mitigat\w*|dampen\w*|driv\w+ down|push\w* down)\b",
    re.IGNORECASE,
)
_SENTENCE = re.compile(r"(?<=[.?!])\s+")


def _claimed_direction(sentence: str) -> Direction | None:
    raises, lowers = bool(_RAISES.search(sentence)), bool(_LOWERS.search(sentence))
    if raises == lowers:
        return None
    return "increases_risk" if raises else "decreases_risk"


def _signed_contributions(
    tool_returns: Sequence[tuple[str, object]],
) -> list[tuple[str, Direction]]:
    """Each `(feature, direction)` from an `explain` return -- from the sign if absent."""
    signed: list[tuple[str, Direction]] = []
    for _tool, content in tool_returns:
        rows = content.get("contributions") if isinstance(content, dict) else None
        for row in rows or []:
            feature = row.get("feature") if isinstance(row, dict) else None
            if not isinstance(feature, str):
                continue
            value = _as_float(row.get("contribution")) or 0.0
            from_sign: Direction = "increases_risk" if value >= 0 else "decreases_risk"
            stated = row.get("direction")
            direction: Direction = (
                stated if stated in ("increases_risk", "decreases_risk") else from_sign
            )
            signed.append((feature, direction))
    return signed


def check_directions(
    answer: str, tool_returns: Sequence[tuple[str, object]]
) -> list[DirectionCheck]:
    """One `DirectionCheck` per contribution the answer gives a direction to."""
    sentences = _SENTENCE.split(answer)
    checks: list[DirectionCheck] = []
    for feature, actual in _signed_contributions(tool_returns):
        for sentence in sentences:
            if feature not in sentence:
                continue
            claimed = _claimed_direction(sentence)
            if claimed is None:
                continue
            checks.append(
                DirectionCheck(
                    claim=" ".join(sentence.split()),
                    feature=feature,
                    claimed=claimed,
                    actual=actual,
                    agrees=claimed == actual,
                )
            )
            break
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
    """The run's answer against its tool returns.

    Ungrounded if a number is unmatched (Task 3), a typed claim's number traced
    to the wrong field or to none (Task 4), or a directional claim contradicts
    the contribution's sign (Task 7).
    """
    answer, returns = answer_and_returns(transcript)
    numbers = check_numbers(answer, returns)
    claims = check_claims(answer, returns)
    directions = check_directions(answer, returns)
    failures = (
        [f"{c.literal} appears in no tool return" for c in numbers if c.source == "unmatched"]
        + [f"{c.claim!r}: {c.detail}" for c in claims if not c.traced]
        + [
            f"{d.feature}: the answer has it {d.claimed.replace('_', ' ')}, "
            f"its contribution {d.actual.replace('_', ' ')}"
            for d in directions
            if not d.agrees
        ]
    )
    return GroundingTrace(
        verdict="ungrounded" if failures else "grounded",
        numbers=numbers,
        claims=claims,
        directions=directions,
        failures=failures,
    )
