"""The golden set: committed transcripts, each with the bar it should clear.

Every entry is one JSON file under `tests/fixtures/golden/` -- a description, the
tools a correct run must call, the outcome expected, and either an inline
transcript or a pointer to a committed one. Adding a case is adding a file;
`scripts/build_golden_fixtures.py` builds the synthetic ones. The bar has two
parts: the answer verifies (`verify`), and the run called the tools its question
needs -- a provenance answer that never calls `versions` fails on the second even
when every number is real.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

from almanac.agent.bounded_agent import load_transcript, tools_called
from almanac.agent.grounding import verify

_FIXTURES = Path(__file__).parents[1] / "fixtures"
_GOLDEN = _FIXTURES / "golden"


@dataclass(frozen=True)
class GoldenResult:
    passes: bool
    reasons: tuple[str, ...]


def evaluate_golden(transcript: list[ModelMessage], requires_tools: list[str]) -> GoldenResult:
    """The golden bar: a grounded answer whose run called every tool its question needs."""
    reasons: list[str] = []
    if not verify(transcript).grounded:
        reasons.append("ungrounded")
    called = set(tools_called(transcript))
    if any(tool not in called for tool in requires_tools):
        reasons.append("missing-required-tool")
    return GoldenResult(passes=not reasons, reasons=tuple(reasons))


def _entries() -> list[dict[str, Any]]:
    return [json.loads(path.read_text()) for path in sorted(_GOLDEN.glob("*.json"))]


def _transcript(entry: dict[str, Any]) -> list[ModelMessage]:
    if "transcript" in entry:
        return list(ModelMessagesTypeAdapter.validate_python(entry["transcript"]))
    return load_transcript(_FIXTURES / entry["transcript_file"])


def _by_name(name: str) -> dict[str, Any]:
    return next(entry for entry in _entries() if entry["name"] == name)


def test_the_golden_set_is_not_empty_and_has_both_outcomes() -> None:
    outcomes = {entry["expect"]["passes"] for entry in _entries()}
    assert outcomes == {True, False}, "the golden set needs a known-good and a known-bad case"


@pytest.mark.parametrize("entry", _entries(), ids=lambda entry: entry["name"])
def test_each_golden_entry_clears_its_bar_exactly_as_declared(entry: dict[str, Any]) -> None:
    transcript = _transcript(entry)
    expect = entry["expect"]

    result = evaluate_golden(transcript, entry["requires_tools"])

    assert result.passes is expect["passes"]
    if not expect["passes"]:
        assert expect["reason"] in result.reasons
    if "missing" in expect:
        called = set(tools_called(transcript))
        assert all(tool not in called for tool in expect["missing"])
    if "ungrounded_claim" in expect:
        want = expect["ungrounded_claim"]
        claim = next(c for c in verify(transcript).claims if c.claim_type == want["claim_type"])
        assert not claim.traced
        assert claim.required_field == want["required_field"]


def test_a_provenance_answer_that_never_calls_versions_fails_even_with_every_number_real() -> None:
    """The exit-gate case: the golden bar is not only 'every number traces'."""
    entry = _by_name("provenance-answer-skips-versions")
    transcript = _transcript(entry)

    assert verify(transcript).grounded, "every number in this answer is real"
    assert not evaluate_golden(transcript, entry["requires_tools"]).passes
    assert "versions" not in tools_called(transcript)


def test_the_committed_window_transcript_is_the_known_bad_fixture() -> None:
    entry = _by_name("live-window-training-claim")
    assert entry["transcript_file"] == "transcripts/2026-09-10-live-predict-explain.json"

    result = evaluate_golden(_transcript(entry), entry["requires_tools"])

    assert not result.passes
    assert "ungrounded" in result.reasons


def test_the_gate_is_this_offline_pytest_step_not_a_separate_ci_job() -> None:
    """Design S6's gate -- "a deliberately hallucinated number fails CI" -- is the
    committed known-bad fixtures failing `evaluate_golden` in the normal `pytest -m
    "not network"` step. No endpoint, no replay harness beyond `load_transcript`,
    no new workflow job. Marking one `passes: true` turns this suite red.
    """
    known_bad = [e for e in _entries() if not e["expect"]["passes"]]
    assert known_bad, "the gate needs a committed hallucination to fail on"
    for entry in known_bad:
        assert not evaluate_golden(_transcript(entry), entry["requires_tools"]).passes
