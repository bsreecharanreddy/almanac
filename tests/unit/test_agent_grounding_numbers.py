"""Numeric grounding: every literal in an answer is a tool return value, modulo rounding."""

from pathlib import Path

import pytest

from almanac.agent.bounded_agent import load_transcript
from almanac.agent.grounding import (
    NumberCheck,
    answer_and_returns,
    check_numbers,
    literals,
    numeric_leaves,
)

_LIVE = (
    Path(__file__).parents[1] / "fixtures" / "transcripts" / "2026-09-10-live-predict-explain.json"
)


def test_literals_ignores_quoted_feature_names_and_echoed_timestamps() -> None:
    answer = (
        'as of 2025-08-04T01:00:12+00:00 the risk is 0.0038, driven by "prior_pr_count" '
        "and version 2"
    )

    assert literals(answer) == ["0.0038", "2"]


def test_numeric_leaves_walks_nested_returns_and_reads_a_numeric_string() -> None:
    content = {"model_version": "2", "provenance": {"read_delta_versions": {"events": 92}}}

    assert set(numeric_leaves(content, "predict")) == {
        ("predict.model_version", 2.0),
        ("predict.provenance.read_delta_versions.events", 92.0),
    }


@pytest.mark.parametrize(
    ("literal", "value", "expected"),
    [
        ("92", 92.0, "exact"),
        ("0.003826", 0.0038260014552166737, "rounded"),
        ("0.0038", 0.0038260014552166737, "rounded"),
        ("0.004", 0.0038260014552166737, None),  # one sig digit -- too aggressive to be a rounding
        ("0.0038", 0.00389, "truncated"),
        ("2", 2.0, "exact"),
        ("3", 2.0, None),  # an integer never rounds
        ("1.76", 1.7634, "rounded"),
        ("50%", 0.5, "exact"),  # a percentage is tried against value*100
    ],
)
def test_the_rounding_rule_at_its_boundary(
    literal: str, value: float, expected: str | None
) -> None:
    checks = check_numbers(f"the number is {literal}", [("t", {"v": value})])

    assert checks[0].source == (expected or "unmatched")


def test_a_fabricated_number_is_unmatched_and_makes_the_run_ungrounded() -> None:
    checks = check_numbers("the risk is 0.5", [("predict", {"breach_risk": 0.0038})])

    assert checks == [NumberCheck(literal="0.5", source="unmatched")]


def test_the_live_transcript_grounds_every_number_it_states() -> None:
    """Every number in the window's answer came from a tool -- the false *claim*
    in it is Task 4's relationship check, not this one's.
    """
    answer, returns = answer_and_returns(load_transcript(_LIVE))
    checks = check_numbers(answer, returns)

    assert checks, "the answer states numbers"
    assert all(c.source != "unmatched" for c in checks)
    assert any(c.field_path.endswith("breach_risk") for c in checks if c.field_path)
    # the score is stated to fewer digits than predict returned
    assert any(c.literal == "0.003826" and c.source == "rounded" for c in checks)
