"""Directional grounding: a claim that a factor raises risk must not cite a
contribution that lowers it.

`explain` returns each contribution with a sign; the answer's directional
language has to agree with it. The window's answer is correct here -- its top
three "decrease the risk of breach" and their contributions are negative -- so
the first failing fixture is a deliberately flipped copy of it.
"""

from pathlib import Path

from almanac.agent.bounded_agent import load_transcript
from almanac.agent.grounding import answer_and_returns, check_directions, verify

_TRANSCRIPTS = Path(__file__).parents[1] / "fixtures" / "transcripts"
_LIVE = _TRANSCRIPTS / "2026-09-10-live-predict-explain.json"
_FLIPPED = _TRANSCRIPTS / "2026-09-10-live-predict-explain-directions-flipped.json"

_EXPLAIN = (
    "explain",
    {
        "contributions": [
            {"feature": "prior_pr_count", "contribution": -2.46, "direction": "decreases_risk"},
            {"feature": "bot_share_to_date", "contribution": 0.38, "direction": "increases_risk"},
        ]
    },
)


def test_a_direction_that_matches_the_contribution_sign_agrees() -> None:
    checks = check_directions(
        'the contribution from "prior_pr_count" decreases the risk of breach', [_EXPLAIN]
    )

    check = next(c for c in checks if c.feature == "prior_pr_count")
    assert check.claimed == "decreases_risk"
    assert check.actual == "decreases_risk"
    assert check.agrees


def test_a_direction_against_the_contribution_sign_disagrees() -> None:
    checks = check_directions(
        'the contribution from "prior_pr_count" increases the risk of breach', [_EXPLAIN]
    )

    check = next(c for c in checks if c.feature == "prior_pr_count")
    assert check.claimed == "increases_risk"
    assert check.actual == "decreases_risk"
    assert not check.agrees


def test_an_answer_with_no_directional_language_produces_no_direction_check() -> None:
    assert check_directions('"prior_pr_count" is the strongest contribution', [_EXPLAIN]) == []


def test_a_sentence_that_points_both_ways_is_not_judged() -> None:
    checks = check_directions(
        'a lower "prior_pr_count" would increase risk, but here it decreases the risk of breach',
        [_EXPLAIN],
    )

    assert all(c.feature != "prior_pr_count" for c in checks)


def test_the_live_transcript_directions_all_agree() -> None:
    answer, returns = answer_and_returns(load_transcript(_LIVE))

    checks = check_directions(answer, returns)

    assert {c.feature for c in checks} == {
        "prior_pr_count",
        "prior_merge_rate",
        "prs_opened_to_date",
    }
    assert all(c.agrees for c in checks)


def test_the_flipped_transcript_is_ungrounded_on_direction() -> None:
    trace = verify(load_transcript(_FLIPPED))

    assert trace.verdict == "ungrounded"
    assert [c.feature for c in trace.directions if not c.agrees] == [
        "prior_pr_count",
        "prior_merge_rate",
        "prs_opened_to_date",
    ]
    assert any("prior_pr_count" in failure for failure in trace.failures)
