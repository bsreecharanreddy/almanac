"""Relationship grounding: a claim's number must trace to the field that claim type means.

The window's answer had every number in a tool return and still stated one false
thing -- "trained on Delta versions 92" when 92 was the version the tools *read*.
"""

from pathlib import Path

from almanac.agent.bounded_agent import load_transcript
from almanac.agent.grounding import ClaimCheck, check_claims, verify

_LIVE = (
    Path(__file__).parents[1] / "fixtures" / "transcripts" / "2026-09-10-live-predict-explain.json"
)

_PREDICT_RETURN = {
    "breach_risk": 0.0038260014552166737,
    "provenance": {"model_version": "2", "read_delta_versions": {"events": 92}},
}
_VERSIONS_RETURN = {
    "model_version": "2",
    "training_data_delta_versions": {"events/clean": 91},
}


def _one(claims: list[ClaimCheck], claim_type: str) -> ClaimCheck:
    return next(c for c in claims if c.claim_type == claim_type)


def test_trained_on_a_read_version_is_untraced_when_versions_was_not_called() -> None:
    claims = check_claims(
        "it was trained on Delta versions 92 for events", [("predict", _PREDICT_RETURN)]
    )

    claim = _one(claims, "training_provenance")
    assert not claim.traced
    assert claim.required_field == "training_data_delta_versions"
    assert claim.detail is not None and "call `versions`" in claim.detail


def test_trained_on_the_actual_training_version_traces() -> None:
    claims = check_claims(
        "the champion was trained on events version 91",
        [("predict", _PREDICT_RETURN), ("versions", _VERSIONS_RETURN)],
    )

    assert _one(claims, "training_provenance").traced


def test_trained_on_92_when_versions_says_91_names_where_92_actually_traced() -> None:
    claims = check_claims(
        "it was trained on Delta versions 92",
        [("predict", _PREDICT_RETURN), ("versions", _VERSIONS_RETURN)],
    )

    claim = _one(claims, "training_provenance")
    assert not claim.traced
    assert claim.detail is not None and "read_delta_versions" in claim.detail


def test_read_version_claim_traces_to_read_delta_versions() -> None:
    claims = check_claims(
        "the tools read Delta version 92 for events", [("predict", _PREDICT_RETURN)]
    )

    assert _one(claims, "read_provenance").traced


def test_an_unrecognized_claim_shape_produces_no_claim_check() -> None:
    claims = check_claims("the weather today is fine", [("predict", _PREDICT_RETURN)])

    assert claims == []


def test_trained_on_a_version_is_not_also_read_as_a_model_version_claim() -> None:
    """'the champion was trained on ... version 91' anchors on 'champion' and ends
    on 'version 91', but the `train` between them makes it a training claim only --
    otherwise 91 fails against `model_version` (which is 2) for the wrong reason.
    """
    claims = check_claims(
        "on model version 2; the champion was trained on events/clean at Delta version 91",
        [("predict", _PREDICT_RETURN), ("versions", _VERSIONS_RETURN)],
    )

    assert {c.claim_type for c in claims} == {"model_version", "training_provenance"}
    assert _one(claims, "model_version").traced  # "model version 2" -> model_version
    assert _one(claims, "training_provenance").traced  # "trained on ... 91" -> training


def test_the_live_transcript_is_ungrounded_on_the_training_claim() -> None:
    trace = verify(load_transcript(_LIVE))

    assert trace.verdict == "ungrounded"
    training = _one(trace.claims, "training_provenance")
    assert not training.traced
    assert training.required_field == "training_data_delta_versions"
    assert any("trained on" in f.lower() for f in trace.failures)
    # the numbers themselves were all real -- the relationship is what failed
    assert all(n.source != "unmatched" for n in trace.numbers)


def test_the_live_transcripts_score_and_model_version_claims_do_trace() -> None:
    trace = verify(load_transcript(_LIVE))

    assert _one(trace.claims, "score").traced
    assert _one(trace.claims, "model_version").traced
