"""The grounding trace: the structured record a build gate reads to see why it went red."""

import pytest
from pydantic import ValidationError

from almanac.agent.grounding import ClaimCheck, DirectionCheck, GroundingTrace, NumberCheck


def _round_tripped(model: object) -> object:
    dumped = model.model_dump_json()  # type: ignore[attr-defined]
    return type(model).model_validate_json(dumped)  # type: ignore[attr-defined]


def test_a_grounded_trace_round_trips_and_has_no_failures() -> None:
    trace = GroundingTrace(
        verdict="grounded",
        numbers=[
            NumberCheck(
                literal="0.0038", source="rounded", tool="predict", field_path="breach_risk"
            )
        ],
    )

    assert _round_tripped(trace) == trace
    assert trace.grounded
    assert trace.failures == []


def test_an_ungrounded_trace_carries_the_rows_that_failed() -> None:
    trace = GroundingTrace(
        verdict="ungrounded",
        numbers=[NumberCheck(literal="0.5", source="unmatched")],
        claims=[
            ClaimCheck(
                claim="trained on Delta versions 92",
                claim_type="training_provenance",
                required_field="training_data_delta_versions",
                traced=False,
                detail="92 traced to read_delta_versions, not training_data_delta_versions",
            )
        ],
        failures=[
            "0.5 appears in no tool result",
            "'trained on ... 92' did not trace to training_data_delta_versions",
        ],
    )

    assert _round_tripped(trace) == trace
    assert not trace.grounded
    assert len(trace.failures) == 2


def test_the_trace_forbids_an_unknown_field() -> None:
    with pytest.raises(ValidationError):
        GroundingTrace(verdict="grounded", surprise=1)  # type: ignore[call-arg]


def test_a_direction_check_records_agreement_with_the_contribution_sign() -> None:
    agree = DirectionCheck(
        claim="prior_pr_count decreases the risk of breach",
        feature="prior_pr_count",
        claimed="decreases_risk",
        actual="decreases_risk",
        agrees=True,
    )

    assert _round_tripped(agree) == agree
    assert agree.agrees
