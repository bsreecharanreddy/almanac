"""Tool schemas: a response the consumer could not have expected must fail to
build, not serialize with a null or a silently realigned shape.

Every model here round-trips through JSON -- the transport an MCP tool call
actually uses -- rather than only through Python construction.
"""

from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter, ValidationError

from almanac.agent.schemas import (
    Contribution,
    EntityKey,
    ExplainResult,
    FeatureProvenance,
    GetFeaturesResult,
    ModelProvenance,
    PredictOutput,
    PredictResult,
    Refusal,
    VersionsResult,
)
from almanac.model.train import FEATURE_COLUMNS

AS_OF = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)
ENTITY = EntityKey(repo_id=1, pr_number=42)


def zero_features() -> dict[str, float | None]:
    return {name: 0.0 for name in FEATURE_COLUMNS}


def round_tripped(model: object) -> object:
    dumped = model.model_dump_json()  # type: ignore[attr-defined]
    return type(model).model_validate_json(dumped)  # type: ignore[attr-defined]


def test_get_features_result_round_trips_through_json() -> None:
    result = GetFeaturesResult(
        entity=ENTITY,
        as_of=AS_OF,
        features=zero_features(),
        provenance=FeatureProvenance(delta_versions={"features/author_activity": 12}),
    )

    assert round_tripped(result) == result


def test_get_features_result_accepts_a_null_feature_value() -> None:
    """A reduced-era window genuinely has no `is_draft` (design doc S4.2).

    Reporting that honestly is this tool's job; refusing to *score* it is
    Task 4's `predict`, not this one -- so a null value here must round-trip,
    not fail validation the way a missing `provenance` does.
    """
    reduced_era = zero_features()
    reduced_era["is_draft"] = None

    result = GetFeaturesResult(
        entity=ENTITY,
        as_of=AS_OF,
        features=reduced_era,
        provenance=FeatureProvenance(delta_versions={"features/pr_state": 4}),
    )

    assert round_tripped(result) == result
    assert result.features["is_draft"] is None


def test_predict_result_round_trips_through_json() -> None:
    result = PredictResult(
        entity=ENTITY,
        as_of=AS_OF,
        breach_risk=0.42,
        provenance=ModelProvenance(model_version="3", delta_versions={"gold": 1}),
    )

    assert round_tripped(result) == result
    assert result.status == "scored"


def test_predict_refusal_round_trips_and_carries_no_score() -> None:
    refusal = Refusal(
        entity=ENTITY,
        as_of=AS_OF,
        missing_feature="is_draft",
        reason="is_draft is null on 100% of this window",
        provenance=FeatureProvenance(delta_versions={"features/pr_state": 4}),
    )

    assert round_tripped(refusal) == refusal
    assert refusal.status == "refused"
    assert "breach_risk" not in type(refusal).model_fields


def test_predict_output_union_discriminates_on_status() -> None:
    adapter: TypeAdapter[PredictOutput] = TypeAdapter(PredictOutput)

    scored = adapter.validate_python(
        {
            "status": "scored",
            "entity": {"repo_id": 1, "pr_number": 42},
            "as_of": AS_OF.isoformat(),
            "breach_risk": 0.1,
            "provenance": {"model_version": "3", "delta_versions": {"gold": 1}},
        }
    )
    refused = adapter.validate_python(
        {
            "status": "refused",
            "entity": {"repo_id": 1, "pr_number": 42},
            "as_of": AS_OF.isoformat(),
            "missing_feature": "is_draft",
            "reason": "null on 100% of this window",
            "provenance": {"delta_versions": {"features/pr_state": 4}},
        }
    )

    assert isinstance(scored, PredictResult)
    assert isinstance(refused, Refusal)


def test_explain_result_round_trips_and_direction_is_derived_from_sign() -> None:
    result = ExplainResult(
        entity=ENTITY,
        as_of=AS_OF,
        baseline=-1.2,
        contributions=[
            Contribution(feature="prior_pr_count", contribution=0.8, direction="increases_risk"),
            Contribution(feature="is_draft", contribution=-0.3, direction="decreases_risk"),
        ],
        top_k=2,
        provenance=ModelProvenance(model_version="3", delta_versions={"gold": 1}),
    )

    assert round_tripped(result) == result


def test_versions_result_round_trips_through_json() -> None:
    result = VersionsResult(
        registered_model_name="uc.almanac.pr_review_sla_risk",
        model_version="3",
        training_run_id="817800814439176",
        training_data_delta_versions={"events/clean": 91, "gold/pr_fact": 1},
        feature_set_version="v0",
    )

    assert round_tripped(result) == result


def test_missing_provenance_fails_validation_rather_than_serializing_with_a_null() -> None:
    with pytest.raises(ValidationError):
        GetFeaturesResult(entity=ENTITY, as_of=AS_OF, features=zero_features())  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        GetFeaturesResult(
            entity=ENTITY,
            as_of=AS_OF,
            features=zero_features(),
            provenance=None,  # type: ignore[arg-type]
        )


def test_features_missing_a_contracted_column_is_refused_not_realigned() -> None:
    incomplete = zero_features()
    del incomplete["prior_pr_count"]

    with pytest.raises(ValidationError, match="missing prior_pr_count"):
        GetFeaturesResult(
            entity=ENTITY,
            as_of=AS_OF,
            features=incomplete,
            provenance=FeatureProvenance(delta_versions={"features/author_activity": 12}),
        )


def test_features_with_an_uncontracted_column_is_refused() -> None:
    extra = zero_features()
    extra["not_a_feature"] = 1.0

    with pytest.raises(ValidationError, match="unexpected not_a_feature"):
        GetFeaturesResult(
            entity=ENTITY,
            as_of=AS_OF,
            features=extra,
            provenance=FeatureProvenance(delta_versions={"features/author_activity": 12}),
        )


def test_contribution_on_a_feature_not_in_the_contract_is_refused() -> None:
    with pytest.raises(ValidationError, match="FEATURE_COLUMNS"):
        Contribution(feature="not_a_feature", contribution=0.1, direction="increases_risk")


def test_contribution_direction_disagreeing_with_the_sign_is_refused() -> None:
    with pytest.raises(ValidationError, match="does not match"):
        Contribution(feature="prior_pr_count", contribution=0.8, direction="decreases_risk")


def test_as_of_must_be_timezone_aware() -> None:
    with pytest.raises(ValidationError, match="naive"):
        GetFeaturesResult(
            entity=ENTITY,
            as_of=datetime(2026, 9, 9, 12, 0),
            features=zero_features(),
            provenance=FeatureProvenance(delta_versions={"features/author_activity": 12}),
        )


def test_an_unexpected_top_level_field_is_refused() -> None:
    with pytest.raises(ValidationError):
        EntityKey(repo_id=1, pr_number=42, extra_field="surprise")  # type: ignore[call-arg]


def test_breach_risk_outside_zero_one_is_refused() -> None:
    with pytest.raises(ValidationError):
        PredictResult(
            entity=ENTITY,
            as_of=AS_OF,
            breach_risk=1.5,
            provenance=ModelProvenance(model_version="3", delta_versions={"gold": 1}),
        )
