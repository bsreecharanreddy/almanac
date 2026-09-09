"""Typed tool inputs and outputs: every response's shape is what a caller may rely on.

Pydantic's own validation is `contracts.py`'s pattern pointed at this surface --
a required field with no default refuses a shape the consumer could not have
expected, the same promise `contracts.enforce` makes for a governed Delta
table, made instead against a Python return value that an agent reads
directly and never computes a number from scratch.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from almanac.model.train import FEATURE_COLUMNS


class _Strict(BaseModel):
    """Every schema's base: no default hides a missing field, no surprise field passes silently."""

    model_config = ConfigDict(extra="forbid", frozen=True)


def _tz_aware(value: datetime) -> datetime:
    """Same guard as `pipeline.bronze.add_ingestion_metadata`: a naive `as_of`
    reads as driver-local time and would pick the wrong instant for the
    point-in-time join this argument feeds -- the governing principle.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            f"as_of must be timezone-aware, got naive {value!r}; "
            "a naive datetime silently picks the wrong instant for a point-in-time join"
        )
    return value


AsOf = Annotated[datetime, AfterValidator(_tz_aware)]


class EntityKey(_Strict):
    """The work item every tool call is about -- the same key `PREDICTIONS_CONTRACT` is keyed on."""

    repo_id: int
    pr_number: int


class FeatureProvenance(_Strict):
    """Which Delta version of each table read produced these feature values. No model involved."""

    delta_versions: dict[str, int] = Field(min_length=1)


class ModelProvenance(_Strict):
    """Which registered model version, reading which Delta versions, produced this number."""

    model_version: str = Field(min_length=1)
    delta_versions: dict[str, int] = Field(min_length=1)


class GetFeaturesInput(_Strict):
    entity: EntityKey
    as_of: AsOf


class GetFeaturesResult(_Strict):
    """`features` is exactly `FEATURE_COLUMNS` -- refused, not realigned, on any other shape."""

    entity: EntityKey
    as_of: AsOf
    features: dict[str, float]
    provenance: FeatureProvenance

    @model_validator(mode="after")
    def _matches_the_contracted_feature_set(self) -> GetFeaturesResult:
        missing = set(FEATURE_COLUMNS) - self.features.keys()
        extra = self.features.keys() - set(FEATURE_COLUMNS)
        if missing or extra:
            problems = [f"missing {name}" for name in sorted(missing)]
            problems += [f"unexpected {name}" for name in sorted(extra)]
            raise ValueError(f"features does not match FEATURE_COLUMNS: {'; '.join(problems)}")
        return self


class PredictInput(_Strict):
    entity: EntityKey
    as_of: AsOf


class PredictResult(_Strict):
    """A served score. `breach_risk` is a probability, `score.py`'s own unit -- not a logit."""

    status: Literal["scored"] = "scored"
    entity: EntityKey
    as_of: AsOf
    breach_risk: float = Field(ge=0.0, le=1.0)
    unit: Literal["probability_of_breach"] = "probability_of_breach"
    provenance: ModelProvenance


class PredictRefusal(_Strict):
    """No score, ever, in place of one the champion cannot honestly produce (design doc S4.2).

    Structured so an agent narrates it without inventing prose to fill the gap:
    which feature is missing, and why -- never a number.
    """

    status: Literal["refused"] = "refused"
    entity: EntityKey
    as_of: AsOf
    missing_feature: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    provenance: FeatureProvenance


PredictOutput = Annotated[PredictResult | PredictRefusal, Field(discriminator="status")]

Direction = Literal["increases_risk", "decreases_risk"]


class Contribution(_Strict):
    """One feature's pull on the prediction, from `model.contributions.contribution_frame`."""

    feature: str
    contribution: float
    direction: Direction

    @model_validator(mode="after")
    def _feature_is_contracted_and_direction_matches_its_sign(self) -> Contribution:
        if self.feature not in FEATURE_COLUMNS:
            raise ValueError(f"{self.feature!r} is not in FEATURE_COLUMNS")
        expected: Direction = "increases_risk" if self.contribution >= 0 else "decreases_risk"
        if self.direction != expected:
            raise ValueError(
                f"direction={self.direction!r} does not match "
                f"contribution={self.contribution} (expected {expected!r})"
            )
        return self


class ExplainInput(_Strict):
    entity: EntityKey
    as_of: AsOf
    top_k: int = Field(default=5, gt=0)


class ExplainResult(_Strict):
    """Task 1's contributions as structured rows -- feature, contribution, direction, baseline."""

    entity: EntityKey
    as_of: AsOf
    baseline: float
    contributions: list[Contribution]
    top_k: int = Field(gt=0)
    provenance: ModelProvenance


class VersionsResult(_Strict):
    """What is actually live, read from the registry -- never inferred from config (Phase 8)."""

    registered_model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    training_run_id: str = Field(min_length=1)
    training_data_delta_versions: dict[str, int] = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
