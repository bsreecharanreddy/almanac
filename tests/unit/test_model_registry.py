"""registered_model_name: pure string composition, no network -- the
live-registration half (register_champion) is exercised for real only
during Phase 4's cloud verification step, mirroring how Phase 3 Task 5
deferred primary_key_sql's live execution the same way.
"""

from almanac.model.registry import registered_model_name


def test_composes_the_three_part_uc_name() -> None:
    assert registered_model_name("almanac", "models") == "almanac.models.pr_review_sla_risk"


def test_the_model_name_is_overridable() -> None:
    assert (
        registered_model_name("almanac", "models", "custom_model") == "almanac.models.custom_model"
    )
