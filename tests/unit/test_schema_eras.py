from datetime import UTC, datetime

from almanac.explore.schema import SchemaEra, diff_field_paths, era_for, field_paths


def test_era_boundary_is_2015_01_01() -> None:
    assert era_for(datetime(2014, 12, 31, 23, 59, tzinfo=UTC)) is SchemaEra.LEGACY_V1
    assert era_for(datetime(2015, 1, 1, 0, 0, tzinfo=UTC)) is SchemaEra.MODERN_V2


def test_field_paths_are_two_levels_deep() -> None:
    event = {"id": "1", "actor": {"id": 2, "login": "x"}, "payload": {"action": "opened"}}
    assert field_paths(event) == {"id", "actor.id", "actor.login", "payload.action"}


def test_field_paths_handles_null_nested_object() -> None:
    # `org` is absent or null on non-org repos; this must not explode.
    assert field_paths({"id": "1", "org": None}) == {"id", "org"}


def test_diff_reports_all_three_sets() -> None:
    only_a, only_b, common = diff_field_paths({"a", "shared"}, {"b", "shared"})
    assert only_a == {"a"}
    assert only_b == {"b"}
    assert common == {"shared"}
