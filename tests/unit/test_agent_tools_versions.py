"""versions: what is actually live, read back from the registry.

No SparkSession and no cloud: the registry is a Protocol and the tests
inject a stub, so the parsing traps in MLflow's `sparkDatasourceInfo` tag
are exercised in the fast suite. Phase 8's finding is the reason this tool
reads rather than infers -- the serving endpoint was live on the retracted
v1 while the alias had already moved to v2, so a config-derived answer
would have been confidently wrong.
"""

from typing import Any

import pytest

from almanac.agent.tools import SPARK_DATASOURCE_TAG, feature_set_version, versions
from almanac.model.train import FEATURE_COLUMNS

_MODEL = "almanac_dbx.models.pr_review_sla_risk"
_RUN_ID = "817800814439176"

_TAG = (
    "path=abfss://lake@almanac.dfs.core.windows.net/silver/events/clean,version=91,format=delta\n"
    "path=abfss://lake@almanac.dfs.core.windows.net/features/author_activity,version=0,format=delta\n"
    "path=abfss://lake@almanac.dfs.core.windows.net/gold/fact_pull_request,version=1,format=delta"
)


class _StubRegistry:
    """The two `MlflowClient` reads `versions` makes, and nothing else."""

    def __init__(
        self, *, version: str = "2", tag: str | None = _TAG, run_id: str = _RUN_ID
    ) -> None:
        self._version = version
        self._tag = tag
        self._run_id = run_id
        self.alias_asked: tuple[str, str] | None = None

    def get_model_version_by_alias(self, name: str, alias: str) -> Any:
        self.alias_asked = (name, alias)
        return type("ModelVersion", (), {"version": self._version, "run_id": self._run_id})()

    def get_run(self, run_id: str) -> Any:
        # Raises on any other id, the way a real client 404s. A stub that
        # returned this run regardless could not express the failure where
        # the tool reports some other run's provenance.
        if run_id != self._run_id:
            raise KeyError(f"no run {run_id!r}")
        tags = {} if self._tag is None else {SPARK_DATASOURCE_TAG: self._tag}
        data = type("RunData", (), {"tags": tags})()
        return type("Run", (), {"data": data})()


def test_the_live_version_is_read_back_from_the_alias_not_configured() -> None:
    """Phase 8's finding, made structural: the answer comes from the registry."""
    registry = _StubRegistry(version="2")

    result = versions(registry, registered_model_name=_MODEL)

    assert result.model_version == "2"
    assert result.training_run_id == _RUN_ID
    assert result.registered_model_name == _MODEL
    assert registry.alias_asked == (_MODEL, "champion")


def test_training_data_delta_versions_come_from_the_run_that_trained_it() -> None:
    """The exact byte-level state of every input the champion's run read."""
    result = versions(_StubRegistry(), registered_model_name=_MODEL)

    assert result.training_data_delta_versions == {
        "abfss://lake@almanac.dfs.core.windows.net/silver/events/clean": 91,
        "abfss://lake@almanac.dfs.core.windows.net/features/author_activity": 0,
        "abfss://lake@almanac.dfs.core.windows.net/gold/fact_pull_request": 1,
    }


def test_a_non_delta_source_is_skipped_rather_than_crashed_on() -> None:
    """MLflow omits `version=` entirely for a non-Delta format, so a run that
    also read a CSV must not take the whole provenance answer down with it.
    """
    tag = _TAG + "\npath=abfss://lake@almanac.dfs.core.windows.net/raw/seed.csv,format=csv"

    result = versions(_StubRegistry(tag=tag), registered_model_name=_MODEL)

    assert len(result.training_data_delta_versions) == 3
    assert not any("seed.csv" in path for path in result.training_data_delta_versions)


def test_a_truncated_tag_is_refused_rather_than_reported_as_complete() -> None:
    """MLflow ellipsizes this tag at 8000 characters. A truncated one has
    silently lost tables, and reporting the survivors as the full provenance
    is a worse answer than refusing -- the number would look checkable.
    """
    truncated = _TAG[:-20] + "..."

    with pytest.raises(ValueError, match="truncated"):
        versions(_StubRegistry(tag=truncated), registered_model_name=_MODEL)


def test_one_path_at_two_versions_is_refused_rather_than_silently_resolved() -> None:
    """A run that read the same table twice at different versions has no single
    answer, and picking one would publish a version the model may not have seen.
    """
    tag = (
        _TAG
        + "\npath=abfss://lake@almanac.dfs.core.windows.net/silver/events/clean,version=92,format=delta"
    )

    with pytest.raises(ValueError, match="two versions"):
        versions(_StubRegistry(tag=tag), registered_model_name=_MODEL)


def test_a_run_with_no_datasource_tag_is_refused() -> None:
    """Provenance is the tool's whole purpose; an empty answer is not one."""
    with pytest.raises(ValueError, match=SPARK_DATASOURCE_TAG):
        versions(_StubRegistry(tag=None), registered_model_name=_MODEL)


def test_the_feature_set_version_tracks_membership_and_order() -> None:
    """It must move when the contracted feature set moves, in either sense:
    dropping a column is a different set, and reordering it is a different
    vector -- `positive_class_probability` indexes by position.
    """
    current = feature_set_version(FEATURE_COLUMNS)

    assert versions(_StubRegistry(), registered_model_name=_MODEL).feature_set_version == current
    assert feature_set_version(FEATURE_COLUMNS[:-1]) != current
    assert feature_set_version(list(reversed(FEATURE_COLUMNS))) != current
    assert feature_set_version(FEATURE_COLUMNS) == current
