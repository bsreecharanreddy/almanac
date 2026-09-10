"""The paid window's runner, in the parts that can be checked without paying.

`run_window` itself needs a live workspace and is exercised in the window; what
is testable here is the two pieces most likely to be quietly wrong -- which
entity the demo picks, and where its credentials come from.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anyio
import pytest
from pydantic_ai.models.fallback import FallbackModel
from pyspark.sql import SparkSession

from almanac.agent.gateway import AuditLog, AuditRecord
from almanac.agent.model_gateway import FALLBACK_ENDPOINT, PRIMARY_ENDPOINT, Workspace
from almanac.agent.window import (
    agent_model,
    fallback_endpoint_from,
    features_that_moved,
    pick_entity,
    publish_audit,
    run_blocking,
    workspace_for,
    write_evidence,
)
from tests.agent_fixtures import pr_opened as opened
from tests.agent_fixtures import silver_frame

_SINCE = datetime(2025, 8, 1, tzinfo=UTC)
_UNTIL = datetime(2025, 9, 1, tzinfo=UTC)


class _Config:
    """`WorkspaceClient.config`'s two reads, with no workspace behind them."""

    def __init__(self, host: str, authorization: str | None) -> None:
        self.host = host
        self._authorization = authorization

    def authenticate(self) -> dict[str, str]:
        return {} if self._authorization is None else {"Authorization": self._authorization}


def _land(spark: SparkSession, tmp_path: Path, events: list[Any]) -> str:
    silver_path = str(tmp_path / "events")
    silver_frame(spark, events).write.format("delta").save(f"{silver_path}/clean")
    return silver_path


def test_workspace_for_takes_the_serving_base_url_and_the_bearer_token() -> None:
    workspace = workspace_for(_Config("https://example.azuredatabricks.net/", "Bearer abc123"))

    assert workspace.base_url == "https://example.azuredatabricks.net/serving-endpoints"
    assert workspace.api_key == "abc123"


def test_workspace_for_refuses_a_client_that_returns_no_bearer_token() -> None:
    """Failing here is free; failing on the first call is not."""
    with pytest.raises(ValueError, match="bearer token"):
        workspace_for(_Config("https://example.azuredatabricks.net", None))


def test_features_that_moved_names_only_the_features_that_differ() -> None:
    early = {"a": 1.0, "b": 2.0, "c": None}
    late = {"a": 1.0, "b": 3.0, "c": 4.0}

    assert features_that_moved(early, late) == ["b", "c"]


@pytest.mark.spark
def test_pick_entity_takes_the_busiest_pull_request_not_the_first(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A PR whose repo never moves would make the two as-of calls agree for an
    uninteresting reason, and reporting that as point-in-time working is the
    panel-renders-so-it-is-fine mistake.
    """
    quiet = opened(1, 100, datetime(2025, 8, 2, 9, tzinfo=UTC), "carol")
    busy = opened(2, 200, datetime(2025, 8, 3, 9, tzinfo=UTC), "dave")
    neighbours = [
        opened(2, 201 + n, datetime(2025, 8, 3, 10 + n, tzinfo=UTC), "dave") for n in range(3)
    ]
    silver_path = _land(spark, tmp_path, [quiet, busy, *neighbours])

    picked = pick_entity(spark, silver_path=silver_path, since=_SINCE, until=_UNTIL)

    assert picked is not None
    assert picked.entity.repo_id == 2
    assert picked.entity.pr_number == 200
    assert picked.opened_at == datetime(2025, 8, 3, 9, tzinfo=UTC)


@pytest.mark.spark
def test_pick_entity_returns_nothing_when_the_window_holds_no_opened_pull_request(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Nothing, so the caller can say which window was empty rather than crash."""
    silver_path = _land(spark, tmp_path, [opened(1, 100, datetime(2025, 8, 2, 9, tzinfo=UTC), "c")])

    assert pick_entity(spark, silver_path=silver_path, since=_UNTIL, until=_UNTIL) is None


def test_run_blocking_works_inside_a_thread_that_already_runs_a_loop() -> None:
    """The exact condition a Databricks `spark_python_task` runs under: an
    IPython shell owning a live asyncio loop, where `anyio.run` refuses to nest.
    Reproduced here rather than discovered again in a paid window.
    """

    async def nested() -> int:
        return run_blocking(_answer)

    async def _answer() -> int:
        return 42

    assert anyio.run(nested) == 42


def test_write_evidence_lands_a_readable_record_and_can_be_rewritten(tmp_path: Path) -> None:
    """Written once before the agent runs and again after, so a failure in the
    later step cannot take the evidence already paid for with it.
    """

    partial = write_evidence(tmp_path / "evidence", {"predict": {"breach_risk": 0.4}})
    assert json.loads(partial.read_text())["predict"]["breach_risk"] == 0.4

    full = write_evidence(tmp_path / "evidence", {"predict": {"breach_risk": 0.4}, "agent": {}})
    assert full == partial
    assert "agent" in json.loads(full.read_text())


_WORKSPACE = Workspace(base_url="https://example.invalid/serving-endpoints", api_key="k")
_SUBSTITUTE = "databricks-meta-llama-3-3-70b-instruct"


def test_agent_model_without_a_fallback_is_the_primary_alone() -> None:
    """The substitute run has no fallback, because the design's fallback cannot be parsed."""
    model = agent_model(_WORKSPACE, _SUBSTITUTE, None)

    assert not isinstance(model, FallbackModel)
    assert model.model_name == _SUBSTITUTE


def test_agent_model_with_a_fallback_chains_the_two_in_order() -> None:
    model = agent_model(_WORKSPACE, PRIMARY_ENDPOINT, FALLBACK_ENDPOINT)

    assert isinstance(model, FallbackModel)
    assert [inner.model_name for inner in model.models] == [PRIMARY_ENDPOINT, FALLBACK_ENDPOINT]


def test_none_on_the_command_line_means_no_fallback() -> None:
    assert fallback_endpoint_from("none") is None
    assert fallback_endpoint_from("NONE") is None
    assert fallback_endpoint_from(FALLBACK_ENDPOINT) == FALLBACK_ENDPOINT


def _record(tool: str) -> AuditRecord:
    return AuditRecord(tool=tool, arguments={}, outcome="ok", latency_ms=1.0)


def test_publish_audit_rewrites_the_copy_whole_rather_than_appending_to_it(
    tmp_path: Path,
) -> None:
    """A UC Volume refuses to append to an existing file, so the evidence copy
    is rewritten whole each time. Appending to it would also duplicate every
    earlier record wherever appending does work.
    """
    live = AuditLog(tmp_path / "live" / "audit.jsonl")
    evidence = tmp_path / "evidence"
    live.append(_record("predict"))
    live.append(_record("explain"))
    publish_audit(live, evidence)

    live.append(_record("versions"))
    published = publish_audit(live, evidence)

    assert published is not None
    assert [r.tool for r in AuditLog(published).records()] == ["predict", "explain", "versions"]


def test_publish_audit_writes_nothing_when_nothing_was_logged(tmp_path: Path) -> None:
    live = AuditLog(tmp_path / "live" / "audit.jsonl")

    assert publish_audit(live, tmp_path / "evidence") is None
    assert not (tmp_path / "evidence").exists()
