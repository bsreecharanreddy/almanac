"""The gateway in front of the real server, not a stub of one.

`test_agent_gateway.py` proves the broker's own rules. This proves the two
things only the real four-tool surface can: that every reviewed tool actually
carries the read-only declaration the allow-list demands, and that a tool's own
refusal reaches the log as a completed call rather than as a gateway refusal --
two different events that would otherwise read identically to Phase 10.
"""

from pathlib import Path
from typing import Any

import anyio
import pytest
from pyspark.sql import SparkSession

from almanac.agent.gateway import AuditLog, ToolGateway, allow_list
from almanac.agent.mcp_server import TOOL_NAMES, ToolContext, build_server
from tests.agent_fixtures import (
    MODEL_NAME,
    REDUCED_AS_OF,
    REDUCED_ENTITY,
    RICH_AS_OF,
    RICH_ENTITY,
    TWO_ERA_EVENTS,
    StubRegistry,
    StubScoringModel,
    land_silver_and_features,
    silver_frame,
)

pytestmark = pytest.mark.spark


def _server(spark: SparkSession, tmp_path: Path) -> Any:
    silver_path, features_path = land_silver_and_features(
        spark, silver_frame(spark, TWO_ERA_EVENTS), tmp_path
    )
    return build_server(
        ToolContext(
            spark=spark,
            model=StubScoringModel(),
            registry=StubRegistry(),
            registered_model_name=MODEL_NAME,
            model_version="2",
            silver_path=silver_path,
            features_path=features_path,
        )
    )


def _gateway(spark: SparkSession, tmp_path: Path) -> tuple[ToolGateway, AuditLog]:
    server = _server(spark, tmp_path)
    audit = AuditLog(tmp_path / "audit" / "calls.jsonl")
    return ToolGateway(server, anyio.run(lambda: allow_list(server)), audit), audit


def _request(entity: Any, as_of: Any) -> dict[str, Any]:
    return {
        "request": {
            "entity": {"repo_id": entity.repo_id, "pr_number": entity.pr_number},
            "as_of": as_of.isoformat(),
        }
    }


def test_the_real_server_declares_every_reviewed_tool_read_only(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The allow-list's second gate, checked against the surface that ships."""
    allowed = anyio.run(lambda: allow_list(_server(spark, tmp_path)))

    assert allowed == frozenset(TOOL_NAMES)


def test_all_four_tools_run_through_the_gateway_and_land_in_the_log_in_order(
    spark: SparkSession, tmp_path: Path
) -> None:
    gateway, audit = _gateway(spark, tmp_path)
    rich = _request(RICH_ENTITY, RICH_AS_OF)
    arguments = {"versions": {}, "get_features": rich, "predict": rich, "explain": rich}

    async def session() -> None:
        for name, argument in arguments.items():
            await gateway.call(name, argument)

    anyio.run(session)

    records = audit.records()
    assert [record.tool for record in records] == list(arguments)
    assert {record.outcome for record in records} == {"ok"}
    assert records[0].result is not None and records[0].result["model_version"] == "2"


def test_a_tools_own_refusal_is_a_completed_call_not_a_gateway_refusal(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Two refusals live in this phase and they must not read alike: `is_draft`
    is missing from the window (the tool answers), versus the tool was never
    allowed to run (the gateway answers). Only the first carries a payload.
    """
    gateway, audit = _gateway(spark, tmp_path)

    payload = anyio.run(lambda: gateway.call("predict", _request(REDUCED_ENTITY, REDUCED_AS_OF)))

    record = audit.records()[-1]
    assert payload["status"] == "refused"
    assert payload["missing_feature"] == "is_draft"
    assert record.outcome == "ok"
    assert record.result == payload
