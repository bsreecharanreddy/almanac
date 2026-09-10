"""The MCP surface: four tools, schemas taken from Task 2's models, all read-only.

No SparkSession here -- listing tools never touches one, and the point of
these assertions is the *surface*, not what happens behind it. Calling
through to real Delta tables is `test_agent_mcp_server_calls.py`.
"""

from typing import Any, cast

import anyio
import pytest
from pyspark.sql import SparkSession

from almanac.agent.mcp_server import TOOL_NAMES, ToolContext, build_server
from almanac.agent.schemas import (
    ExplainInput,
    GetFeaturesInput,
    PredictInput,
)


def _context() -> ToolContext:
    """Nothing here is called -- listing a tool never reaches its body -- so the
    dependencies are placeholders rather than a Spark session nobody needs.
    """
    unused = cast(Any, object())
    return ToolContext(
        spark=cast(SparkSession, unused),
        model=unused,
        registry=unused,
        registered_model_name="almanac_dbx.models.pr_review_sla_risk",
        model_version="2",
        silver_path="/unused/events",
        features_path="/unused/features",
    )


def _tools() -> dict[str, Any]:
    server = build_server(_context())
    return {tool.name: tool for tool in anyio.run(server.list_tools)}


def test_the_server_exposes_exactly_the_four_tools() -> None:
    """Exactly four: an extra one is a surface nobody reviewed, and a missing
    one is a capability the agent will not know it has.
    """
    assert set(_tools()) == set(TOOL_NAMES)
    assert len(TOOL_NAMES) == 4


@pytest.mark.parametrize(
    ("tool_name", "model"),
    [
        ("get_features", GetFeaturesInput),
        ("predict", PredictInput),
        ("explain", ExplainInput),
    ],
)
def test_input_schemas_come_from_task_2s_models_rather_than_a_second_copy(
    tool_name: str, model: type
) -> None:
    """Two statements of one contract means one of them is wrong (CLAUDE.md
    item 4), and a hand-written schema is exactly that -- it would drift from
    the model the tool actually validates against and nothing would say so.
    """
    schema = _tools()[tool_name].input_schema

    served = schema["$defs"][model.__name__]["properties"]
    assert served.keys() == model.model_fields.keys()  # type: ignore[attr-defined]


def test_versions_takes_no_arguments() -> None:
    """It asks the registry what is live; there is nothing for a caller to vary."""
    assert not _tools()["versions"].input_schema.get("properties")


def test_every_tool_declares_a_structured_output_schema() -> None:
    """Structured data, never prose -- the decision Phase 10's verifier rests on."""
    for name, tool in _tools().items():
        assert tool.output_schema is not None, name


def test_every_tool_is_annotated_read_only() -> None:
    """The hint a client reads before deciding what a tool may do. Task 8
    enforces it structurally; this is the declaration that must not disagree.
    """
    for name, tool in _tools().items():
        assert tool.annotations is not None, name
        assert tool.annotations.read_only_hint is True, name
