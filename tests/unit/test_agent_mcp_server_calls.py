"""Calling all four tools through MCP, against the same real Delta tables the
tools' own tests use.

The surface is asserted in `test_agent_mcp_server.py`; this is the other half
of the plan's gate -- a client lists the tools *and calls each one*, over the
protocol rather than by Python import, since a tool reachable only by import
is a library and not a platform.
"""

from pathlib import Path
from typing import Any

import anyio
import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from almanac.agent.mcp_server import TOOL_NAMES, ToolContext, build_server, tool_payload
from almanac.agent.tools import SPARK_DATASOURCE_TAG
from almanac.model.train import FEATURE_COLUMNS
from tests.agent_fixtures import (
    REDUCED_AS_OF,
    REDUCED_ENTITY,
    RICH_AS_OF,
    RICH_ENTITY,
    TWO_ERA_EVENTS,
    land_silver_and_features,
    silver_frame,
)

pytestmark = pytest.mark.spark

_MODEL_NAME = "almanac_dbx.models.pr_review_sla_risk"
_RUN_ID = "817800814439176"
_TAG = "path=abfss://lake@almanac.dfs.core.windows.net/silver/events/clean,version=91,format=delta"


class _StubModel:
    """Both halves the tools need: a probability, and a contribution matrix."""

    def predict_proba(self, features: pd.DataFrame) -> Any:
        return np.tile([0.3, 0.7], (len(features), 1))

    def predict(self, features: pd.DataFrame, **kwargs: Any) -> Any:
        row = [float(i) for i in range(len(FEATURE_COLUMNS))] + [0.25]
        return np.tile(row, (len(features), 1))


class _StubRegistry:
    def get_model_version_by_alias(self, name: str, alias: str) -> Any:
        # Raises on any other name, so a tool wired to a hardcoded model
        # rather than to its context cannot pass unnoticed.
        if name != _MODEL_NAME:
            raise KeyError(f"no registered model {name!r}")
        return type("ModelVersion", (), {"version": "2", "run_id": _RUN_ID})()

    def get_run(self, run_id: str) -> Any:
        data = type("RunData", (), {"tags": {SPARK_DATASOURCE_TAG: _TAG}})()
        return type("Run", (), {"data": data})()


def _server(spark: SparkSession, tmp_path: Path) -> Any:
    silver_path, features_path = land_silver_and_features(
        spark, silver_frame(spark, TWO_ERA_EVENTS), tmp_path
    )
    return build_server(
        ToolContext(
            spark=spark,
            model=_StubModel(),
            registry=_StubRegistry(),
            registered_model_name=_MODEL_NAME,
            model_version="2",
            silver_path=silver_path,
            features_path=features_path,
        )
    )


def _call(server: Any, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result = anyio.run(lambda: server.call_tool(name, arguments))
    assert result.is_error is not True, result.content
    return dict(tool_payload(result))


def _request(entity: Any, as_of: Any) -> dict[str, Any]:
    return {
        "request": {
            "entity": {"repo_id": entity.repo_id, "pr_number": entity.pr_number},
            "as_of": as_of.isoformat(),
        }
    }


def test_every_advertised_tool_can_actually_be_called(spark: SparkSession, tmp_path: Path) -> None:
    """Listing a tool and serving it are different claims. This asserts the second."""
    server = _server(spark, tmp_path)
    rich = _request(RICH_ENTITY, RICH_AS_OF)
    arguments = {"get_features": rich, "predict": rich, "explain": rich, "versions": {}}

    called = {name: _call(server, name, arguments[name]) for name in TOOL_NAMES}

    assert set(called) == set(TOOL_NAMES)


def test_get_features_returns_the_contracted_vector_over_the_protocol(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The vector survives JSON transport intact -- exactly FEATURE_COLUMNS."""
    payload = _call(_server(spark, tmp_path), "get_features", _request(RICH_ENTITY, RICH_AS_OF))

    assert payload["features"].keys() == set(FEATURE_COLUMNS)
    assert payload["provenance"]["delta_versions"]["events"] == 0


def test_predict_returns_a_score_and_explain_returns_ranked_contributions(
    spark: SparkSession, tmp_path: Path
) -> None:
    server = _server(spark, tmp_path)
    rich = _request(RICH_ENTITY, RICH_AS_OF)

    scored = _call(server, "predict", rich)
    explained = _call(server, "explain", rich)

    assert scored["status"] == "scored"
    assert scored["breach_risk"] == 0.7
    assert scored["provenance"]["model_version"] == "2"
    assert explained["status"] == "explained"
    assert len(explained["contributions"]) == explained["top_k"]


def test_the_refusal_survives_the_protocol_rather_than_becoming_an_error(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A refusal is a *result*, not a transport failure. If MCP surfaced it as
    an error the agent would see a broken tool instead of design doc S4.2's
    answer, and would have every reason to retry or work around it.
    """
    server = _server(spark, tmp_path)

    payload = _call(server, "predict", _request(REDUCED_ENTITY, REDUCED_AS_OF))

    assert payload["status"] == "refused"
    assert payload["missing_feature"] == "is_draft"
    assert "breach_risk" not in payload


def test_a_union_returning_tool_is_nested_and_a_model_returning_one_is_not(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Pinned rather than discovered later: MCP nests a discriminated union
    under `result` because structured output must be one JSON object, and
    leaves a plain model unwrapped. `tool_payload` is the only thing that
    should ever need to know that.
    """
    server = _server(spark, tmp_path)
    rich = _request(RICH_ENTITY, RICH_AS_OF)

    union = anyio.run(lambda: server.call_tool("predict", rich))
    plain = anyio.run(lambda: server.call_tool("get_features", rich))

    assert union.structured_content is not None
    assert plain.structured_content is not None
    assert union.structured_content.keys() == {"result"}
    assert "features" in plain.structured_content


def test_versions_reports_the_live_registry_over_the_protocol(
    spark: SparkSession, tmp_path: Path
) -> None:
    payload = _call(_server(spark, tmp_path), "versions", {})

    assert payload["model_version"] == "2"
    assert payload["training_run_id"] == _RUN_ID


def test_an_unregistered_tool_name_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    """The allow-list Task 8 builds on: a name that was never added cannot run."""
    server = _server(spark, tmp_path)

    with pytest.raises(Exception, match=r"drop_features|Unknown tool|not found"):
        anyio.run(lambda: server.call_tool("drop_features", {}))
