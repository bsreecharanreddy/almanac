"""The four tools over MCP, so the surface is reachable by something other than a Python import."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib.metadata import version as installed_version
from typing import Protocol, TypedDict

from mcp.server.mcpserver import MCPServer
from mcp.types import CallToolResult, ToolAnnotations
from pyspark.sql import SparkSession

from almanac.agent import tools
from almanac.agent.schemas import (
    ExplainInput,
    ExplainOutput,
    GetFeaturesInput,
    GetFeaturesResult,
    PredictInput,
    PredictOutput,
    VersionsResult,
)
from almanac.model.contributions import ContributionModel
from almanac.model.score import ProbabilityModel

SERVER_NAME = "almanac"

# MCP requires structured output to be a single JSON object. `predict` and
# `explain` return discriminated unions, which have no one object schema, so
# the SDK nests those under this key while the other two arrive unwrapped.
# Knowing that is this module's job, not every consumer's.
_UNION_ENVELOPE = "result"

# Stated once, asserted against the built server: an extra tool is a surface
# nobody reviewed, a missing one is a capability the agent never learns it has.
TOOL_NAMES = ("get_features", "predict", "explain", "versions")


class ScoringModel(ProbabilityModel, ContributionModel, Protocol):
    """The champion as both scoring tools need it: a probability, and the contributions."""


@dataclass(frozen=True)
class ToolContext:
    """What the four tools read from, bound once so each MCP tool takes only its own arguments."""

    spark: SparkSession
    model: ScoringModel
    registry: tools.ModelRegistry
    registered_model_name: str
    model_version: str
    silver_path: str
    features_path: str
    silver_version: int | None = None
    features_versions: Mapping[str, int] | None = None


class TablePaths(TypedDict):
    """Typed so `**` unpacking is checked at the call site rather than trusted."""

    silver_path: str
    features_path: str
    silver_version: int | None
    features_versions: Mapping[str, int] | None


def build_server(context: ToolContext) -> MCPServer:
    """The four tools, each declared read-only and each schema taken from `schemas.py`.

    The tool functions take their Pydantic input model as a single parameter
    rather than flattened fields: flattening would restate the contract a
    second time, which is the duplication that goes stale silently.
    """

    def get_features(request: GetFeaturesInput) -> GetFeaturesResult:
        """Point-in-time feature vector for one work item, as of one instant."""
        return tools.get_features(context.spark, request, **_paths(context))

    def predict(request: PredictInput) -> PredictOutput:
        """Breach risk for one work item, or a structured refusal naming a missing feature."""
        return tools.predict(
            context.spark,
            context.model,
            request,
            model_version=context.model_version,
            **_paths(context),
        )

    def explain(request: ExplainInput) -> ExplainOutput:
        """Per-feature contributions behind one work item's score, strongest pull first."""
        return tools.explain(
            context.spark,
            context.model,
            request,
            model_version=context.model_version,
            **_paths(context),
        )

    def versions() -> VersionsResult:
        """What is live: model version, training run, and the Delta versions it trained on."""
        return tools.versions(context.registry, registered_model_name=context.registered_model_name)

    server = MCPServer(name=SERVER_NAME, version=installed_version("almanac"))
    for function in (get_features, predict, explain, versions):
        server.add_tool(
            function,
            # Declared here, enforced structurally by the gateway in front of it.
            annotations=ToolAnnotations(read_only_hint=True),
            structured_output=True,
        )
    return server


def tool_payload(result: CallToolResult) -> dict[str, object]:
    """A called tool's own structured payload, envelope or not.

    Two shapes reaching four consumers is a shape a consumer could not have
    expected, which is the thing `schemas.py` exists to refuse; this is the
    one place that knows the difference.
    """
    if result.structured_content is None:
        raise ValueError(f"tool returned no structured content: {result.content!r}")
    content = dict(result.structured_content)
    nested = content.get(_UNION_ENVELOPE)
    return (
        dict(nested)
        if content.keys() == {_UNION_ENVELOPE} and isinstance(nested, dict)
        else content
    )


def _paths(context: ToolContext) -> TablePaths:
    """The table arguments every reading tool takes, forwarded rather than restated."""
    return TablePaths(
        silver_path=context.silver_path,
        features_path=context.features_path,
        silver_version=context.silver_version,
        features_versions=context.features_versions,
    )
