"""The paid window: every Phase 9 claim exercised against real tables and a live endpoint."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import anyio
from databricks.sdk import WorkspaceClient
from mlflow.tracking import MlflowClient
from pydantic_ai.models import Model
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from almanac.agent.bounded_agent import (
    Answered,
    answer,
    build_agent,
    gateway_tools,
    save_transcript,
)
from almanac.agent.gateway import AuditLog, ToolGateway, allow_list
from almanac.agent.mcp_server import ScoringModel, TablePaths, ToolContext, build_server
from almanac.agent.model_gateway import (
    FALLBACK_ENDPOINT,
    PRIMARY_ENDPOINT,
    Workspace,
    build_model,
    fallback_model,
    preflight,
)
from almanac.agent.schemas import EntityKey, ExplainInput, GetFeaturesInput, PredictInput
from almanac.agent.tools import explain, get_features, predict, versions
from almanac.model.registry import CHAMPION_ALIAS
from almanac.model.score_runner import load_champion
from almanac.spark import active_or_local_session

# Far enough apart that a repo with any activity moves between them, close
# enough that both sit inside one PR's life.
EARLY_OFFSET = timedelta(hours=1)
LATE_OFFSET = timedelta(hours=24)


def run_blocking[T](call: Callable[[], Awaitable[T]]) -> T:
    """One coroutine to completion, even inside a thread that already runs a loop.

    Databricks executes a `spark_python_task` inside an IPython shell that owns
    a running asyncio loop, and anyio refuses to nest one inside another. Same
    shell, second quirk: `cli.run_cli` exists because `SystemExit` does not mean
    there what it means anywhere else.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: anyio.run(call)).result()


@dataclass(frozen=True)
class WindowConfig:
    """What the window reads and where its evidence lands, in one record."""

    silver_path: str
    features_path: str
    registered_model_name: str
    rich_since: datetime
    rich_until: datetime
    reduced_since: datetime
    reduced_until: datetime
    evidence_dir: Path
    primary_endpoint: str = PRIMARY_ENDPOINT
    fallback_endpoint: str | None = FALLBACK_ENDPOINT
    agent_only: bool = False


@dataclass(frozen=True)
class WindowEntity:
    """One real work item to demonstrate on, and when it opened."""

    entity: EntityKey
    opened_at: datetime


def pick_entity(
    spark: SparkSession, *, silver_path: str, since: datetime, until: datetime
) -> WindowEntity | None:
    """The busiest open pull request in a window, or nothing if the window holds none.

    Busiest rather than first: a PR whose repo never moves would make the two
    `as_of` calls agree for an uninteresting reason, and reporting that as
    "point-in-time works" would be the panel-renders-so-it-is-fine mistake.
    """
    events = spark.read.format("delta").load(f"{silver_path}/clean")
    opened = events.where(
        (F.col("event_type") == "PullRequestEvent")
        & (F.col("event_action") == "opened")
        & (F.col("created_at") >= F.lit(since))
        & (F.col("created_at") < F.lit(until))
    ).select("repo_id", "pr_number", "created_at")

    # The neighbour side is bounded to the same window plus the 24 hours the
    # count looks at. Unbounded, this is a join against the whole 341M-row
    # table for a handful of rows -- the shape this project has already paid
    # for twice, in `compute_author_activity` and then in `as_of_join` itself.
    neighbours = events.where(
        (F.col("created_at") >= F.lit(since))
        & (F.col("created_at") < F.lit(until) + F.expr("INTERVAL 24 HOURS"))
    ).select("repo_id", F.col("created_at").alias("repo_event_at"))

    busiest = (
        opened.alias("o")
        .join(neighbours.alias("r"), on="repo_id")
        .where(
            (F.col("repo_event_at") >= F.col("o.created_at"))
            & (F.col("repo_event_at") < F.col("o.created_at") + F.expr("INTERVAL 24 HOURS"))
        )
        .groupBy("repo_id", "pr_number", "created_at")
        .count()
        .orderBy(F.col("count").desc(), F.col("repo_id"), F.col("pr_number"))
        .limit(1)
        # Epoch seconds rather than the timestamp itself: Spark hands the driver
        # a *naive* datetime already converted to the session zone, so stamping
        # UTC onto it moves the instant by the driver's offset -- four hours, in
        # the run that found this. The epoch has no zone to get wrong.
        .select("repo_id", "pr_number", F.unix_timestamp("created_at").alias("opened_epoch"))
        .collect()
    )
    if not busiest:
        return None
    row = busiest[0]
    return WindowEntity(
        entity=EntityKey(repo_id=int(row["repo_id"]), pr_number=int(row["pr_number"])),
        opened_at=datetime.fromtimestamp(int(row["opened_epoch"]), tz=UTC),
    )


def load_scoring_champion(model_uri: str) -> ScoringModel:
    """The champion as both scoring tools need it.

    `load_champion` returns the native LightGBM estimator and annotates it as
    `ProbabilityModel`, which is all `score_quarter` ever needed. The same
    object also carries `predict`, which is what `contribution_frame` calls.
    The cast states that, rather than loading the model a second time under a
    second name.
    """
    return cast(ScoringModel, load_champion(model_uri, "databricks-uc"))


def workspace_for(config: Any) -> Workspace:
    """The serving base URL and a bearer token, from the SDK's own auth.

    Read off the client rather than out of the environment, so a job running as
    a managed identity and a laptop holding a personal token take the same path.
    """
    authorization = config.authenticate().get("Authorization", "")
    if not authorization.startswith("Bearer "):
        raise ValueError(
            "the workspace client did not return a bearer token, so the OpenAI-compatible "
            "serving API cannot be called; check the profile's auth type"
        )
    return Workspace(
        base_url=f"{str(config.host).rstrip('/')}/serving-endpoints",
        api_key=authorization.removeprefix("Bearer "),
    )


def write_evidence(evidence_dir: Path, record: Mapping[str, Any]) -> Path:
    """The window's record on disk, as JSON, rewritable as the run progresses."""
    evidence_dir.mkdir(parents=True, exist_ok=True)
    path = evidence_dir / "window.json"
    path.write_text(json.dumps(record, indent=2, default=str))
    return path


def fallback_endpoint_from(argument: str) -> str | None:
    """`none` on the command line means no fallback at all, stated rather than implied."""
    return None if argument.lower() == "none" else argument


def agent_model(workspace: Workspace, primary: str, fallback: str | None) -> Model:
    """The chain the agent runs on: one endpoint, or two with the transient-only fallback."""
    model = build_model(primary, workspace)
    return model if fallback is None else fallback_model(model, build_model(fallback, workspace))


def publish_audit(audit: AuditLog, evidence_dir: Path) -> Path | None:
    """The live audit log, copied whole into the evidence directory, or nothing if empty."""
    if not audit.path.exists():
        return None
    evidence_dir.mkdir(parents=True, exist_ok=True)
    return Path(shutil.copyfile(audit.path, evidence_dir / audit.path.name))


def features_that_moved(
    early: Mapping[str, float | None], late: Mapping[str, float | None]
) -> list[str]:
    """Which features differ between the two instants."""
    return sorted(name for name, value in early.items() if late.get(name) != value)


def run_window(spark: SparkSession, config: WindowConfig) -> dict[str, Any]:
    """Every claim in the plan's exit gate, against real data, in one pass."""
    client = WorkspaceClient()

    # Gate 9d first, before anything reads a table: a missing endpoint should
    # fail here for free rather than 404 on the first call inside the window.
    fallback = [config.fallback_endpoint] if config.fallback_endpoint else []
    preflight(client.serving_endpoints, required=[config.primary_endpoint, *fallback])

    # Both URIs named: the version and its alias live in Unity Catalog, the run
    # that produced it lives in workspace MLflow, and `versions` reads both.
    registry = MlflowClient(tracking_uri="databricks", registry_uri="databricks-uc")
    live = versions(registry, registered_model_name=config.registered_model_name)
    model = load_scoring_champion(f"models:/{config.registered_model_name}@{CHAMPION_ALIAS}")

    rich = pick_entity(
        spark, silver_path=config.silver_path, since=config.rich_since, until=config.rich_until
    )
    if rich is None:
        raise ValueError(
            f"no opened pull request between {config.rich_since} and {config.rich_until}; "
            "the demo needs one real entity"
        )

    context = ToolContext(
        spark=spark,
        model=model,
        registry=registry,
        registered_model_name=config.registered_model_name,
        model_version=live.model_version,
        silver_path=config.silver_path,
        features_path=config.features_path,
    )
    record: dict[str, Any] = {
        "captured_at": datetime.now(UTC).isoformat(),
        "workspace_id": client.get_workspace_id(),
        "endpoints": {"primary": config.primary_endpoint, "fallback": config.fallback_endpoint},
        "versions": live.model_dump(mode="json"),
        "rich_entity": {
            "entity": rich.entity.model_dump(),
            "opened_at": rich.opened_at.isoformat(),
        },
    }
    if config.primary_endpoint != PRIMARY_ENDPOINT:
        # In the evidence itself, not only in a commit message: a substitute that
        # reads like the design's own choice is the silent-fallback failure this
        # phase exists to prevent, done by hand instead of by a framework.
        record["substitute_for"] = PRIMARY_ENDPOINT
    if not config.agent_only:
        record.update(_tool_demonstrations(context, config, rich))

    # Written before the agent runs, and again after. Everything above this
    # line has already been paid for, and gate 3 is that perishable evidence
    # survives whatever happens next -- including the step that fails.
    write_evidence(config.evidence_dir, record)
    try:
        record["agent"] = _run_agent(
            client, context, config, rich.entity, rich.opened_at + EARLY_OFFSET
        )
    except Exception as error:
        record["agent"] = {"failed": repr(error)}
    write_evidence(config.evidence_dir, record)
    return record


def _tool_demonstrations(
    context: ToolContext, config: WindowConfig, rich: WindowEntity
) -> dict[str, Any]:
    """The four tools on real tables: the as-of demo, a score, its contributions, a refusal."""
    spark, model, version = context.spark, context.model, context.model_version
    reduced = pick_entity(
        spark,
        silver_path=config.silver_path,
        since=config.reduced_since,
        until=config.reduced_until,
    )
    if reduced is None:
        raise ValueError(
            f"no opened pull request between {config.reduced_since} and {config.reduced_until}; "
            "the refusal demo needs one real reduced-era entity"
        )

    tables = TablePaths(
        silver_path=config.silver_path,
        features_path=config.features_path,
        silver_version=None,
        features_versions=None,
    )
    early_at, late_at = rich.opened_at + EARLY_OFFSET, rich.opened_at + LATE_OFFSET
    reduced_at = reduced.opened_at + EARLY_OFFSET

    early = get_features(spark, GetFeaturesInput(entity=rich.entity, as_of=early_at), **tables)
    late = get_features(spark, GetFeaturesInput(entity=rich.entity, as_of=late_at), **tables)
    again = get_features(spark, GetFeaturesInput(entity=rich.entity, as_of=early_at), **tables)

    def scored(entity: EntityKey, as_of: datetime) -> dict[str, Any]:
        request = PredictInput(entity=entity, as_of=as_of)
        return predict(spark, model, request, model_version=version, **tables).model_dump(
            mode="json"
        )

    def explained(entity: EntityKey, as_of: datetime) -> dict[str, Any]:
        request = ExplainInput(entity=entity, as_of=as_of)
        return explain(spark, model, request, model_version=version, **tables).model_dump(
            mode="json"
        )

    return {
        "reduced_entity": {
            "entity": reduced.entity.model_dump(),
            "opened_at": reduced.opened_at.isoformat(),
        },
        "as_of_demo": {
            "early": early.model_dump(mode="json"),
            "late": late.model_dump(mode="json"),
            "features_that_moved": features_that_moved(early.features, late.features),
            "recompute_is_byte_identical": again.features == early.features,
        },
        "predict": scored(rich.entity, early_at),
        "explain": explained(rich.entity, early_at),
        "refusal": scored(reduced.entity, reduced_at),
        "refusal_explain": explained(reduced.entity, reduced_at),
    }


def _run_agent(
    client: WorkspaceClient,
    context: ToolContext,
    config: WindowConfig,
    entity: EntityKey,
    as_of: datetime,
) -> dict[str, Any]:
    """One bounded run against the live endpoints, with its transcript kept."""
    # A UC Volume is a FUSE mount that refuses to append to a file that already
    # exists -- errno 29, "Illegal seek", on the second audit record of the
    # 2026-09-10 run. So the live log sits on the driver's own disk, where
    # append-only holds, and reaches the evidence directory as a whole copy.
    audit = AuditLog(Path(tempfile.mkdtemp(prefix="almanac-audit-")) / "audit.jsonl")
    try:
        server = build_server(context)
        gateway = ToolGateway(server, run_blocking(lambda: allow_list(server)), audit)
        chain = agent_model(
            workspace_for(client.config), config.primary_endpoint, config.fallback_endpoint
        )
        tools = run_blocking(lambda: gateway_tools(server, gateway))
        agent = build_agent(chain, tools, audit=audit)

        question = (
            f"What is the breach risk for pull request {entity.pr_number} in repository "
            f"{entity.repo_id} as of {as_of.isoformat()}, and which features drive it? "
            "Report the model version and the Delta versions behind your answer."
        )
        outcome = run_blocking(lambda: answer(agent, question))
        if isinstance(outcome, Answered):
            save_transcript(config.evidence_dir / "transcript.json", outcome.transcript)

        return {
            "question": question,
            "outcome": outcome.model_dump(mode="json", exclude={"transcript"}),
            "audit": [record.model_dump(mode="json") for record in audit.records()],
        }
    finally:
        # Published whether the run answered or not: the failed run is the one
        # whose audit trail matters most.
        publish_audit(audit, config.evidence_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--registered-model-name", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--rich-since", required=True)
    parser.add_argument("--rich-until", required=True)
    parser.add_argument("--reduced-since", required=True)
    parser.add_argument("--reduced-until", required=True)
    parser.add_argument("--primary-endpoint", default=PRIMARY_ENDPOINT)
    parser.add_argument("--fallback-endpoint", default=FALLBACK_ENDPOINT)
    parser.add_argument(
        "--agent-only",
        action="store_true",
        help="skip the tool demonstrations, which a previous run already captured",
    )
    args = parser.parse_args()

    evidence_dir = Path(args.evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    record = run_window(
        active_or_local_session("almanac-agent-window"),
        WindowConfig(
            silver_path=args.silver_path,
            features_path=args.features_path,
            registered_model_name=args.registered_model_name,
            rich_since=datetime.fromisoformat(args.rich_since),
            rich_until=datetime.fromisoformat(args.rich_until),
            reduced_since=datetime.fromisoformat(args.reduced_since),
            reduced_until=datetime.fromisoformat(args.reduced_until),
            evidence_dir=evidence_dir,
            primary_endpoint=args.primary_endpoint,
            fallback_endpoint=fallback_endpoint_from(args.fallback_endpoint),
            agent_only=args.agent_only,
        ),
    )

    # Printed as well as written: the run output survives even if the volume
    # write is the thing that fails.
    print(json.dumps(record, indent=2, default=str))
    failure = record.get("agent", {}).get("failed")
    if failure:
        print(f"the evidence landed but the agent failed: {failure}")
        return 1
    return 0
