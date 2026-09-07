"""The live stream as four job stages: poll, ingest, features, publish.

Separate stages rather than one fused process, and rather than threads: the
poller is network-bound and needs no SparkSession, the ingest is a Spark
streaming query and needs no token, and the publish talks to a billing online
store neither of the others touches. Splitting them keeps each failure
attributable and lets the Databricks job run them in order on one cluster
(infra/terraform/streaming.tf), the same one-runner-per-package shape
features/runner.py and gold/runner.py already use.

Together they are §4.6's whole chain: a live event reaches a served online
feature value, which is Phase 6's exit gate.
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

import httpx
from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.config import Settings
from almanac.features.runner import FeatureTableSpec, write_and_register
from almanac.pipeline.source import EventStreamConfig
from almanac.spark import active_or_local_session
from almanac.stream.features import compute_actor_stream_features, compute_repo_stream_features
from almanac.stream.ingest import (
    late_event_count,
    non_reduced_count,
    stream_events,
    write_stream_silver,
)
from almanac.stream.online_store import (
    PublishMode,
    load_client,
    publish_feature_table,
    require_store,
)
from almanac.stream.poller import PollerSession, resolve_token, run_poller

POLL = "poll"
INGEST = "ingest"
FEATURES = "features"
PUBLISH = "publish"

# The reduced-era payload supports entity activity and nothing else (§4.6),
# so these are the two entities the live feed can key on. Same spec shape as
# features/runner.py's FEATURE_TABLES, so both go through write_and_register.
STREAM_FEATURE_TABLES: list[FeatureTableSpec] = [
    FeatureTableSpec(
        "repo_stream_activity", compute_repo_stream_features, ["repo_id"], "event_time"
    ),
    FeatureTableSpec(
        "actor_stream_activity", compute_actor_stream_features, ["actor_login"], "event_time"
    ),
]


def run_poll_stage(config: EventStreamConfig, landing: Path, *, max_polls: int) -> None:
    """Poll the live feed for a bounded number of intervals.

    Bounded, never `max_polls=None`: a job task that polls forever is stopped
    only by a timeout, and the window this project runs is deliberately short
    (§4.6). The token is read from the environment by `resolve_token`, never
    passed as a job parameter, so it cannot land in a run's parameter list.
    """
    timeout = Settings().http_timeout_seconds
    with httpx.Client(timeout=timeout) as client:
        session = PollerSession(client=client, config=config, token=resolve_token(config.auth))
        stats = run_poller(session, landing, max_polls=max_polls)
    print(f"[stream] stage=poll polls={stats.polls} events_written={stats.events_written}")


def run_ingest_stage(
    spark: SparkSession, *, landing: str, silver_path: str, checkpoint: str, late_after_minutes: int
) -> None:
    """Drain whatever the landing zone holds, then stop.

    `available_now=True`, not the continuous default: the poll stage has
    already finished by the time this task runs, so a query that never
    terminates would hold the cluster until the job timeout for nothing. The
    continuous trigger stays available for a genuinely concurrent window.
    """
    events = stream_events(spark, landing, late_after=timedelta(minutes=late_after_minutes))
    query = write_stream_silver(events, silver_path, checkpoint, available_now=True)
    query.awaitTermination()
    # `late_events` is now a report, not a loss: since 2026-09-07 dedup is an
    # insert-only MERGE and nothing is dropped for being late, so `rows` should
    # account for every event polled. Reporting only `late_events` is what made
    # a 161-row loss invisible until it was reconstructed from raw JSONL.
    print(
        f"[stream] stage=ingest late_events={late_event_count(query)} "
        f"non_reduced={non_reduced_count(query)} "
        f"rows={spark.read.format('delta').load(silver_path).count()}"
    )


def run_features_stage(
    spark: SparkSession, *, silver_path: str, features_path: str, register: bool, schema: str
) -> None:
    """Build the online feature tables from the live Silver table.

    Reads `silver_path` directly, not `silver_path/clean`: the batch pipeline
    quarantines into a sibling directory, but this path has no quarantine tier
    to read past -- `write_stream_silver` lands every era it receives, and only
    refuses a row it could not deduplicate at all.
    """
    events = spark.read.format("delta").load(silver_path)
    for spec in STREAM_FEATURE_TABLES:
        path = write_and_register(
            spark, spec, events, features_path=features_path, register=register, schema=schema
        )
        rows = spark.read.format("delta").load(path).count()
        print(
            f"[stream] stage=features table={spec.name} rows={rows} "
            f"path={path} registered={register}"
        )


def run_publish_stage(
    spark: SparkSession, *, store_name: str, schema: str, online_schema: str, mode: PublishMode
) -> None:
    """Publish each registered stream feature table into the online store.

    The store is looked up, never created -- Terraform owns its lifecycle, and
    it is the one resource here that bills purely for existing.

    `online_schema` is separate from `schema` rather than derived from it:
    Databricks documents that an online table's *catalog* name must equal its
    backing Postgres database name, which the source catalog has no reason to
    satisfy. Settled by the 2026-09-07 run: that catalog must already exist and
    must be a **standard** catalog -- a Database Catalog is rejected outright
    ("Publishing table to a MANAGED_ONLINE_CATALOG is not currently
    supported"), and its schema is not created for you either.
    """
    client = load_client()
    store = require_store(client, name=store_name)
    for spec in STREAM_FEATURE_TABLES:
        publish_feature_table(
            client,
            spark,
            store=store,
            source=f"{schema}.{spec.name}",
            online=f"{online_schema}.{spec.name}",
            mode=mode,
        )
        print(f"[stream] stage=publish table={spec.name} mode={mode}")


# argparse cannot express "required only for this stage", and every one of
# these has to be caught before the stage runs: a streaming query that fails
# partway has already written a checkpoint directory, and a publish that
# fails partway has already started a sync pipeline.
_REQUIRED_BY_STAGE: dict[str, tuple[str, ...]] = {
    POLL: ("landing",),
    INGEST: ("landing", "silver_path", "checkpoint"),
    FEATURES: ("silver_path", "features_path"),
    PUBLISH: ("store_name", "online_schema"),
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one stage of the live event stream.")
    parser.add_argument("stage", choices=list(_REQUIRED_BY_STAGE))
    parser.add_argument("--landing", help="Landing zone for polled event JSONL.")
    parser.add_argument("--config", default="conf/sources/github_events.yml")
    parser.add_argument("--max-polls", type=int, default=30)
    parser.add_argument("--silver-path")
    parser.add_argument("--checkpoint")
    # Reporting threshold only: nothing is dropped for exceeding it.
    parser.add_argument("--late-after-minutes", type=int, default=10)
    parser.add_argument("--features-path")
    # Fully qualified (catalog.schema): register_feature_table emits
    # "{schema}.{table}", which is only a valid Unity Catalog name if the
    # catalog is already in it.
    parser.add_argument("--schema", default="almanac_dbx.features")
    parser.add_argument("--online-schema")
    parser.add_argument("--store-name")
    parser.add_argument("--publish-mode", default="TRIGGERED", choices=["TRIGGERED", "CONTINUOUS"])
    parser.add_argument(
        "--register",
        action="store_true",
        help="Register each table in UC with the PK, CDF and NOT NULL keys publishing requires.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    missing = [n for n in _REQUIRED_BY_STAGE[args.stage] if getattr(args, n) is None]
    if missing:
        flags = ", ".join(f"--{n.replace('_', '-')}" for n in missing)
        parser.error(f"stage {args.stage} requires {flags}")

    if args.stage == POLL:
        run_poll_stage(
            EventStreamConfig.load(Path(args.config)),
            Path(args.landing),
            max_polls=args.max_polls,
        )
    elif args.stage == INGEST:
        run_ingest_stage(
            active_or_local_session("almanac-stream"),
            landing=args.landing,
            silver_path=args.silver_path,
            checkpoint=args.checkpoint,
            late_after_minutes=args.late_after_minutes,
        )
    elif args.stage == FEATURES:
        run_features_stage(
            active_or_local_session("almanac-stream"),
            silver_path=args.silver_path,
            features_path=args.features_path,
            register=args.register,
            schema=args.schema,
        )
    else:
        run_publish_stage(
            active_or_local_session("almanac-stream"),
            store_name=args.store_name,
            schema=args.schema,
            online_schema=args.online_schema,
            mode=args.publish_mode,
        )
    return 0


if __name__ == "__main__":
    run_cli(main)
