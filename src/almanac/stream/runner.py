"""The live stream as two job stages: `poll` lands events, `ingest` writes Silver.

Two stages rather than one fused process, and rather than two threads: the
poller is network-bound and needs no SparkSession, the ingest is a Spark
streaming query and needs no token. Splitting them keeps each stage's failure
attributable and lets the Databricks job run them as two tasks on one cluster
(infra/terraform/streaming.tf), the same one-runner-per-package shape
features/runner.py and gold/runner.py already use.
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

import httpx
from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.config import Settings
from almanac.pipeline.source import EventStreamConfig
from almanac.spark import active_or_local_session
from almanac.stream.ingest import late_event_count, stream_events, write_stream_silver
from almanac.stream.poller import PollerSession, resolve_token, run_poller

POLL = "poll"
INGEST = "ingest"


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
    spark: SparkSession, *, landing: str, silver_path: str, checkpoint: str, watermark_minutes: int
) -> None:
    """Drain whatever the landing zone holds, then stop.

    `available_now=True`, not the continuous default: the poll stage has
    already finished by the time this task runs, so a query that never
    terminates would hold the cluster until the job timeout for nothing. The
    continuous trigger stays available for a genuinely concurrent window.
    """
    events = stream_events(spark, landing, watermark=timedelta(minutes=watermark_minutes))
    query = write_stream_silver(events, silver_path, checkpoint, available_now=True)
    query.awaitTermination()
    print(f"[stream] stage=ingest late_events={late_event_count(query)}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one stage of the live event stream.")
    parser.add_argument("stage", choices=[POLL, INGEST])
    parser.add_argument("--landing", required=True, help="Landing zone for polled event JSONL.")
    parser.add_argument("--config", default="conf/sources/github_events.yml")
    parser.add_argument("--max-polls", type=int, default=30)
    parser.add_argument("--silver-path")
    parser.add_argument("--checkpoint")
    parser.add_argument("--watermark-minutes", type=int, default=10)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.stage == POLL:
        run_poll_stage(
            EventStreamConfig.load(Path(args.config)),
            Path(args.landing),
            max_polls=args.max_polls,
        )
        return 0

    missing = [n for n in ("silver_path", "checkpoint") if getattr(args, n) is None]
    if missing:
        # argparse cannot express "required only for this stage", and the
        # check has to happen before run_ingest_stage: a streaming query that
        # fails partway has already written a checkpoint directory.
        parser.error(
            f"stage {INGEST} requires " + ", ".join(f"--{n.replace('_', '-')}" for n in missing)
        )
    run_ingest_stage(
        active_or_local_session("almanac-stream"),
        landing=args.landing,
        silver_path=args.silver_path,
        checkpoint=args.checkpoint,
        watermark_minutes=args.watermark_minutes,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
