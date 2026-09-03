"""Launch shim for the §8.2 Photon A/B; ``compare`` logic is almanac.burn.photon.

    run --no-photon --out arm_off.json <paths>   # on the STANDARD cluster
    run --photon    --out arm_on.json  <paths>   # on the PHOTON cluster
    compare --off arm_off.json --on arm_on.json --usd-per-dbu 0.30 \\
        --usd-per-node-hour 0.249 --num-nodes 5  # once billing has settled
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

import httpx
from pyspark.sql import SparkSession

from almanac.burn.context import BurnContext, LakePaths
from almanac.burn.day import process_day
from almanac.burn.photon import ArmMeasurement, compare_arms
from almanac.cli import run_cli
from almanac.config import Settings
from almanac.gold.runner import DEFAULT_PROJECT_DIR, GoldTarget, run_dbt
from almanac.pipeline.source import SourceConfig
from almanac.spark import local_session

_CALIBRATION_DAY = date(2025, 8, 13)


def _session() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-photon-ab")


def measure_arm(
    spark: SparkSession, ctx: BurnContext, gold: GoldTarget, *, photon: bool, dbus: float
) -> ArmMeasurement:
    """The calibration slice through bronze + silver + gold, timed per layer."""
    result = process_day(spark, _CALIBRATION_DAY, ctx)

    started = time.monotonic()
    # No Path(): ctx.paths.silver is an abfss:// URI on a cluster, and Path
    # collapses the '//' into a relative path Spark cannot resolve.
    dbt_result = run_dbt(["build"], gold, silver_path=ctx.paths.silver)
    gold_seconds = time.monotonic() - started
    if not dbt_result.success:
        raise SystemExit("dbt build failed; this arm is not measurable")

    return ArmMeasurement(
        photon=photon,
        compressed_gb=result.compressed_gb,
        bronze_seconds=result.timings.bronze_seconds,
        silver_seconds=result.timings.silver_seconds,
        gold_seconds=gold_seconds,
        dbus_consumed=dbus,
    )


def _run(args: argparse.Namespace) -> int:
    spark = _session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    with httpx.Client() as client:
        ctx = BurnContext(
            paths=LakePaths(
                bronze=args.bronze_path, silver=args.silver_path, staging=args.staging_dir
            ),
            config=SourceConfig.load(args.source_config),
            client=client,
            settings=Settings(),
        )
        # project_dir/profiles_dir explicitly, never GoldTarget's relative
        # default: a job task's cwd is not the repo root, which is the same
        # thing that made --source-config's relative default fail (defect #3).
        gold = GoldTarget(
            warehouse=args.warehouse,
            metastore=args.metastore,
            project_dir=args.project_dir,
            profiles_dir=args.project_dir,
            target_path=args.target_path,
        )
        arm = measure_arm(spark, ctx, gold, photon=args.photon, dbus=args.dbus)

    payload = json.dumps(arm.__dict__, indent=2)
    args.out.write_text(payload)
    print(payload)
    return 0


def _compare(args: argparse.Namespace) -> int:
    off = [ArmMeasurement(**json.loads(p.read_text())) for p in args.off]
    on = [ArmMeasurement(**json.loads(p.read_text())) for p in args.on]
    comparison = compare_arms(
        off,
        on,
        usd_per_dbu=args.usd_per_dbu,
        usd_per_node_hour=args.usd_per_node_hour,
        num_nodes=args.num_nodes,
    )
    print(json.dumps(comparison.summary(), indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Photon A/B on the calibration slice.")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="measure one arm on the current cluster")
    run.add_argument("--photon", action=argparse.BooleanOptionalAction, required=True)
    run.add_argument("--bronze-path", required=True)
    run.add_argument("--silver-path", required=True)
    run.add_argument("--staging-dir", type=Path, required=True)
    run.add_argument("--warehouse", required=True)
    run.add_argument("--metastore", type=Path, required=True)
    run.add_argument("--source-config", type=Path, default=Path("conf/sources/gharchive.yml"))
    run.add_argument("--project-dir", type=Path, default=DEFAULT_PROJECT_DIR)
    # dbt writes target/ under --project-dir, which on a cluster is a workspace
    # path it cannot write to.
    run.add_argument("--target-path", type=Path, default=None)
    run.add_argument("--dbus", type=float, default=0.0, help="run's DBU total from the billing API")
    run.add_argument("--out", type=Path, required=True)
    run.set_defaults(func=_run)

    compare = sub.add_parser("compare", help="diff two measured arms into the §8.2 table")
    # nargs="+": one run per arm cannot separate a small effect from this
    # cluster's run-to-run noise, so the comparison takes replicates.
    compare.add_argument("--off", type=Path, nargs="+", required=True)
    compare.add_argument("--on", type=Path, nargs="+", required=True)
    compare.add_argument("--usd-per-dbu", type=float, required=True)
    compare.add_argument("--usd-per-node-hour", type=float, required=True)
    compare.add_argument("--num-nodes", type=int, required=True)
    compare.set_defaults(func=_compare)

    args = parser.parse_args(argv)
    exit_code: int = args.func(args)
    return exit_code


if __name__ == "__main__":
    run_cli(main)
