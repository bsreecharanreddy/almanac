"""Launch shim for the Tier 3 backfill; logic lives in almanac.burn.backfill."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import httpx
from pyspark.sql import SparkSession

from almanac.burn.backfill import backfill
from almanac.burn.checkpoint import BackfillCheckpoint
from almanac.burn.context import BurnContext, LakePaths
from almanac.config import Settings
from almanac.pipeline.source import SourceConfig
from almanac.spark import local_session

# Tier 3's derived span (STATUS.md); a run may pass any sub-range.
_Q3_2025_START = date(2025, 7, 1)
_Q3_2025_END = date(2025, 9, 30)


def _session() -> SparkSession:
    """The cluster's session on Databricks, a local one anywhere else."""
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-backfill")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tier 3 backfill over a date range.")
    parser.add_argument("--start", type=date.fromisoformat, default=_Q3_2025_START)
    parser.add_argument("--end", type=date.fromisoformat, default=_Q3_2025_END)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--staging-dir", type=Path, required=True)
    parser.add_argument("--checkpoint-dir", type=Path, required=True)
    parser.add_argument("--source-config", type=Path, default=Path("conf/sources/gharchive.yml"))
    args = parser.parse_args(argv)

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
        report = backfill(spark, args.start, args.end, ctx, BackfillCheckpoint(args.checkpoint_dir))

    print(json.dumps(report.summary(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
