"""Invoking dbt. The I/O boundary for Gold.

dbt-spark's session method calls ``builder.enableHiveSupport().getOrCreate()``,
which reuses the live session and cannot retrofit Delta or a persistent
metastore onto it. So the order is load bearing -- session first, dbt second
-- which is why Gold runs through this module, not a bare ``dbt`` command.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

from almanac.cli import run_cli
from almanac.config import Settings
from almanac.gold.sources import register_silver_sources
from almanac.spark import dbt_session

DEFAULT_PROJECT_DIR = Path("dbt")
SESSION_TARGET = "session"


@dataclass(frozen=True)
class GoldTarget:
    """Where one dbt invocation reads its project and writes its output.

    ``profiles_dir`` is separate from ``project_dir`` so the regression test
    can run a throwaway project against the profile this repo ships.
    """

    # warehouse is a str, metastore a Path, and the asymmetry is the point:
    # the warehouse may be an abfss:// URI, which Path would corrupt, while the
    # metastore is a Derby database file and is genuinely local.
    warehouse: str
    metastore: Path
    project_dir: Path = DEFAULT_PROJECT_DIR
    profiles_dir: Path | None = None
    name: str = SESSION_TARGET
    target_path: Path | None = None

    @property
    def is_local(self) -> bool:
        """Local compute, vs a Databricks endpoint that already exists."""
        return self.name == SESSION_TARGET

    def cli_flags(self) -> list[str]:
        flags = [
            "--project-dir",
            str(self.project_dir),
            "--profiles-dir",
            str(self.profiles_dir if self.profiles_dir is not None else DEFAULT_PROJECT_DIR),
            "--target",
            self.name,
        ]
        if self.target_path is not None:
            flags += ["--target-path", str(self.target_path)]
        return flags


def run_dbt(
    command: list[str], target: GoldTarget, *, silver_path: str | None = None
) -> dbtRunnerResult:
    """Run one dbt command against the local session or a remote warehouse.

    A SparkSession is built only for the local target. ``silver_path`` is
    Silver's base dir (holding ``clean`` / ``quarantine``); pass it for any
    command that selects from ``source('silver', ...)``. Registration is
    idempotent, and re-doing it every call beats assuming a prior process did.
    """
    if target.is_local:
        spark = dbt_session(warehouse=target.warehouse, metastore=target.metastore)
        if silver_path is not None:
            register_silver_sources(
                spark,
                clean_path=f"{silver_path}/clean",
                quarantine_path=f"{silver_path}/quarantine",
            )
    return dbtRunner().invoke([*command, *target.cli_flags()])


def main(argv: list[str] | None = None) -> int:
    settings = Settings()
    parser = argparse.ArgumentParser(description="Run dbt for the Gold layer.")
    parser.add_argument("--warehouse", default=str(settings.warehouse_dir))
    parser.add_argument("--metastore", type=Path, default=settings.metastore_dir)
    parser.add_argument("--project-dir", type=Path, default=DEFAULT_PROJECT_DIR)
    parser.add_argument("--profiles-dir", type=Path, default=None)
    parser.add_argument("--target", default=SESSION_TARGET)
    parser.add_argument("--target-path", type=Path, default=None)
    parser.add_argument(
        "--silver-path",
        default=None,
        help=(
            "Base dir holding Silver's clean/ and quarantine/ subdirs; "
            "registered into the metastore as silver.events / "
            "silver.events_quarantine before dbt runs. Required by any "
            "command that selects from a Gold model reading a source."
        ),
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="dbt command, e.g. `build`")
    args = parser.parse_args(argv)

    result = run_dbt(
        args.command,
        GoldTarget(
            warehouse=args.warehouse,
            metastore=args.metastore,
            project_dir=args.project_dir,
            profiles_dir=args.profiles_dir,
            name=args.target,
            target_path=args.target_path,
        ),
        silver_path=args.silver_path,
    )
    return 0 if result.success else 1


if __name__ == "__main__":
    run_cli(main)
