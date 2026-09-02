"""Invoking dbt. The I/O boundary for Gold.

dbt-spark's session method builds its connection with
``SparkSession.builder.enableHiveSupport().getOrCreate()``, which **reuses**
whatever session is already live in the process. That is the hook this
module exists for: the session has to already carry Delta and a persistent
metastore before dbt asks for one, because ``getOrCreate`` returns the
running session and cannot retrofit either onto it.

So the order here is load bearing -- session first, dbt second -- and it is
why Gold is invoked through this module rather than through a bare ``dbt``
command line.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

from almanac.config import Settings
from almanac.spark import dbt_session

DEFAULT_PROJECT_DIR = Path("dbt")
SESSION_TARGET = "session"


@dataclass(frozen=True)
class GoldTarget:
    """Where one dbt invocation reads its project and writes its output.

    ``profiles_dir`` is separate from ``project_dir`` because they genuinely
    come apart: the regression test runs a throwaway project against the
    profile this repository actually ships, which is the only way it can
    prove anything about that profile.
    """

    warehouse: Path
    metastore: Path
    project_dir: Path = DEFAULT_PROJECT_DIR
    profiles_dir: Path | None = None
    name: str = SESSION_TARGET
    target_path: Path | None = None

    @property
    def is_local(self) -> bool:
        """Local compute, as opposed to a Databricks endpoint that already exists."""
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


def run_dbt(command: list[str], target: GoldTarget) -> dbtRunnerResult:
    """Run one dbt command against the local session or a remote warehouse.

    A SparkSession is built only for the local target. The ``databricks``
    target connects over HTTP to compute that already exists, and starting a
    local JVM to talk to it would be pure waste.
    """
    if target.is_local:
        dbt_session(warehouse=target.warehouse, metastore=target.metastore)
    return dbtRunner().invoke([*command, *target.cli_flags()])


def main(argv: list[str] | None = None) -> int:
    settings = Settings()
    parser = argparse.ArgumentParser(description="Run dbt for the Gold layer.")
    parser.add_argument("--warehouse", type=Path, default=settings.warehouse_dir)
    parser.add_argument("--metastore", type=Path, default=settings.metastore_dir)
    parser.add_argument("--project-dir", type=Path, default=DEFAULT_PROJECT_DIR)
    parser.add_argument("--profiles-dir", type=Path, default=None)
    parser.add_argument("--target", default=SESSION_TARGET)
    parser.add_argument("--target-path", type=Path, default=None)
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
    )
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
