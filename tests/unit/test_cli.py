"""``run_cli``: success must not raise, failure must still exit non-zero."""

from __future__ import annotations

from pathlib import Path

import pytest

from almanac.cli import run_cli


def test_success_does_not_raise_system_exit() -> None:
    """The defect this exists for: `sys.exit(main())` on a successful run raises
    SystemExit(0), which Databricks' IPython task host reports as FAILED --
    measured 2026-09-02 on a backfill that had written all 92 days."""
    run_cli(lambda: 0)


@pytest.mark.parametrize("code", [1, 2, 255])
def test_failure_still_exits_with_the_code(code: int) -> None:
    """Non-zero must keep propagating, or a real failure would report success."""
    with pytest.raises(SystemExit) as exc_info:
        run_cli(lambda: code)

    assert exc_info.value.code == code


# The scripts a Databricks `spark_python_task` names as its `python_file`.
# Local-only scripts are deliberately absent: `sys.exit` is correct there.
JOB_ENTRYPOINTS = ("backfill", "photon_ab", "gold")


@pytest.mark.parametrize("name", JOB_ENTRYPOINTS)
def test_every_job_entrypoint_exits_through_run_cli(name: str) -> None:
    """Checked as source, because the defect is in the line that never runs under test.

    `photon_ab.py` carried `sys.exit(main())` unnoticed until the day it was
    first deployed -- it had never been run as a job, so its first *successful*
    execution would have been the one to report FAILED.
    """
    source = Path(f"scripts/{name}.py").read_text()
    assert "run_cli(main)" in source
    assert "sys.exit(main())" not in source
