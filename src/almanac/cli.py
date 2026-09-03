"""The exit convention for entrypoints that may run as a Databricks job task."""

import sys
from collections.abc import Callable


def run_cli(main: Callable[[], int]) -> None:
    """Exit non-zero on failure; return normally on success.

    Never ``sys.exit(0)``. Databricks runs a ``spark_python_task`` inside an
    IPython shell, where ``SystemExit`` propagates as an exception and marks
    the task FAILED even when the code is 0 -- measured 2026-09-02, on a
    backfill that had already written all 92 days and printed its summary.
    A shell that treats a bare ``return`` as success and a non-zero exit as
    failure sees the same thing either way, so nothing is given up.
    """
    code = main()
    if code:
        sys.exit(code)
