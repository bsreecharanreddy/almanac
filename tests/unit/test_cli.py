"""``run_cli``: success must not raise, failure must still exit non-zero."""

from __future__ import annotations

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
