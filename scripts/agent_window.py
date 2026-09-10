"""Launch shim for the Phase 9 window on a job cluster; the CLI is almanac.agent.window."""

from almanac.agent.window import main
from almanac.cli import run_cli

if __name__ == "__main__":
    run_cli(main)
