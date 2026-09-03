"""Launch shim for Gold on a job cluster; the CLI itself is almanac.gold.runner."""

from almanac.cli import run_cli
from almanac.gold.runner import main

if __name__ == "__main__":
    run_cli(main)
