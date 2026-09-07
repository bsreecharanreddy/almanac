"""Launch shim for the live event stream on a job cluster; the CLI itself is
almanac.stream.runner.
"""

from almanac.cli import run_cli
from almanac.stream.runner import main

if __name__ == "__main__":
    run_cli(main)
