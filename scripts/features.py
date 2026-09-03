"""Launch shim for the feature platform on a job cluster; the CLI itself is
almanac.features.runner.
"""

from almanac.cli import run_cli
from almanac.features.runner import main

if __name__ == "__main__":
    run_cli(main)
