"""Launch shim for the model runner on a job cluster; the CLI itself is
almanac.model.runner.
"""

from almanac.cli import run_cli
from almanac.model.runner import main

if __name__ == "__main__":
    run_cli(main)
