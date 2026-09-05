"""Launch shim for Task 4's real similarity computation on a job cluster;
the CLI itself is almanac.features.similarity_runner.
"""

from almanac.cli import run_cli
from almanac.features.similarity_runner import main

if __name__ == "__main__":
    run_cli(main)
