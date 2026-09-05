"""Launch shim for Task 6's real with/without comparison on a job
cluster; the CLI itself is almanac.model.similarity_comparison.
"""

from almanac.cli import run_cli
from almanac.model.similarity_comparison import main

if __name__ == "__main__":
    run_cli(main)
