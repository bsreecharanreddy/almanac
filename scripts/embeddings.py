"""Launch shim for the embedding pipeline on a job cluster; the CLI itself
is almanac.embed.pipeline.
"""

from almanac.cli import run_cli
from almanac.embed.pipeline import main

if __name__ == "__main__":
    run_cli(main)
