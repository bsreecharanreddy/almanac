"""Databricks job entrypoint for almanac.model.score_runner."""

from almanac.cli import run_cli
from almanac.model.score_runner import main

if __name__ == "__main__":
    run_cli(main)
