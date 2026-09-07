"""_build_parser and main's stage dispatch, as pure unit tests. The real
stages need the live Events API and a SparkSession respectively, and are
exercised for real only in Task 9's cloud window -- the same deferral
tests/unit/test_features_runner_cli.py already uses.
"""

import pytest

from almanac.stream.runner import _build_parser, main


def test_poll_is_bounded_by_default() -> None:
    """`run_poller` accepts max_polls=None and would then never return; the
    CLI must not be the thing that reaches for it."""
    args = _build_parser().parse_args(["poll", "--landing", "/l"])

    assert args.stage == "poll"
    assert args.max_polls == 30
    assert args.watermark_minutes == 10


def test_stage_must_be_one_of_the_two() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["publish", "--landing", "/l"])


def test_landing_is_required_for_either_stage() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["poll"])


def test_ingest_refuses_to_start_without_a_silver_path_or_checkpoint() -> None:
    """Caught before the query starts: a streaming query that fails partway
    has already created its checkpoint directory."""
    with pytest.raises(SystemExit):
        main(["ingest", "--landing", "/l"])

    with pytest.raises(SystemExit):
        main(["ingest", "--landing", "/l", "--silver-path", "/s"])
