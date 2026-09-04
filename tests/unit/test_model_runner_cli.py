"""_build_parser / _parse_args: argument defaults and required flags, as
a pure unit test -- main()'s real dispatch is
tests/integration/test_model_runner.py's job, exactly like Phase 3's
runner tests split the same way.
"""

import pytest

from almanac.model.runner import _build_parser, _parse_args

_BASE_ARGS = [
    "--silver-path",
    "/s",
    "--features-path",
    "/f",
    "--gold-table",
    "almanac_dbx.gold.fact_pull_request",
    "--tracking-uri",
    "file:///tmp/mlruns",
    "--experiment-name",
    "pr-review-sla-risk",
]


def test_register_defaults_false_and_names_default() -> None:
    args = _build_parser().parse_args(_BASE_ARGS)

    assert args.register is False
    assert args.objective == "regression"
    assert args.threshold_seconds is None
    assert args.catalog == "almanac"
    assert args.schema == "models"
    assert args.model_name == "pr_review_sla_risk"


def test_required_flags_are_enforced() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--features-path", "/f"])


def test_threshold_seconds_is_required_with_classification_objective() -> None:
    with pytest.raises(SystemExit):
        _parse_args([*_BASE_ARGS, "--objective", "classification"])


def test_threshold_seconds_is_not_required_for_the_default_regression_objective() -> None:
    args = _parse_args(_BASE_ARGS)

    assert args.threshold_seconds is None


def test_classification_objective_with_threshold_seconds_parses_cleanly() -> None:
    args = _parse_args(
        [*_BASE_ARGS, "--objective", "classification", "--threshold-seconds", "1487"]
    )

    assert args.objective == "classification"
    assert args.threshold_seconds == 1487
