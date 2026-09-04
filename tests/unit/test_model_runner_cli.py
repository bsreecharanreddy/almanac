"""_build_parser: argument defaults and required flags, as a pure unit
test -- main()'s real dispatch is tests/integration/test_model_runner.py's
job, exactly like Phase 3's runner tests split the same way.
"""

import pytest

from almanac.model.runner import _build_parser


def test_register_defaults_false_and_names_default() -> None:
    args = _build_parser().parse_args(
        [
            "--silver-path",
            "/s",
            "--features-path",
            "/f",
            "--gold-warehouse",
            "/g",
            "--tracking-uri",
            "file:///tmp/mlruns",
            "--experiment-name",
            "pr-review-sla-risk",
        ]
    )

    assert args.register is False
    assert args.catalog == "almanac"
    assert args.schema == "models"
    assert args.model_name == "pr_review_sla_risk"


def test_required_flags_are_enforced() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--features-path", "/f"])
