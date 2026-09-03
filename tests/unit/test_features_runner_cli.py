"""_build_parser: argument defaults and required flags, as a pure unit
test. The CLI's real dispatch (main() calling run_features against a
live SparkSession) is tests/integration/test_features_runner.py's job --
argparse.Namespace needs no Spark and no mocking to test directly.
"""

import pytest

from almanac.features.runner import _build_parser


def test_register_defaults_false_and_schema_defaults_features() -> None:
    args = _build_parser().parse_args(["--silver-path", "/s", "--features-path", "/f"])

    assert args.register is False
    assert args.schema == "features"


def test_register_flag_and_explicit_schema_are_both_honored() -> None:
    args = _build_parser().parse_args(
        ["--silver-path", "/s", "--features-path", "/f", "--register", "--schema", "custom"]
    )

    assert args.register is True
    assert args.schema == "custom"


def test_silver_path_and_features_path_are_required() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--features-path", "/f"])
