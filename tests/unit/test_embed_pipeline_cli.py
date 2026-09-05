"""_build_parser: argument defaults and required flags, as a pure unit
test. main()'s real dispatch downloads real sentence-transformers weights
-- verified only by Task 7's real cloud run, the same "not a unit test"
boundary load_encoder itself already draws.
"""

import pytest

from almanac.embed.pipeline import DEFAULT_MODEL, _build_parser


def test_defaults_cover_model_batch_size_schema_and_register() -> None:
    args = _build_parser().parse_args(["--bronze-path", "/b", "--embeddings-path", "/e"])

    assert args.model_name == DEFAULT_MODEL
    assert args.batch_size == 64
    assert args.num_partitions == 16
    assert args.schema == "embeddings"
    assert args.register is False


def test_every_default_can_be_overridden() -> None:
    args = _build_parser().parse_args(
        [
            "--bronze-path",
            "/b",
            "--embeddings-path",
            "/e",
            "--model-name",
            "some/other-model",
            "--batch-size",
            "32",
            "--num-partitions",
            "64",
            "--schema",
            "custom",
            "--register",
        ]
    )

    assert args.model_name == "some/other-model"
    assert args.batch_size == 32
    assert args.num_partitions == 64
    assert args.schema == "custom"
    assert args.register is True


def test_bronze_path_and_embeddings_path_are_required() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--embeddings-path", "/e"])
