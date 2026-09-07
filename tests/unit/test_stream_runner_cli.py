"""_build_parser and main's stage dispatch, as pure unit tests. The real
stages need the live Events API and a SparkSession respectively, and are
exercised for real only in Task 9's cloud window -- the same deferral
tests/unit/test_features_runner_cli.py already uses.
"""

import pytest

from almanac.features.registration import not_null_key_sql, primary_key_sql
from almanac.stream.runner import _REQUIRED_BY_STAGE, STREAM_FEATURE_TABLES, _build_parser, main


def test_poll_is_bounded_by_default() -> None:
    """`run_poller` accepts max_polls=None and would then never return; the
    CLI must not be the thing that reaches for it."""
    args = _build_parser().parse_args(["poll", "--landing", "/l"])

    assert args.stage == "poll"
    assert args.max_polls == 30
    assert args.watermark_minutes == 10


def test_stage_must_be_one_of_the_known_stages() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["teardown", "--landing", "/l"])


def test_landing_is_required_by_the_stages_that_read_it() -> None:
    """Enforced in main, not by argparse: --landing is meaningless to the
    features and publish stages, so it cannot be globally required."""
    with pytest.raises(SystemExit):
        main(["poll"])

    with pytest.raises(SystemExit):
        main(["ingest", "--silver-path", "/s", "--checkpoint", "/c"])


def test_ingest_refuses_to_start_without_a_silver_path_or_checkpoint() -> None:
    """Caught before the query starts: a streaming query that fails partway
    has already created its checkpoint directory."""
    with pytest.raises(SystemExit):
        main(["ingest", "--landing", "/l"])

    with pytest.raises(SystemExit):
        main(["ingest", "--landing", "/l", "--silver-path", "/s"])


def test_publish_refuses_to_start_without_a_store_name() -> None:
    """Caught before any client is built: a publish that fails partway has
    already started a sync pipeline against a billing store."""
    with pytest.raises(SystemExit):
        main(["publish"])


def test_features_and_publish_do_not_require_a_landing_zone() -> None:
    """They read Delta, not the poller's files -- requiring --landing would
    make a job task pass a path it never opens."""
    assert "landing" not in _REQUIRED_BY_STAGE["features"]
    assert "landing" not in _REQUIRED_BY_STAGE["publish"]


def test_registration_is_opt_in_so_a_dry_run_creates_no_uc_objects() -> None:
    args = _build_parser().parse_args(["features", "--silver-path", "/s", "--features-path", "/f"])

    assert args.register is False
    assert args.publish_mode == "TRIGGERED"


def test_stream_feature_specs_are_temporal_and_keyed_on_their_own_entity() -> None:
    """The specs feed write_and_register, so a wrong entity column would emit
    a PRIMARY KEY on a column the feature frame does not have -- and that only
    fails against real Unity Catalog, in Task 9, not here.
    """
    by_name = {spec.name: spec for spec in STREAM_FEATURE_TABLES}

    assert by_name["repo_stream_activity"].entity_cols == ["repo_id"]
    assert by_name["actor_stream_activity"].entity_cols == ["actor_login"]
    for spec in STREAM_FEATURE_TABLES:
        # Every stream feature is a per-event timeseries row, so none may
        # register as a plain lookup table (the pr_static shape).
        assert spec.event_time_col == "event_time"


def test_stream_specs_produce_the_ddl_publishing_requires() -> None:
    """Ties the specs to Task 6's statements: publish_table needs a TIMESERIES
    primary key whose columns are all NOT NULL."""
    spec = STREAM_FEATURE_TABLES[0]

    _, add_sql = primary_key_sql(
        schema="features",
        table=spec.name,
        entity_cols=spec.entity_cols,
        event_time_col=spec.event_time_col,
    )
    not_nulls = not_null_key_sql(
        schema="features",
        table=spec.name,
        entity_cols=spec.entity_cols,
        event_time_col=spec.event_time_col,
    )

    assert add_sql.endswith("PRIMARY KEY (repo_id, event_time TIMESERIES)")
    assert len(not_nulls) == 2
