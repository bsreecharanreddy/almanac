from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from almanac.explore.schema import era_for
from almanac.pipeline.eras import normalize_events
from tests.helpers import epoch_of, one

pytestmark = pytest.mark.spark

RAW_SCHEMA = (
    "created_at_raw string, actor_raw string, repo_id long, "
    "repo_name string, event_type string, id string, "
    "event_url string, ingested_at timestamp"
)

type RawRow = tuple[str, str | None, int | None, str, str, str | None, str | None, datetime]

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)

LEGACY: RawRow = ("2014-06-12T03:00:00-07:00", "abc", 1, "o/r", "PushEvent", None, None, INGESTED)
MODERN: RawRow = ("2025-08-13T14:00:00Z", "abc", 1, "o/r", "PushEvent", "999", None, INGESTED)


def raw(spark: SparkSession, *rows: RawRow) -> DataFrame:
    return spark.createDataFrame(list(rows), RAW_SCHEMA)


def instant(year: int, month: int, day: int, hour: int, minute: int) -> int:
    """The epoch second of a UTC wall-clock time."""
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp())


def test_legacy_timestamp_offset_is_converted_not_truncated(spark: SparkSession) -> None:
    """Measured: pre-2015 `created_at` carries -07:00.

    A naive parse reads 2014-06-12T03:00:00-07:00 as 03:00 UTC. It is
    10:00 UTC. Seven hours, silently, on every legacy event.
    """
    assert epoch_of(normalize_events(raw(spark, LEGACY)), "created_at") == instant(
        2014, 6, 12, 10, 0
    )


def test_modern_timestamp_is_utc_unchanged(spark: SparkSession) -> None:
    assert epoch_of(normalize_events(raw(spark, MODERN)), "created_at") == instant(
        2025, 8, 13, 14, 0
    )


def test_legacy_event_id_is_a_content_hash(spark: SparkSession) -> None:
    """Measured: 0 of 2,000 legacy events carry an `id`."""
    out = one(normalize_events(raw(spark, LEGACY)))
    assert out["event_id"], "legacy events must still get an id"
    assert out["event_id_source"] == "content_hash"


def test_modern_event_id_is_the_native_id(spark: SparkSession) -> None:
    out = one(normalize_events(raw(spark, MODERN)))
    assert out["event_id"] == "999"
    assert out["event_id_source"] == "native"


def test_content_hash_is_stable_across_runs(spark: SparkSession) -> None:
    # If the hash were not deterministic, dedup would fail and every
    # re-run would duplicate legacy history.
    first = one(normalize_events(raw(spark, LEGACY)))["event_id"]
    second = one(normalize_events(raw(spark, LEGACY)))["event_id"]
    assert first == second


def test_distinct_legacy_events_hash_differently(spark: SparkSession) -> None:
    other: RawRow = (
        "2014-06-12T03:00:00-07:00",
        "abc",
        2,
        "o/s",
        "PushEvent",
        None,
        None,
        INGESTED,
    )
    ids = [r["event_id"] for r in normalize_events(raw(spark, LEGACY, other)).collect()]
    assert len(set(ids)) == 2


def test_era_is_labelled_on_every_row(spark: SparkSession) -> None:
    rows: list[RawRow] = [
        ("2014-06-12T03:00:00-07:00", "a", 1, "o/r", "PushEvent", None, None, INGESTED),
        ("2025-08-13T14:00:00Z", "b", 2, "o/s", "PushEvent", "1", None, INGESTED),
        ("2025-11-01T00:00:00Z", "c", 3, "o/t", "PushEvent", "2", None, INGESTED),
    ]
    eras = {r["schema_era"] for r in normalize_events(raw(spark, *rows)).collect()}
    assert eras == {"legacy_v1", "modern_v2", "reduced_v3"}


def test_repo_name_case_is_preserved(spark: SparkSession) -> None:
    """Measured: case-only renames exist (GLB -> glb).

    Lower-casing here would make them invisible to SCD2 in Phase 2.
    """
    row: RawRow = (
        "2025-08-13T14:00:00Z",
        "a",
        1,
        "Lumacaonta/GLB",
        "PushEvent",
        "1",
        None,
        INGESTED,
    )
    assert one(normalize_events(raw(spark, row)))["repo_name"] == "Lumacaonta/GLB"


def test_output_is_exactly_the_canonical_silver_shape(spark: SparkSession) -> None:
    """The declared contract, asserted rather than assumed.

    Leaking `created_at_raw`, `actor_raw` and `id` downstream would leave
    two id columns in Silver -- one of them null for every legacy event --
    which is precisely the shape a later dedup is most likely to key on by
    mistake.
    """
    assert set(normalize_events(raw(spark, MODERN)).columns) == {
        "event_id",
        "event_id_source",
        "actor_login",
        "created_at",
        "repo_id",
        "repo_name",
        "event_type",
        "schema_era",
        "ingested_at",
    }


def test_spark_era_labels_agree_with_the_python_implementation(spark: SparkSession) -> None:
    """One rule, two implementations; they must not drift apart.

    `era_for` decides eras in Python for exploration, this module decides
    them again in Spark for the pipeline. Both boundaries are asserted from
    either side, so moving one without the other mislabels history instead
    of failing.
    """
    boundaries = [
        datetime(2014, 12, 31, 23, 59, tzinfo=UTC),
        datetime(2015, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 10, 14, 23, 59, tzinfo=UTC),
        datetime(2025, 10, 15, 0, 0, tzinfo=UTC),
    ]
    rows: list[RawRow] = [
        (d.strftime("%Y-%m-%dT%H:%M:%SZ"), "a", 1, "o/r", "PushEvent", str(n), None, INGESTED)
        for n, d in enumerate(boundaries)
    ]
    out = normalize_events(raw(spark, *rows)).collect()
    labelled = {r["event_id"]: r["schema_era"] for r in out}
    assert labelled == {str(n): era_for(d).value for n, d in enumerate(boundaries)}


def test_content_hash_does_not_depend_on_the_session_timezone(spark: SparkSession) -> None:
    """The dedup key must be a property of the event, not of the cluster.

    Measured against the planned implementation, which hashed the timestamp
    *rendered* as a string: one event produced three different ids under
    UTC, America/New_York and Asia/Kolkata. This id is persisted and is the
    only one a legacy event will ever have, so a backfill and a later
    incremental run under different session timezones would re-ingest all
    of legacy history as new rows without dedup noticing.
    """
    original = spark.conf.get("spark.sql.session.timeZone", "UTC")
    assert original is not None
    try:
        ids = set()
        for tz in ("UTC", "America/New_York", "Asia/Kolkata"):
            spark.conf.set("spark.sql.session.timeZone", tz)
            ids.add(one(normalize_events(raw(spark, LEGACY)))["event_id"])
        assert len(ids) == 1, "the content hash moved with the session timezone"
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)


def test_nulls_hold_their_position_in_the_content_hash(spark: SparkSession) -> None:
    """Two different events, each null in a different field, are not one event.

    `concat_ws` skips nulls rather than propagating them, which stops one
    missing field collapsing every row's hash -- but on its own it also
    makes ("a", null, "c") and ("a", "c", null) render identically. For
    legacy events that is a silent merge of two real events with no native
    id to fall back on.
    """
    no_actor: RawRow = (
        "2014-06-12T03:00:00-07:00",
        None,
        5,
        "o/r",
        "PushEvent",
        None,
        None,
        INGESTED,
    )
    no_repo: RawRow = (
        "2014-06-12T03:00:00-07:00",
        "5",
        None,
        "o/r",
        "PushEvent",
        None,
        None,
        INGESTED,
    )
    ids = [r["event_id"] for r in normalize_events(raw(spark, no_actor, no_repo)).collect()]
    assert len(set(ids)) == 2


def test_same_actor_repo_second_and_type_are_still_two_events(spark: SparkSession) -> None:
    """The collision measured in a real legacy hour, pinned.

    One actor pushed two different commit ranges to one repo inside the same
    second; another opened two different issues inside the same second. On
    `(created_at, actor_login, repo_id, event_type)` alone both pairs hash
    identically, and dedup deletes one of each -- roughly 1 in 1,000 legacy
    events, silently. Phase 0's 1-in-6.0M duplicate ratio was measured on
    modern data, which carries native ids and never reaches this hash.
    """
    push_a: RawRow = (
        "2014-06-12T14:07:42-07:00",
        "oschettler",
        18377459,
        "oschettler/allesuns",
        "PushEvent",
        None,
        "https://github.com/oschettler/allesuns/compare/19d1cb231c...c59c2cc6f1",
        INGESTED,
    )
    push_b: RawRow = (
        "2014-06-12T14:07:42-07:00",
        "oschettler",
        18377459,
        "oschettler/allesuns",
        "PushEvent",
        None,
        "https://github.com/oschettler/allesuns/compare/f8ccc599d0...19d1cb231c",
        INGESTED,
    )
    ids = {r["event_id"] for r in normalize_events(raw(spark, push_a, push_b)).collect()}
    assert len(ids) == 2
