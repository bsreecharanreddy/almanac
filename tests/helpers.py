"""Shared assertions and builders for the Spark test modules."""

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from almanac.features.similarity import Neighbor
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze

if TYPE_CHECKING:
    from pyspark.sql.streaming.query import StreamingQuery


def one(df: DataFrame) -> Row:
    """``first()`` is typed ``Row | None``; every call here expects a row."""
    row = df.first()
    assert row is not None
    return row


def epoch_of(df: DataFrame, column: str) -> int:
    """A timestamp column's stored instant as an epoch second.

    No test asserts on a collected datetime: PySpark returns it naive in the
    driver's timezone, so the comparison passes or fails by machine (and
    always passes on a UTC CI runner). An epoch carries no ambiguity.
    """
    return int(one(df.selectExpr(f"unix_timestamp({column}) AS epoch"))["epoch"])


# 2025-08-04 is a Monday, so PRs spread from here land in whole weeks.
_FIRST_MONDAY = datetime(2025, 8, 4, 9, tzinfo=UTC)


def opened_at(pr_number: int, *, minutes: int = 0, weeks: int = 4) -> datetime:
    """When PR `pr_number` opened, spread over `weeks` calendar weeks.

    `temporal_split` refuses a frame inside a single week, so a fixture
    whose PRs all open at one instant cannot be trained on at all. The week
    is `pr_number % weeks` rather than monotonic on purpose: these fixtures
    derive the label from `pr_number`, and a monotonic spread would put one
    class wholly on one side of the split, leaving `roc_auc_score` nothing
    to score.
    """
    return _FIRST_MONDAY + timedelta(weeks=pr_number % weeks, days=pr_number % 7, minutes=minutes)


def build_bronze(
    spark: SparkSession,
    fixture_path: Path,
    bronze_path: Path,
    *,
    event_date: str,
    event_hour: int,
    ingested_at: datetime,
    part: tuple[int, int] | None = None,
) -> None:
    """Land one fixture as a real Bronze partition, through the shipped ``write_bronze``.

    ``part=(index, total)`` lands a deterministic disjoint slice (split on a
    record hash, since Spark does not promise row order) so two hours can
    hold different events like two real hourly files.
    """
    raw = spark.read.text(str(fixture_path)).withColumnRenamed("value", "raw_json")
    if part is not None:
        index, total = part
        raw = raw.filter(F.pmod(F.crc32(F.col("raw_json")), F.lit(total)) == index)
    stamped = add_ingestion_metadata(raw, ingested_at=ingested_at, source_file=str(fixture_path))
    partitioned = stamped.withColumn("event_date", F.lit(event_date)).withColumn(
        "event_hour", F.lit(event_hour)
    )
    write_bronze(partitioned, str(bronze_path), event_date=event_date, event_hour=event_hour)


# The parse_events -> normalize_events contract, declared once (it lived in
# two test modules and drifted -- the two-places-one-contract failure).
RAW_SCHEMA = (
    "created_at_raw string, actor_raw string, repo_id long, "
    "repo_name string, event_type string, id string, "
    "event_url string, ingested_at timestamp"
)

type RawRow = tuple[str, str | None, int | None, str, str, str | None, str | None, datetime]


def _parsed_defaults() -> dict[str, Column]:
    """The rest of what ``parse_events`` produces, so each test names only
    the fields it is about. Built lazily -- ``F.lit`` needs a live SparkContext."""
    return {
        "event_action": F.lit(None).cast("string"),
        "pr_number": F.lit(None).cast("long"),
        "pr_merged": F.lit(None).cast("boolean"),
        "pr_draft": F.lit(None).cast("boolean"),
        "issue_is_pr": F.lit(None).cast("boolean"),
        "push_size": F.lit(None).cast("long"),
        "push_distinct_size": F.lit(None).cast("long"),
        "event_date": F.lit("2025-08-13"),
        "event_hour": F.lit(14),
    }


def raw(spark: SparkSession, *rows: RawRow) -> DataFrame:
    """Crafted rows in the shape `parse_events` hands to `normalize_events`."""
    df = spark.createDataFrame(list(rows), RAW_SCHEMA)
    for name, default in _parsed_defaults().items():
        df = df.withColumn(name, default)
    return df


def land_poll(landing: Path, events: list[dict[str, object]], *, polled_at: str, name: str) -> Path:
    """One poller landing file -- the same envelope ``stream.poller._write_poll`` produces.

    ``event`` is JSON-encoded as a string, not embedded as a nested object:
    a nested object would force the landing zone's read schema to type it
    via ``payloads.EVENT_SCHEMA``, which omits ``actor`` on purpose, silently
    dropping it before ``parse_events`` ever sees the event.
    """
    path = landing / f"{name}.jsonl"
    lines = (json.dumps({"polled_at": polled_at, "event": json.dumps(e)}) for e in events)
    path.write_text("\n".join(lines) + "\n")
    return path


def run_streaming_query(query: "StreamingQuery", *, timeout: int = 180) -> None:
    """``awaitTermination`` bounded, failing loudly rather than hanging the suite
    if a streaming test's query never stops on its own (e.g. ``availableNow``
    finding nothing to do is instant; a genuine hang is a real bug to see).

    180s, not the 60s this started at. Coverage instrumentation costs ~2.4x on
    this suite (46:19 against a 19:32 baseline, measured 2026-09-07 on a quiet
    machine), so under `make coverage` a 60s budget bought about 25s of real
    work -- and four streaming tests failed on the budget alone, all four
    passing serially in 85s total. This is a guard against a hang, which is
    unbounded; it is not an assertion about latency, so widening it gives up
    nothing a test here was ever meant to catch.
    """
    finished = query.awaitTermination(timeout)
    if not finished:
        query.stop()
        raise AssertionError(f"streaming query did not terminate within {timeout}s")


class FakeSimilarityIndex:
    """A small brute-force index -- real nearest-by-L2-distance and a real
    as_of filter -- standing in for Task 3's real Vector Search index
    (`almanac.features.similarity.SimilarityIndex`), the same test-double
    shape as `embed.pipeline`'s `_FakeEncoder`. `entries` is (Neighbor, its
    own vector); the vector `query()` receives is only used for distance,
    never for the filter.
    """

    def __init__(self, entries: list[tuple[Neighbor, list[float]]]) -> None:
        self._entries = entries

    def query(self, vector: list[float], *, as_of: datetime, k: int) -> list[Neighbor]:
        eligible = [(n, v) for n, v in self._entries if n.event_time < as_of]
        by_distance = sorted(eligible, key=lambda nv: math.dist(vector, nv[1]))
        return [n for n, _ in by_distance[:k]]
