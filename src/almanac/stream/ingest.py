"""Structured Streaming ingest from the poller's landing zone to a live Silver table.

The landing zone (``stream.poller``'s ``.jsonl`` files) is the streaming
equivalent of Bronze -- raw, replayable, never transformed, exactly what
design doc §4.6 means by "a poller lands raw event JSON to cloud storage."
This module is the streaming equivalent of Silver: it reuses
``pipeline.payloads.parse_events`` and ``pipeline.eras.normalize_events``
unchanged, so a row landed here has the exact same shape
(``pipeline.eras.SILVER_COLUMNS``) and the exact same ``event_id`` a batch
Silver run would compute for the same event -- the two paths cannot
silently disagree about what a duplicate is, because they share the code
that decides it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from almanac.explore.schema import SchemaEra
from almanac.pipeline.eras import normalize_events
from almanac.pipeline.payloads import parse_events

if TYPE_CHECKING:
    from pyspark.sql.streaming.query import StreamingQuery

# The poller's own envelope (stream.poller._write_poll): one raw event plus
# the moment it was polled. `event` is a raw JSON *string*, not a struct
# typed by payloads.EVENT_SCHEMA -- EVENT_SCHEMA deliberately omits `actor`
# (its type varies pre/post 2015), so typing this field would silently drop
# it before `parse_events` ever saw the event, nulling every `actor_login`
# in the stream. Caught for real by Task 4's batch-equality gate, 2026-09-06:
# Task 3's own unit tests never exercised it because their fixture events
# carried no `actor` field at all. A string preserves exactly what GitHub
# sent, matching Bronze's own raw-string contract.
_LANDING_SCHEMA = StructType(
    [
        StructField("polled_at", StringType()),
        StructField("event", StringType()),
    ]
)

_LATE_METRIC = "late_events"


def stream_events(
    spark: SparkSession, landing: str, *, late_after: timedelta = timedelta(minutes=10)
) -> DataFrame:
    """A streaming read of the poller's landing zone, annotated with lateness.

    Reuses ``parse_events``/``normalize_events`` verbatim, so ``event_id``
    and ``schema_era`` are computed identically to the batch path (§4.2) --
    the dedup key below is the same fact, not a second copy of it.
    ``event_date``/``event_hour`` are derived from the event's own
    ``created_at`` rather than an archive file's, because there is no
    archive file here: the file-vs-event-time distinction in
    ``eras.SILVER_COLUMNS`` exists only for a legacy hour-file spanning two
    UTC hours, a case a live per-event feed cannot produce.

    ``ingested_at`` carries the poller's own ``polled_at`` -- the streaming
    equivalent of Bronze's wall-clock landing stamp, and, compared against
    ``created_at``, the exact divergence ``stream.poller`` was built to
    expose (measured 2026-09-06: a stable ~305s feed lag).

    ``late_after`` no longer discards anything, and the rename is the point.
    It was a Spark watermark feeding ``dropDuplicatesWithinWatermark`` until
    2026-09-07, when a live window showed what that cost: **161 repos never
    reached the online store**, because a distinct, never-before-seen event
    whose event time is behind the watermark is dropped, not merely
    deduplicated. Cross-batch dedup moved to an insert-only Delta ``MERGE``
    in ``write_stream_silver``, which bounds nothing by event time, so this
    is now purely the threshold at which a row is *reported* ``is_late``.
    Read the total with ``late_event_count(query)``.
    """
    raw = spark.readStream.schema(_LANDING_SCHEMA).json(landing)
    as_raw_json = raw.select(
        F.col("event").alias("raw_json"),
        F.to_timestamp(F.col("polled_at")).alias("ingested_at"),
    )

    parsed = parse_events(as_raw_json)
    created_at = F.to_timestamp(F.col("created_at_raw"))
    # `date_format`, not `to_date`: batch stamps event_date as the string
    # `burn.day`/`build_bronze` are given (`F.lit(event_date)`), and a DateType
    # column here would silently disagree with that schema on write -- caught
    # by Task 4's batch-equality gate, 2026-09-06.
    with_partition_cols = parsed.withColumn(
        "event_date", F.date_format(created_at, "yyyy-MM-dd")
    ).withColumn("event_hour", F.hour(created_at))
    normalized = normalize_events(with_partition_cols)

    delay_seconds = F.unix_timestamp("ingested_at") - F.unix_timestamp("created_at")
    annotated = normalized.withColumn("is_late", delay_seconds > F.lit(late_after.total_seconds()))
    observed = annotated.observe(
        _LATE_METRIC,
        F.sum(F.col("is_late").cast("long")).alias("late_count"),
        F.count("*").alias("total"),
        # Measured, not assumed: the era mix was previously asserted to be
        # REDUCED_V3-only, and live data falsified that (see
        # _reject_id_less_event). A counter reports the real rate instead.
        F.sum((F.col("schema_era") != SchemaEra.REDUCED_V3.value).cast("long")).alias(
            "non_reduced_count"
        ),
    )

    # No `withWatermark`/`dropDuplicatesWithinWatermark` here on purpose: both
    # were removed 2026-09-07 because they discard late rows outright, and the
    # live feed's late rows are overwhelmingly *distinct* events rather than
    # duplicates (18.0% of a window arrive behind a 10-minute watermark; 16.4%
    # carry an event time over a day old). Dedup is now `write_stream_silver`'s
    # insert-only MERGE, which is bounded by storage layout rather than by
    # event time -- so the stream is stateless and nothing is dropped.
    return observed


def _observed_sum(query: StreamingQuery, field: str) -> int:
    """Sum one ``observe()`` field across every micro-batch run so far.

    Read from Spark's own progress (``recentProgress``, bounded by Spark's
    own retention) rather than from written output, for the reason
    ``stream_events`` documents: a row counted here may never be written.
    """
    total = 0
    for progress in query.recentProgress:
        metrics = progress.get("observedMetrics", {}).get(_LATE_METRIC)
        if metrics is not None and metrics[field] is not None:
            total += metrics[field]
    return total


def late_event_count(query: StreamingQuery) -> int:
    """Rows seen with a processing delay beyond ``late_after``. Reported,
    not dropped -- see ``write_stream_silver``."""
    return _observed_sum(query, "late_count")


def non_reduced_count(query: StreamingQuery) -> int:
    """Rows whose ``schema_era`` is not REDUCED_V3 -- rare but real on the
    live feed (3 of 6801 in a 30-poll window, 2026-09-07)."""
    return _observed_sum(query, "non_reduced_count")


def _reject_id_less_event(batch: DataFrame) -> None:
    """Stop the stream on an event with no usable ``event_id``.

    Corrects a narrower-than-intended guard. This previously asserted the
    feed was REDUCED_V3-only, on a 2026-09-06 measurement; a 30-poll live
    window on 2026-09-07 falsified that -- 3 of 6801 events were
    ``modern_v2``, including a PullRequestEvent created 2021-07-30. GitHub
    stamps event ids on delivery, not on occurrence, so an arbitrarily old
    ``created_at`` can arrive at any time.

    The era was never the real hazard: ``pipeline.eras`` already handles all
    three eras, and those rows carried valid ids. The hazard is an event
    ``pipeline.dedup`` cannot deduplicate, because it passes null
    ``event_id`` rows straight through -- so duplicates would accumulate
    unnoticed. That is what this asserts, and the era mix is now reported by
    the ``non_reduced_count`` metric rather than assumed.
    """
    if batch.filter(F.col("event_id").isNull()).limit(1).count():
        raise ValueError(
            "streaming ingest received an event with no event_id; "
            "pipeline.dedup cannot deduplicate it, so this is a real anomaly"
        )


def _batch_date_span(batch: DataFrame) -> tuple[str, str] | None:
    """The batch's own ``[min, max]`` ``event_date``, or ``None`` if empty.

    This bounds the MERGE's target-side scan. Databricks' documented example
    bounds it by wall clock instead (``current_date() - INTERVAL 7 DAYS``),
    which is a *heuristic*: it assumes duplicates arrive within days of the
    original, and silently stops deduplicating anything older. The batch's own
    span is **exact** -- ``event_date`` is derived from ``created_at``, so a
    duplicate necessarily carries the same one and cannot fall outside this
    range -- and it prunes at least as hard, since a poll cycle spans minutes.
    It also removes a wall-clock dependency that made replaying archive data
    behave differently from a live window.
    """
    row = batch.agg(F.min("event_date"), F.max("event_date")).first()
    if row is None or row[0] is None:
        return None
    return str(row[0]), str(row[1])


def write_stream_silver(
    df: DataFrame, dest: str, checkpoint: str, *, available_now: bool = False
) -> StreamingQuery:
    """Write each micro-batch to the live Silver table, deduplicating on
    ``event_id`` without discarding late events.

    An **insert-only MERGE**, which is Databricks' own documented pattern for
    deduplicating a stream into Delta (checked live 2026-09-07). It replaced
    ``dropDuplicatesWithinWatermark``, which deduplicated correctly and lost
    data doing it: a distinct event whose event time is behind the watermark
    is dropped, and on this feed those are the common case, not the exotic one
    -- a real window lost **161 repos** that way.

    Two departures from the documented example, both deliberate:

    - The docs also bound the *source* side
      (``WHEN NOT MATCHED AND s.date > current_date() - 7 DAYS``). Copying
      that would reintroduce exactly the bug being fixed, since it refuses to
      insert old events at all. Only the **target** side is bounded here, so
      dedup stays cheap and every row still lands.
    - That bound is the batch's own ``event_date`` span rather than the docs'
      wall-clock window -- see ``_batch_date_span`` for why it is exact where
      the heuristic is approximate. Caught by a test, not by review: the
      wall-clock form silently stopped deduplicating fixture data dated
      months earlier, which is also how it would behave on any replay.

    ``txnAppId``/``txnVersion`` are gone with the append: they are
    ``DataFrameWriter`` options and do not apply to MERGE. Exactly-once is not
    weakened by that, it is restated -- an insert-only merge keyed on
    ``event_id`` is idempotent by construction, so a replayed batch matches
    every row it already wrote and inserts nothing.

    ``available_now=True`` processes whatever the landing zone holds right
    now and then stops -- a bounded, deterministic run for tests and for
    replay. The default (Spark's own continuous micro-batch trigger) is
    Task 9's real shape: a live poller keeps producing new files for the
    whole window, so the query must not stop on its own.
    """

    def _write_batch(batch: DataFrame, batch_id: int) -> None:
        # Scanned more than once below (the id check, then the write); cached
        # so that is one shuffle-free read. Databricks documents the same
        # caching for MERGE specifically, which reads its source repeatedly.
        batch.cache()
        try:
            _reject_id_less_event(batch)
            # MERGE dedups source against target, never source against itself
            # -- the docs are explicit -- so intra-batch duplicates must go
            # first or they land as duplicates.
            deduped = batch.dropDuplicates(["event_id"])
            spark = batch.sparkSession
            span = _batch_date_span(deduped)
            if span is not None and DeltaTable.isDeltaTable(spark, dest):
                first_date, last_date = span
                (
                    DeltaTable.forPath(spark, dest)
                    .alias("t")
                    .merge(
                        deduped.alias("s"),
                        (F.col("t.event_id") == F.col("s.event_id"))
                        & F.col("t.event_date").between(first_date, last_date),
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                # Nothing to merge into yet. Partitioning is established here
                # and inherited by every later MERGE.
                (
                    deduped.write.format("delta")
                    .partitionBy("event_date", "event_hour")
                    .mode("append")
                    .save(dest)
                )
        finally:
            batch.unpersist()

    writer = df.writeStream.foreachBatch(_write_batch).option("checkpointLocation", checkpoint)
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()
