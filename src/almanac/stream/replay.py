"""Replay real GH Archive hours through the identical streaming path.

The live feed samples ~7% of real GitHub volume (measured 2026-09-06,
docs/findings/2026-09-06-events-api-and-online-store-rates.md) and turns
over completely between polls -- it cannot demonstrate completeness, late
arrival, duplication or out-of-order delivery on demand, only whatever its
current window happens to contain. Replay forces each case deterministically
against real archive data, through the exact same `stream_events`/
`write_stream_silver` functions Task 3 built and Task 9 will run live, so a
result proved here is a claim about the real pipeline, not a stand-in.
"""

from __future__ import annotations

import gzip
import json
import random
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from almanac.config import Settings
from almanac.stream.ingest import late_event_count, stream_events, write_stream_silver

_DEFAULT_SOURCE_GLOB = "reduced-*.jsonl.gz"


@dataclass(frozen=True, slots=True)
class ReplayStats:
    """What one `replay_hours` call actually did, not what it was asked to do."""

    events_replayed: int  # distinct source events fed in
    events_landed: int  # total landing-file rows written, including redeliveries
    late_events: int  # summed from `late_event_count` across every poll cycle


def _load_events(source: Path) -> list[dict[str, object]]:
    with gzip.open(source, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def _created_at(event: dict[str, object]) -> datetime:
    return datetime.fromisoformat(str(event["created_at"]))


def _partition(events: list[dict[str, object]], cycles: int) -> list[list[dict[str, object]]]:
    """`events` (already sorted by `created_at`) split into `cycles`
    chronologically contiguous slices -- cycle i's events are never earlier
    than cycle i-1's, so delivering cycles in order advances the watermark
    smoothly instead of spuriously dropping a later cycle's own events.
    `cycles == 1` is the identity split, which the batch-equality gate needs.
    """
    base, extra = divmod(len(events), cycles)
    buckets = []
    start = 0
    for i in range(cycles):
        size = base + (1 if i < extra else 0)
        buckets.append(events[start : start + size])
        start += size
    return buckets


def _land_cycle(
    events: list[dict[str, object]], landing: Path, cycle: int, *, lateness: timedelta
) -> None:
    """One poll file, `stream_events`'s exact envelope -- but with a
    per-event `polled_at`, unlike the real poller's one-stamp-per-response
    (`stream.poller._write_poll`): the poller cannot know an event's
    `created_at` in advance, while replay works backward from it on
    purpose, to control delay precisely per event rather than per poll.

    `event` is JSON-encoded as a string, matching `_write_poll`'s own
    envelope: a nested object would be typed by the landing zone's read
    schema, silently dropping `actor` (§ingest.py's `_LANDING_SCHEMA`).
    """
    lines = [
        json.dumps({"polled_at": (_created_at(e) + lateness).isoformat(), "event": json.dumps(e)})
        for e in events
    ]
    (landing / f"cycle_{cycle:04d}.jsonl").write_text("\n".join(lines) + "\n")


def replay_hours(
    spark: SparkSession,
    hours: list[str],
    dest: str,
    *,
    source: Path | None = None,
    lateness: timedelta = timedelta(0),
    duplicate_rate: float = 0.0,
    shuffle: bool = False,
    seed: int = 0,
) -> ReplayStats:
    """Replay one real archive hour as `len(hours)` poll cycles against `dest`,
    through the exact `stream_events`/`write_stream_silver` path, delivered in
    chronological order so nothing is spuriously watermark-dropped by
    construction.

    `duplicate_rate` carries that fraction of each cycle's own *latest*
    events forward into the next cycle's landing file too -- close enough to
    the watermark boundary to survive it, so what gets exercised is
    cross-batch dedup, the case `dropDuplicatesWithinWatermark`'s state
    exists for, not an incidental watermark drop.

    `lateness`, when set, redelivers every event once more in a final
    trailing cycle, each `lateness` after its own `created_at`. By then every
    natural cycle has already advanced the watermark past the whole replay,
    so the redelivery is genuinely late on both axes: dropped by
    `dropDuplicatesWithinWatermark` (`created_at` is behind the watermark)
    and counted by `late_event_count` (`ingested_at - created_at` exceeds it)
    -- the two, otherwise-independent mechanisms `stream_events` computes
    (§ingest.py) only agree here because this is what the trailing cycle is
    built to force.

    `seed` is not decoration: Phase 5's Bug 3 was a nondeterministic `rand`
    sample read twice, and an unseeded shuffle here would be the same defect
    in a correctness harness.
    """
    source = source or sorted(Settings().fixture_dir.glob(_DEFAULT_SOURCE_GLOB))[0]
    events = _load_events(source)
    ordered = sorted(events, key=lambda e: str(e["created_at"]))
    buckets = _partition(ordered, len(hours))

    rng = random.Random(seed)
    if shuffle:
        for bucket in buckets:
            rng.shuffle(bucket)

    events_landed = 0
    late_events = 0

    with tempfile.TemporaryDirectory() as tmp:
        landing = Path(tmp) / "landing"
        checkpoint = Path(tmp) / "checkpoint"
        landing.mkdir()

        def run_cycle(to_land: list[dict[str, object]], index: int, *, delay: timedelta) -> None:
            nonlocal events_landed, late_events
            if to_land:
                _land_cycle(to_land, landing, index, lateness=delay)
                events_landed += len(to_land)
            stream = stream_events(spark, str(landing))
            query = write_stream_silver(stream, dest, str(checkpoint), available_now=True)
            query.awaitTermination()
            late_events += late_event_count(query)

        carry_forward: list[dict[str, object]] = []
        for index, bucket in enumerate(buckets):
            run_cycle(bucket + carry_forward, index, delay=timedelta(0))

            is_last = index == len(buckets) - 1
            carried = round(len(bucket) * duplicate_rate)
            carry_forward = [] if is_last or not carried else bucket[-carried:]

        if lateness > timedelta(0) and ordered:
            run_cycle(ordered, len(buckets), delay=lateness)

    return ReplayStats(
        events_replayed=len(events), events_landed=events_landed, late_events=late_events
    )
