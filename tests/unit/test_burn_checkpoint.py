"""``pending_days`` and the per-day resume markers."""

import json
from datetime import date
from pathlib import Path

import pytest

from almanac.burn.checkpoint import BackfillCheckpoint, pending_days


def test_pending_days_is_the_whole_range_when_nothing_is_done() -> None:
    days = pending_days(date(2025, 7, 1), date(2025, 7, 5), done=set())
    assert days == [date(2025, 7, d) for d in range(1, 6)]


def test_pending_days_skips_the_done_ones_and_keeps_order() -> None:
    done = {date(2025, 7, 2), date(2025, 7, 4)}
    assert pending_days(date(2025, 7, 1), date(2025, 7, 5), done) == [
        date(2025, 7, 1),
        date(2025, 7, 3),
        date(2025, 7, 5),
    ]


def test_pending_days_empty_when_all_done() -> None:
    span = [date(2025, 7, d) for d in range(1, 4)]
    assert pending_days(date(2025, 7, 1), date(2025, 7, 3), set(span)) == []


def test_pending_days_single_day_range() -> None:
    assert pending_days(date(2025, 7, 1), date(2025, 7, 1), set()) == [date(2025, 7, 1)]


def test_pending_days_rejects_a_reversed_range() -> None:
    with pytest.raises(ValueError, match="must not be before"):
        pending_days(date(2025, 7, 5), date(2025, 7, 1), set())


def test_pending_days_ignores_done_days_outside_the_range() -> None:
    done = {date(2024, 1, 1), date(2025, 7, 2)}
    assert pending_days(date(2025, 7, 1), date(2025, 7, 3), done) == [
        date(2025, 7, 1),
        date(2025, 7, 3),
    ]


# --- the marker directory ---


def test_checkpoint_of_a_missing_directory_is_empty_not_an_error(tmp_path: Path) -> None:
    assert BackfillCheckpoint(tmp_path / "nope").completed() == set()


def test_recorded_days_come_back(tmp_path: Path) -> None:
    cp = BackfillCheckpoint(tmp_path)
    cp.record(date(2025, 7, 1), {"rows_bronze": 10})
    cp.record(date(2025, 7, 2), {"rows_bronze": 20})
    assert cp.completed() == {date(2025, 7, 1), date(2025, 7, 2)}


def test_record_is_a_full_json_document_with_the_day_and_the_stats(tmp_path: Path) -> None:
    cp = BackfillCheckpoint(tmp_path)
    cp.record(
        date(2025, 7, 1),
        {"rows_bronze": 10, "hours_missing": ["2025-07-01T05:00:00+00:00"]},
    )
    written = json.loads((tmp_path / "2025-07-01.json").read_text())
    assert written["day"] == "2025-07-01"
    assert written["rows_bronze"] == 10
    assert written["hours_missing"] == ["2025-07-01T05:00:00+00:00"]


def test_completed_ignores_unrelated_and_half_written_files(tmp_path: Path) -> None:
    cp = BackfillCheckpoint(tmp_path)
    cp.record(date(2025, 7, 1), {})
    (tmp_path / "notes.json").write_text("{}")
    (tmp_path / "2025-07-02.json.part").write_text("{}")
    assert cp.completed() == {date(2025, 7, 1)}


def test_re_recording_a_day_overwrites_rather_than_duplicates(tmp_path: Path) -> None:
    cp = BackfillCheckpoint(tmp_path)
    cp.record(date(2025, 7, 1), {"attempt": 1})
    cp.record(date(2025, 7, 1), {"attempt": 2})
    assert cp.completed() == {date(2025, 7, 1)}
    assert len(list(tmp_path.glob("2025-07-01*.json"))) == 1
