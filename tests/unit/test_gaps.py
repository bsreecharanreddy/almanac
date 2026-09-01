from datetime import UTC, date, datetime

from almanac.pipeline.gaps import GapReport, expected_hours, missing_hours


def h(day: int, hour: int) -> datetime:
    return datetime(2025, 8, day, hour, tzinfo=UTC)


def test_no_gaps_when_all_present() -> None:
    expected = [h(13, 0), h(13, 1), h(13, 2)]
    assert missing_hours(expected, set(expected)) == []


def test_single_gap_detected() -> None:
    expected = [h(13, 0), h(13, 1), h(13, 2)]
    assert missing_hours(expected, {h(13, 0), h(13, 2)}) == [h(13, 1)]


def test_result_is_sorted() -> None:
    # Deliberately shuffled input. With an already-ascending `expected` a
    # plain filter comes out sorted by accident, so the sort goes untested
    # -- checked, and dropping `sorted()` kept the whole file green.
    expected = [h(13, 3), h(13, 0), h(13, 2), h(13, 1)]
    assert missing_hours(expected, {h(13, 0)}) == [h(13, 1), h(13, 2), h(13, 3)]


def test_extra_present_hours_are_ignored() -> None:
    # A present hour outside the expected range is not this function's
    # concern and must not crash it.
    assert missing_hours([h(13, 0)], {h(13, 0), h(14, 5)}) == []


def test_report_counts_are_consistent() -> None:
    r = GapReport.build([h(13, 0), h(13, 1)], {h(13, 0)})
    assert r.expected == 2
    assert r.present == 1
    assert r.missing == [h(13, 1)]
    assert not r.is_complete


def test_present_counts_only_expected_hours() -> None:
    # `present` means "expected hours that arrived", not "hours that exist
    # somewhere". Counting the `present` set directly reports 2 here and
    # claims more coverage than the range holds -- checked, that mutation
    # passes every other test in this file.
    r = GapReport.build([h(13, 0)], {h(13, 0), h(14, 5)})
    assert r.expected == 1
    assert r.present == 1
    assert r.is_complete


def test_complete_report_is_flagged_complete() -> None:
    r = GapReport.build([h(13, 0)], {h(13, 0)})
    assert r.is_complete


def test_expected_hours_covers_the_range_inclusive() -> None:
    got = expected_hours(date(2025, 8, 13), date(2025, 8, 14))
    assert len(got) == 48
    assert got[0] == h(13, 0)
    assert got[-1] == h(14, 23)
    assert got == sorted(got)


def test_expected_hours_are_timezone_aware() -> None:
    assert all(d.tzinfo is not None for d in expected_hours(date(2025, 8, 13), date(2025, 8, 13)))


def test_naive_hours_would_report_a_total_false_outage() -> None:
    """Why `expected_hours` owns the conversion instead of each caller.

    Aware and naive datetimes are never equal and never hash alike, so one
    naive side turns a fully-present day into a reported outage of all 24
    hours -- silent, and shaped exactly like a real one.
    """
    aware = expected_hours(date(2025, 8, 13), date(2025, 8, 13))
    naive = {d.replace(tzinfo=None) for d in aware}
    assert len(missing_hours(aware, naive)) == 24
