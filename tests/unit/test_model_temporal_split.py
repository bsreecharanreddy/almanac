"""temporal_split: the split design doc §4.5 requires and §5.1 constrains.

§4.5 states "Train/test must be split **temporally**; a random split is
itself a leakage bug". §5.1 adds that the boundary must fall on a whole
week, because weekday and weekend review latency differ sharply and an
arbitrary split point encodes day-of-week.

The champion registered on 2026-09-04 was trained through
`train_test_split(random_state=42)` instead. These tests pin the
replacement.
"""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from almanac.model.train import AS_OF_COLUMN, temporal_split

# 2025-07-07 is a Monday, so a frame starting here has clean week boundaries.
START = datetime(2025, 7, 7, 9, 0, tzinfo=UTC)


def frame(days: int = 28, per_day: int = 5) -> pd.DataFrame:
    stamps = [START + timedelta(days=d, hours=h) for d in range(days) for h in range(per_day)]
    return pd.DataFrame({AS_OF_COLUMN: stamps, "value": range(len(stamps))})


def test_no_test_row_precedes_any_train_row() -> None:
    """The property the whole thing exists for."""
    train, test = temporal_split(frame())

    assert train[AS_OF_COLUMN].max() < test[AS_OF_COLUMN].min()


def test_the_boundary_is_a_monday_at_midnight() -> None:
    """§5.1: an arbitrary split point encodes day-of-week as leakage."""
    _, test = temporal_split(frame())
    boundary = test[AS_OF_COLUMN].min()

    week_start = (boundary - timedelta(days=boundary.weekday())).normalize()

    assert boundary >= week_start
    assert min(test[AS_OF_COLUMN]) >= week_start
    # every test row is on or after the boundary week's Monday
    assert (test[AS_OF_COLUMN] >= week_start).all()
    assert (train_max := temporal_split(frame())[0][AS_OF_COLUMN].max()) < week_start, train_max


def test_every_row_lands_in_exactly_one_side() -> None:
    """Conservation, the same assertion the quarantine split gets."""
    original = frame()
    train, test = temporal_split(original)

    assert len(train) + len(test) == len(original)
    assert set(train["value"]) | set(test["value"]) == set(original["value"])
    assert not set(train["value"]) & set(test["value"])


def test_the_split_is_deterministic() -> None:
    """Byte-for-byte reproducibility is the governing invariant; a split
    that moves between runs breaks it as surely as an unpinned version.
    """
    first_train, first_test = temporal_split(frame())
    second_train, second_test = temporal_split(frame())

    pd.testing.assert_frame_equal(first_train, second_train)
    pd.testing.assert_frame_equal(first_test, second_test)


def test_it_picks_the_week_boundary_closest_to_the_requested_size() -> None:
    """28 days from a Monday gives boundaries at weeks 1, 2 and 3."""
    _, quarter = temporal_split(frame(), test_size=0.25)
    _, half = temporal_split(frame(), test_size=0.5)

    assert len(quarter) < len(half)


def test_a_frame_inside_one_week_cannot_be_split_and_says_so() -> None:
    """Refusing is correct: a silent empty test set would look like a pass."""
    with pytest.raises(ValueError, match="week boundary"):
        temporal_split(frame(days=3))


def test_a_naive_timestamp_column_works_too() -> None:
    """Local Spark hands back tz-naive timestamps; cloud runs are tz-aware."""
    naive = frame()
    naive[AS_OF_COLUMN] = naive[AS_OF_COLUMN].dt.tz_localize(None)

    train, test = temporal_split(naive)

    assert train[AS_OF_COLUMN].max() < test[AS_OF_COLUMN].min()
