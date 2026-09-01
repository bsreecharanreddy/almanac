from datetime import date

import pytest

from almanac.extract.urls import archive_url, hours_in_range


def test_hour_is_unpadded() -> None:
    # THE trap in this dataset's URL scheme: hour is 3, never 03.
    # Zero-padding yields a 404 for ten of every twenty-four files.
    assert archive_url(date(2025, 3, 15), 3).endswith("/2025-03-15-3.json.gz")


def test_midnight_is_zero_not_24() -> None:
    assert archive_url(date(2025, 3, 15), 0).endswith("/2025-03-15-0.json.gz")


def test_double_digit_hour_unchanged() -> None:
    assert archive_url(date(2025, 3, 15), 14).endswith("/2025-03-15-14.json.gz")


def test_date_is_zero_padded_even_though_hour_is_not() -> None:
    # Asymmetric on purpose, and easy to "tidy" into a bug later.
    assert archive_url(date(2014, 6, 2), 7).endswith("/2014-06-02-7.json.gz")


@pytest.mark.parametrize("bad", [-1, 24, 100])
def test_hour_out_of_range_rejected(bad: int) -> None:
    with pytest.raises(ValueError, match="hour"):
        archive_url(date(2025, 3, 15), bad)


def test_hours_in_range_is_inclusive_and_ordered() -> None:
    hours = list(hours_in_range(date(2025, 1, 1), date(2025, 1, 2)))
    assert len(hours) == 48
    assert hours[0] == (date(2025, 1, 1), 0)
    assert hours[-1] == (date(2025, 1, 2), 23)


def test_hours_in_range_rejects_backwards_range() -> None:
    with pytest.raises(ValueError, match="before"):
        list(hours_in_range(date(2025, 1, 2), date(2025, 1, 1)))
