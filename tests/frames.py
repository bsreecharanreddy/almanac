"""Pandas-only builders for the model tests.

Kept out of `tests/helpers.py` on purpose: that module imports PySpark at
module level, and these are used by the non-Spark tests that make up the
fast inner loop.
"""

from datetime import UTC, datetime, timedelta

import pandas as pd

from almanac.model.train import AS_OF_COLUMN

# A Monday, so a frame built from it has clean week boundaries.
FIRST_MONDAY = datetime(2025, 7, 7, tzinfo=UTC)


def with_as_of(frame: pd.DataFrame, *, weeks: int = 4) -> pd.DataFrame:
    """Spread the rows evenly across `weeks` calendar weeks from a Monday.

    `train_classifier` and `train_model` split temporally, so a frame with
    no `as_of_timestamp` -- or one landing inside a single week -- cannot be
    split at all. Every model fixture therefore has to carry real time.
    """
    span = timedelta(weeks=weeks)
    step = span / max(len(frame), 1)
    stamps = [FIRST_MONDAY + step * i for i in range(len(frame))]
    return frame.assign(**{AS_OF_COLUMN: stamps})
