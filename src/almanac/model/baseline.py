"""The measured comparison every model has to beat (design doc §5.1,
'baseline first, always'): the segment's own statistic (median response
time for regression, breach rate for classification -- §5.3), with a
global fallback for a segment value never seen during training.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


@dataclass(frozen=True)
class NaiveBaseline:
    values: dict[bool, float]
    overall_value: float

    def predict(self, frame: pd.DataFrame) -> pd.Series[float]:
        """Map each row's segment to its trained statistic; unseen -> the overall one."""
        segment_col = next(iter(frame.columns))
        return frame[segment_col].map(self.values).fillna(self.overall_value)


def fit_naive_baseline(
    frame: pd.DataFrame,
    *,
    label_col: str = "time_to_first_response_seconds",
    segment_col: str = "is_bot_author",
    agg: Literal["median", "mean"] = "median",
) -> NaiveBaseline:
    """`agg` per segment, plus the overall `agg` as a fallback.

    `"median"` (default) is §5.1's regression baseline; `"mean"` on a
    boolean label is §5.3's classification baseline -- the segment's
    breach rate, used as a predicted probability.
    """
    per_segment = frame.groupby(segment_col)[label_col].agg(agg)
    values = {bool(segment): float(value) for segment, value in per_segment.items()}
    return NaiveBaseline(values=values, overall_value=float(frame[label_col].agg(agg)))
