"""The measured comparison every model has to beat (design doc §5.1,
'baseline first, always'): the segment's own median response time, with
a global fallback for a segment value never seen during training.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class NaiveBaseline:
    medians: dict[bool, float]
    overall_median: float

    def predict(self, frame: pd.DataFrame) -> pd.Series[float]:
        """Map each row's segment to its trained median; unseen -> the overall one."""
        segment_col = next(iter(frame.columns))
        return frame[segment_col].map(self.medians).fillna(self.overall_median)


def fit_naive_baseline(
    frame: pd.DataFrame,
    *,
    label_col: str = "time_to_first_response_seconds",
    segment_col: str = "is_bot_author",
) -> NaiveBaseline:
    """Median label per segment, plus the overall median as a fallback."""
    per_segment = frame.groupby(segment_col)[label_col].median()
    medians = {bool(segment): float(median) for segment, median in per_segment.items()}
    return NaiveBaseline(medians=medians, overall_median=float(frame[label_col].median()))
