"""train_model: a real LightGBM fit on real (synthetic, controlled) data,
never a mock -- proving it actually learns a known relationship, and that
the beats-baseline gate (design doc §5.1) fires correctly in both
directions.
"""

import numpy as np
import pandas as pd

from almanac.model.train import train_model


def _separable_frame(n: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    x = rng.uniform(0, 10, size=n)
    return pd.DataFrame(
        {
            "prior_pr_count": x,
            "prior_merge_rate": rng.uniform(0, 1, size=n),
            "events_total_to_date": rng.integers(0, 100, size=n),
            "bot_events_to_date": rng.integers(0, 10, size=n),
            "prs_opened_to_date": rng.integers(0, 20, size=n),
            "bot_share_to_date": rng.uniform(0, 1, size=n),
            "is_draft": rng.integers(0, 2, size=n).astype(bool),
            "is_bot_author": np.zeros(n, dtype=bool),
            "opened_day_of_week": rng.integers(1, 8, size=n),
            "opened_hour": rng.integers(0, 24, size=n),
            # The one feature that actually determines the label -- LightGBM
            # should learn this and comfortably beat the baseline's median.
            "time_to_first_response_seconds": x * 1000,
        }
    )


def test_a_perfectly_separable_feature_is_learned_and_beats_the_baseline() -> None:
    frame = _separable_frame()

    result = train_model(frame, random_state=42)

    assert result.beats_baseline is True
    assert result.model_mae < result.baseline_mae


def test_a_feature_with_no_signal_does_not_falsely_beat_the_baseline() -> None:
    rng = np.random.default_rng(1)
    n = 200
    frame = pd.DataFrame(
        {
            "prior_pr_count": rng.uniform(0, 10, size=n),
            "prior_merge_rate": rng.uniform(0, 1, size=n),
            "events_total_to_date": rng.integers(0, 100, size=n),
            "bot_events_to_date": rng.integers(0, 10, size=n),
            "prs_opened_to_date": rng.integers(0, 20, size=n),
            "bot_share_to_date": rng.uniform(0, 1, size=n),
            "is_draft": rng.integers(0, 2, size=n).astype(bool),
            "is_bot_author": np.zeros(n, dtype=bool),
            "opened_day_of_week": rng.integers(1, 8, size=n),
            "opened_hour": rng.integers(0, 24, size=n),
            # Pure noise, independent of every feature -- a well-behaved
            # model should not beat the baseline here, and the gate must
            # say so rather than silently registering an overfit model.
            "time_to_first_response_seconds": rng.uniform(0, 1, size=n) * 1000,
        }
    )

    result = train_model(frame, random_state=42)

    assert result.beats_baseline is False


def test_the_same_seed_produces_the_same_result_twice() -> None:
    frame = _separable_frame()

    first = train_model(frame, random_state=42)
    second = train_model(frame, random_state=42)

    assert first.model_mae == second.model_mae
    assert first.baseline_mae == second.baseline_mae
