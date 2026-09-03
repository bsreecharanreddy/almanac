"""fit_naive_baseline: the measured comparison every model has to beat
(design doc §5.1, 'baseline first, always') -- median response time,
segmented by is_bot_author, with a global fallback for an unseen segment.
"""

import pandas as pd

from almanac.model.baseline import fit_naive_baseline


def test_predicts_each_segments_own_median() -> None:
    train = pd.DataFrame(
        {
            "is_bot_author": [False, False, False, True, True],
            "time_to_first_response_seconds": [100, 200, 300, 10, 30],
        }
    )
    baseline = fit_naive_baseline(train)

    test = pd.DataFrame({"is_bot_author": [False, True]})
    predictions = baseline.predict(test)

    assert list(predictions) == [200, 20]


def test_an_unseen_segment_falls_back_to_the_overall_median() -> None:
    train = pd.DataFrame(
        {"is_bot_author": [False, False, False], "time_to_first_response_seconds": [10, 20, 30]}
    )
    baseline = fit_naive_baseline(train)

    test = pd.DataFrame({"is_bot_author": [True]})

    assert list(baseline.predict(test)) == [20]
