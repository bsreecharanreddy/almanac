"""train_model / train_classifier: a real LightGBM fit on real (synthetic,
controlled) data, never a mock -- proving it actually learns a known
relationship, and that each beats-baseline gate (design doc §5.1, §5.3)
fires correctly in both directions.
"""

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from mlflow.models import infer_signature

from almanac.model.train import (
    FEATURE_COLUMNS,
    SIMILARITY_FEATURE_COLUMNS,
    ClassifierCandidateResult,
    _best_candidate,
    train_classifier,
    train_model,
)
from tests.frames import with_as_of


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

    result = train_model(with_as_of(frame), random_state=42)

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

    result = train_model(with_as_of(frame), random_state=42)

    assert result.beats_baseline is False


def test_the_same_seed_produces_the_same_result_twice() -> None:
    frame = _separable_frame()

    first = train_model(with_as_of(frame), random_state=42)
    second = train_model(with_as_of(frame), random_state=42)

    assert first.model_mae == second.model_mae
    assert first.baseline_mae == second.baseline_mae


def _separable_classification_frame(n: int = 400) -> pd.DataFrame:
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
            # The one feature that actually determines the label.
            "breach": x > 5,
        }
    )


def test_a_perfectly_separable_feature_beats_the_baseline_by_pr_auc() -> None:
    frame = _separable_classification_frame()

    result = train_classifier(with_as_of(frame), random_state=42)

    assert result.beats_baseline is True
    assert len(result.candidates) >= 2  # a comparison sweep, not one config
    assert result.candidates[result.best_candidate].average_precision > (
        result.baseline_average_precision
    )


def test_a_classification_feature_with_no_signal_scores_near_chance() -> None:
    """Measured, not assumed: average precision rewards a model's ranking
    *variation*, so a baseline predicting one tied value for the whole
    population is close to unbeatable-proof under a PR-AUC gate
    regardless of true signal -- confirmed directly, a constant-segment
    version of this exact fixture "beat" the baseline by chance even at
    n=10,000. `beats_baseline` on genuinely no-signal data is therefore
    not the robust thing to assert (two near-chance estimators, and
    whichever wins is close to a coin flip); ROC-AUC near 0.5 is --
    proving no candidate actually learned a real relationship, which is
    what "no signal" means.
    """
    rng = np.random.default_rng(1)
    n = 3000
    frame = pd.DataFrame(
        {
            "prior_pr_count": rng.uniform(0, 10, size=n),
            "prior_merge_rate": rng.uniform(0, 1, size=n),
            "events_total_to_date": rng.integers(0, 100, size=n),
            "bot_events_to_date": rng.integers(0, 10, size=n),
            "prs_opened_to_date": rng.integers(0, 20, size=n),
            "bot_share_to_date": rng.uniform(0, 1, size=n),
            "is_draft": rng.integers(0, 2, size=n).astype(bool),
            "is_bot_author": rng.integers(0, 2, size=n).astype(bool),
            "opened_day_of_week": rng.integers(1, 8, size=n),
            "opened_hour": rng.integers(0, 24, size=n),
            # Pure noise, independent of every feature including is_bot_author.
            "breach": rng.integers(0, 2, size=n).astype(bool),
        }
    )

    result = train_classifier(with_as_of(frame), random_state=42)

    for candidate in result.candidates.values():
        assert 0.4 < candidate.roc_auc < 0.6


def test_classifier_same_seed_produces_the_same_result_twice() -> None:
    frame = _separable_classification_frame()

    first = train_classifier(with_as_of(frame), random_state=42)
    second = train_classifier(with_as_of(frame), random_state=42)

    assert first.candidates["default"].average_precision == (
        second.candidates["default"].average_precision
    )
    assert first.baseline_average_precision == second.baseline_average_precision


def test_train_classifier_defaults_to_feature_columns_unchanged() -> None:
    """The None path stays byte-identical to before Task 6 (design doc
    §8.3a) -- every existing call site passes no feature_columns at all."""
    frame = _separable_classification_frame()

    result = train_classifier(with_as_of(frame), random_state=42)

    assert result.feature_columns == FEATURE_COLUMNS


def test_similarity_columns_let_the_with_run_win_when_the_world_says_it_should() -> None:
    """Base features carry no signal (the no-signal fixture's own shape);
    only a similarity column does. The without-similarity arm should stay
    near chance and the with-similarity arm should actually learn it --
    mirrors test_a_classification_feature_with_no_signal_scores_near_chance's
    own no-signal-passes-first-try discipline, one level up.
    """
    rng = np.random.default_rng(2)
    n = 400
    similarity_signal = rng.uniform(0, 10, size=n)
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
            "similar_neighbor_count": rng.integers(0, 10, size=n),
            "similar_prior_breach_rate": similarity_signal / 10,
            "breach": similarity_signal > 5,
        }
    )

    timed = with_as_of(frame)
    without = train_classifier(timed, feature_columns=FEATURE_COLUMNS, random_state=42)
    with_similarity = train_classifier(
        timed,
        feature_columns=FEATURE_COLUMNS + SIMILARITY_FEATURE_COLUMNS,
        random_state=42,
    )

    assert without.feature_columns == FEATURE_COLUMNS
    assert with_similarity.feature_columns == FEATURE_COLUMNS + SIMILARITY_FEATURE_COLUMNS
    without_ap = without.candidates[without.best_candidate].average_precision
    with_ap = with_similarity.candidates[with_similarity.best_candidate].average_precision
    assert with_ap > without_ap
    assert with_similarity.beats_baseline is True


def test_best_candidate_picks_the_higher_average_precision() -> None:
    weaker = LGBMClassifier()
    stronger = LGBMClassifier()
    signature = infer_signature(pd.DataFrame({"x": [1.0]}), np.array([0]))
    candidates = {
        "default": ClassifierCandidateResult(
            model=weaker, roc_auc=0.5, average_precision=0.3, log_loss=1.0, signature=signature
        ),
        "is_unbalance": ClassifierCandidateResult(
            model=stronger, roc_auc=0.6, average_precision=0.7, log_loss=0.9, signature=signature
        ),
    }

    assert _best_candidate(candidates) == "is_unbalance"
