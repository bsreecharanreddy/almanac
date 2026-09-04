"""log_training_run: against a real local MLflow file-store, never
mocked -- the same discipline as everywhere else in this codebase.
"""

from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import pytest

from almanac.model.train import log_training_run, train_model


def _frame(n: int = 60) -> pd.DataFrame:
    rng = np.random.default_rng(2)
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
            "time_to_first_response_seconds": x * 1000,
        }
    )


def test_logs_a_run_with_the_expected_metrics_and_returns_a_model_uri(tmp_path: Path) -> None:
    tracking_uri = f"file://{tmp_path}/mlruns"
    result = train_model(_frame(), random_state=42)

    model_uri = log_training_run(
        result, experiment_name="test-pr-review-sla-risk", tracking_uri=tracking_uri
    )

    assert model_uri.startswith("runs:/") and model_uri.endswith("/model")

    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["test-pr-review-sla-risk"])
    assert isinstance(runs, pd.DataFrame)
    assert len(runs) == 1
    assert runs.iloc[0]["metrics.model_mae"] == pytest.approx(result.model_mae)
    assert runs.iloc[0]["metrics.baseline_mae"] == pytest.approx(result.baseline_mae)
    assert bool(runs.iloc[0]["metrics.beats_baseline"]) == result.beats_baseline
