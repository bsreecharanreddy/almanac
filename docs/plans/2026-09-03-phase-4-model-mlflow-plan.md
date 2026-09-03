# Phase 4: Model + MLflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Train, track, register, and serve the PR review-SLA risk model
(§5) — a point-in-time-correct training frame built by joining Phase 3's
feature platform to Gold's label, a LightGBM regressor measured against a
naive baseline, MLflow tracking, Unity Catalog registration with a
`@champion` alias, and a live Databricks Model Serving endpoint — with the
real cloud verification step run now, against the still-live workspace,
rather than deferred past the credit deadline.

**Architecture:** A new `almanac/model/` package, parallel to
`almanac/features/` and `almanac/gold/`, split the same way: pure
transforms (`dataset.py`'s label join, `baseline.py`, `train.py`'s model
fit) versus one I/O-boundary module (`runner.py`) that reads pinned Delta
versions, orchestrates training, and talks to MLflow/Unity Catalog. A CLI
mirrors `almanac/features/runner.py` exactly. Serving is a Terraform-
managed `databricks_model_serving` endpoint invoked directly — no custom
API service.

**Tech Stack:** LightGBM (regressor), scikit-learn (baseline + metrics +
train/test split), pandas (the collected training frame), MLflow
(tracking + Unity Catalog model registry), Databricks Model Serving
(Terraform-managed), all on top of the existing PySpark/Delta stack.

**Spec:** `docs/design/2026-09-01-almanac-system-design.md` §5 (the
model), §5.1 (label definition), §5.2 (Phase 4 concretized — the four
decisions this plan implements), §6 (endpoints, scoped down per §5.2),
§8.1 (serving topology), §9 (phasing).

## Global Constraints

- **Regression, not classification.** Target is
  `time_to_first_response_seconds`; a "risk score" is the predicted
  duration compared against a threshold applied outside the model, not
  baked into it (§5.2).
- **Training population is `label_exclusion IS NULL` only** (§5.1's
  5-category taxonomy, unchanged) — never reframe or extend it.
- **The label is read from Gold, pinned to a matching Delta version**
  alongside the pinned features/Silver versions — a label is supervision,
  not a feature, so it does not extend `almanac/features/`'s own
  never-read-Gold rule, but it does inherit the same reproducibility
  discipline Task 7 (Phase 3) proved for features (§5.2).
- **Single-node training** (LightGBM + scikit-learn via pandas), not
  Spark MLlib — Spark's job stops at building the training set (§5.1,
  §5.2).
- **No model registers unless it beats the naive baseline by a measured
  margin.** A losing model logs its result and documents the null result
  in code, not by a human remembering to check (§5.1, §5.2).
- **`@champion` alias only, no `@challenger` machinery** — v1 has nothing
  to challenge against (§5.2).
- **Serving is Databricks Model Serving's own REST endpoint, invoked
  directly.** No custom FastAPI/HTTP service. `GET /features/{id}?as_of=`
  and `POST /similar-prs` are out of scope entirely (§5.2, §6).
- **The real cloud step (training run, MLflow experiment, UC
  registration, the serving endpoint) runs now**, against the still-live
  Databricks workspace, before the Sep 24 credit expiry — not deferred to
  a post-expiry paid window (§5.2, §9).
- **Dependency version floors, checked live 2026-09-03**: `mlflow>=3.15.2`,
  `lightgbm>=4.7.0`, `scikit-learn>=1.9.0`, `pandas>=3.0.5`. Follow
  `pyproject.toml`'s existing `spark`/`dbt` optional-dependency group
  style (a comment stating what was checked and when) exactly.
- **`make check` green before every commit** — the whole suite, not just
  this plan's new tests. `docs/STATUS.md` updates in the same commit as
  the work it describes. One commit per completed task.
- **No mocking anywhere** — this codebase has zero precedent for
  `unittest.mock`/`monkeypatch` (`GoldTarget.cli_flags()`,
  `RestSession`'s injectable `sleep`/`now`, Phase 3's `_build_parser()`
  tests are all real objects). The CLI is tested via real
  `argparse.Namespace` output; end-to-end wiring is tested via a real
  local MLflow `file://` tracking store and a real `SparkSession`, never
  a mock.
- **Full-recompute, not incremental** — training reads the latest (or an
  explicitly pinned) snapshot each run, matching Phase 3's own runner.

---

## Task 1: The `ml` dependency group, and the label join

**Files:**
- Modify: `pyproject.toml` (new `[project.optional-dependencies] ml` group)
- Create: `src/almanac/model/__init__.py`
- Create: `src/almanac/model/dataset.py`
- Test: `tests/unit/test_model_dataset.py`

**Interfaces:**
- Produces: `almanac.model.dataset.join_label(training_frame: DataFrame, fact_pull_request: DataFrame) -> DataFrame` — pure Spark transform, no I/O. Joins on `(repo_id, pr_number)`, keeps every column already on `training_frame` plus `time_to_first_response_seconds`, and filters to rows where `label_exclusion IS NULL`.

- [ ] **Step 1: Add the `ml` optional-dependency group**

In `pyproject.toml`, after the existing `dbt` group:

```toml
# Phase 4 (design doc §5.2): the model, tracking, and registry stack.
# Version floors checked live on 2026-09-03, not carried over from a plan.
ml = [
    "mlflow>=3.15.2",
    "lightgbm>=4.7.0",
    "scikit-learn>=1.9.0",
    "pandas>=3.0.5",
]
```

Run `uv sync --all-extras --dev` to confirm the group resolves.

- [ ] **Step 2: Create the empty package `__init__.py`**

```python
```

(Matches `almanac/features/__init__.py` and `almanac/gold/__init__.py` — empty.)

- [ ] **Step 3: Write the failing test for `join_label`**

```python
"""join_label: attaches Gold's label onto a point-in-time-correct
training frame -- a label is supervision, not a feature, so this lives
outside almanac/features/ (which never reads Gold, §3.1) without
extending that same boundary to the label (§5.2).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.model.dataset import join_label

pytestmark = pytest.mark.spark


def test_joins_the_label_and_drops_rows_with_no_defined_outcome(spark: SparkSession) -> None:
    training_frame = spark.createDataFrame(
        [(1, 5, "alice"), (1, 6, "bob")],
        "repo_id long, pr_number long, author_login string",
    )
    fact_pull_request = spark.createDataFrame(
        [
            (1, 5, 3600, None),
            (1, 6, None, "right_censored"),
        ],
        "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string",
    )

    result = join_label(training_frame, fact_pull_request).collect()

    assert len(result) == 1
    assert result[0]["pr_number"] == 5
    assert result[0]["time_to_first_response_seconds"] == 3600
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_model_dataset.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.model.dataset'`

- [ ] **Step 5: Implement `join_label`**

```python
"""Attaches Gold's label onto a point-in-time-correct training frame.

A label is supervision, not a feature: §3.1's peer-of-Gold rule was about
avoiding leakage inside point-in-time *feature* computation, and a label
is definitionally about the future outcome, so this module reads Gold
without extending almanac/features/'s own never-read-Gold boundary to it
(design doc §5.2).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def join_label(training_frame: DataFrame, fact_pull_request: DataFrame) -> DataFrame:
    label = fact_pull_request.select(
        "repo_id", "pr_number", "time_to_first_response_seconds", "label_exclusion"
    )
    joined = training_frame.join(label, on=["repo_id", "pr_number"], how="inner")
    return joined.where(F.col("label_exclusion").isNull()).drop("label_exclusion")
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_model_dataset.py -v`
Expected: PASS

- [ ] **Step 7: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/almanac/model/__init__.py src/almanac/model/dataset.py tests/unit/test_model_dataset.py
git commit -m "feat: Phase 4 Task 1 -- the ml dependency group and the label join"
```

---

## Task 2: `build_training_frame` — the pinned-version I/O boundary

**Files:**
- Modify: `src/almanac/model/dataset.py`
- Test: `tests/integration/test_model_dataset_versions.py`

**Interfaces:**
- Consumes: `almanac.features.spine.build_pr_opened_spine(events: DataFrame) -> DataFrame`; `almanac.features.assemble.assemble_training_set(spine, *, author_activity, repo_activity, pr_static) -> DataFrame`; `almanac.model.dataset.join_label` (Task 1).
- Produces: `almanac.model.dataset.build_training_frame(spark: SparkSession, *, silver_path: str, features_path: str, gold_warehouse: str, silver_version: int | None = None, features_version: int | None = None, gold_version: int | None = None) -> pandas.DataFrame` — reads Silver events, the three v1 feature tables, and `fact_pull_request`, each at its own optionally-pinned Delta version; returns one collected pandas frame.

This is where the leakage suite's invariant (Task 7, Phase 3) extends to
the label side: a training frame built from pinned versions must
reproduce byte-for-byte later, even after a late-arriving Gold label
update whose own timestamp predates the training cutoff.

- [ ] **Step 1: Write the failing test**

```python
"""build_training_frame: every Delta read is independently version-
pinnable, so a specific training frame -- features AND label -- stays
reproducible even after later Gold/Silver activity, extending Task 7's
leakage-suite invariant to the label join (design doc §5.2).
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from almanac.model.dataset import build_training_frame

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)
_GOLD_SCHEMA = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string"
)


def _latest_version(spark: SparkSession, path: Path) -> int:
    row = DeltaTable.forPath(spark, str(path)).history(1).select("version").collect()[0]
    return int(row["version"])


def test_pinning_every_version_reproduces_the_frame_after_a_later_label_update(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_warehouse = tmp_path / "warehouse"

    spark.createDataFrame(
        [
            (
                1,
                5,
                datetime(2025, 8, 13, 9, tzinfo=UTC),
                "PullRequestEvent",
                "opened",
                "alice",
                None,
                False,
                None,
                datetime(2025, 8, 13, 9, 5, tzinfo=UTC),
            )
        ],
        _SILVER_SCHEMA,
    ).write.format("delta").save(str(silver_path / "clean"))

    from almanac.features.runner import run_features

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )
    features_v1 = _latest_version(spark, features_path / "author_activity")

    gold_path = gold_warehouse / "gold.db" / "fact_pull_request"
    spark.createDataFrame([(1, 5, 3600, None)], _GOLD_SCHEMA).write.format("delta").save(
        str(gold_path)
    )
    gold_v1 = _latest_version(spark, gold_path)

    def build(features_version: int, gold_version: int) -> list[dict[str, object]]:
        frame = build_training_frame(
            spark,
            silver_path=str(silver_path),
            features_path=str(features_path),
            gold_warehouse=str(gold_warehouse),
            features_version=features_version,
            gold_version=gold_version,
        )
        return frame.to_dict("records")

    original = build(features_v1, gold_v1)
    assert original[0]["time_to_first_response_seconds"] == 3600

    # A later Gold correction: a different PR's label lands as a second
    # Delta version, well after the original build -- not backdated in place.
    spark.createDataFrame([(1, 6, 7200, None)], _GOLD_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(gold_path))
    gold_v2 = _latest_version(spark, gold_path)

    pinned = build(features_v1, gold_v1)
    assert pinned == original

    live = build(features_v1, gold_v2)
    assert len(live) == 1  # PR 6 has no feature-side spine row; still one training row
    assert live == original  # this PR's own label is untouched by the other PR's append
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/integration/test_model_dataset_versions.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_training_frame'`

- [ ] **Step 3: Implement `build_training_frame`**

Append to `src/almanac/model/dataset.py`:

```python
from pyspark.sql import DataFrame, SparkSession

from almanac.features.assemble import assemble_training_set
from almanac.features.spine import build_pr_opened_spine


def _read_delta(spark: SparkSession, path: str, version: int | None) -> DataFrame:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def build_training_frame(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_warehouse: str,
    silver_version: int | None = None,
    features_version: int | None = None,
    gold_version: int | None = None,
) -> "pandas.DataFrame":  # noqa: F821 -- pandas imported below, quoted to keep this diff-local
    events = _read_delta(spark, f"{silver_path}/clean", silver_version)
    spine = build_pr_opened_spine(events)

    author_activity = _read_delta(spark, f"{features_path}/author_activity", features_version)
    repo_activity = _read_delta(spark, f"{features_path}/repo_activity", features_version)
    pr_static = _read_delta(spark, f"{features_path}/pr_static", features_version)
    training_frame = assemble_training_set(
        spine, author_activity=author_activity, repo_activity=repo_activity, pr_static=pr_static
    )

    fact_pull_request = _read_delta(
        spark, f"{gold_warehouse}/gold.db/fact_pull_request", gold_version
    )
    return join_label(training_frame, fact_pull_request).toPandas()
```

Then fix the import at the top of the file to a real one (drop the quoted
annotation once it compiles):

```python
import pandas as pd
```

and change the return type to `pd.DataFrame`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/integration/test_model_dataset_versions.py -v`
Expected: PASS

- [ ] **Step 5: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean. `pandas` ships inline type hints as of the 3.x series
checked in Task 1; if mypy still reports missing stubs, this is a real,
environment-specific finding to fix and record in STATUS.md, the same as
every other real bug this project has found via `make check` rather than
assumed away.

- [ ] **Step 6: Commit**

```bash
git add src/almanac/model/dataset.py tests/integration/test_model_dataset_versions.py
git commit -m "feat: Phase 4 Task 2 -- build_training_frame, version-pinned end to end"
```

---

## Task 3: The naive baseline

**Files:**
- Create: `src/almanac/model/baseline.py`
- Test: `tests/unit/test_model_baseline.py`

**Interfaces:**
- Produces: `almanac.model.baseline.NaiveBaseline` (frozen dataclass: `medians: dict[bool, float]`, `overall_median: float`), `almanac.model.baseline.fit_naive_baseline(frame: pd.DataFrame, *, label_col: str = "time_to_first_response_seconds", segment_col: str = "is_bot_author") -> NaiveBaseline`, `NaiveBaseline.predict(frame: pd.DataFrame) -> pd.Series`.

- [ ] **Step 1: Write the failing test**

```python
"""fit_naive_baseline: the measured comparison every model has to beat
(design doc §5.1, 'baseline first, always') -- median response time,
segmented by is_bot_author, with a global fallback for an unseen segment.
"""

import pandas as pd

from almanac.model.baseline import fit_naive_baseline


def test_predicts_the_segments_own_median_and_falls_back_for_an_unseen_segment() -> None:
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_model_baseline.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.model.baseline'`

- [ ] **Step 3: Implement `baseline.py`**

```python
"""The measured comparison every model has to beat (design doc §5.1,
'baseline first, always'): the segment's own median response time, with
a global fallback for a segment value never seen during training.
"""

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class NaiveBaseline:
    medians: dict[bool, float]
    overall_median: float

    def predict(self, frame: pd.DataFrame) -> pd.Series:
        segment_col = next(iter(frame.columns))
        return frame[segment_col].map(self.medians).fillna(self.overall_median)


def fit_naive_baseline(
    frame: pd.DataFrame,
    *,
    label_col: str = "time_to_first_response_seconds",
    segment_col: str = "is_bot_author",
) -> NaiveBaseline:
    medians = frame.groupby(segment_col)[label_col].median().to_dict()
    overall_median = float(frame[label_col].median())
    return NaiveBaseline(medians=medians, overall_median=overall_median)
```

`NaiveBaseline.predict` reading `frame.columns[0]` rather than a stored
segment-column name keeps the dataclass a plain value object; the caller
always hands it a single-column frame carrying exactly the segment it was
fit on, matching how `train.py` (Task 4) will call it.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_model_baseline.py -v`
Expected: PASS

- [ ] **Step 5: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean

- [ ] **Step 6: Commit**

```bash
git add src/almanac/model/baseline.py tests/unit/test_model_baseline.py
git commit -m "feat: Phase 4 Task 3 -- the naive baseline"
```

---

## Task 4: `train_model` — LightGBM, fixed seeds, the beats-baseline gate

**Files:**
- Create: `src/almanac/model/train.py`
- Test: `tests/unit/test_model_train.py`

**Interfaces:**
- Consumes: `almanac.model.baseline.fit_naive_baseline`, `NaiveBaseline` (Task 3).
- Produces: `almanac.model.train.FEATURE_COLUMNS: list[str]` (the ten columns `assemble_training_set` + `join_label` produce, minus identifiers and the label); `almanac.model.train.TrainResult` (frozen dataclass: `model: LGBMRegressor`, `baseline: NaiveBaseline`, `model_mae: float`, `baseline_mae: float`, `beats_baseline: bool`); `almanac.model.train.train_model(frame: pd.DataFrame, *, random_state: int = 42, test_size: float = 0.3, **lgbm_params: object) -> TrainResult`.

`FEATURE_COLUMNS` is stated once here because it is the one place both
`train_model` (which columns to feed the regressor) and `runner.py`
(Task 6, which columns to log as a param) need the same list — declaring
it as a second, drifted copy in `runner.py` would be exactly the
two-places-one-contract failure this codebase's own `tests/helpers.py`
already calls out.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_model_train.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.model.train'`

- [ ] **Step 3: Implement `train.py`**

```python
"""Trains the SLA-risk regressor and measures it against the naive
baseline -- 'no model ships without a measured comparison' (design doc
§5.1), enforced here in code rather than left to a checklist step.
"""

from dataclasses import dataclass

import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

from almanac.model.baseline import NaiveBaseline, fit_naive_baseline

LABEL_COLUMN = "time_to_first_response_seconds"
SEGMENT_COLUMN = "is_bot_author"

# The columns assemble_training_set() + join_label() produce, minus
# identifiers (repo_id, pr_number, author_login, as_of_timestamp) and the
# label itself. Stated once: runner.py (Task 6) logs this same list as an
# MLflow param rather than re-deriving or re-typing it.
FEATURE_COLUMNS: list[str] = [
    "prior_pr_count",
    "prior_merge_rate",
    "events_total_to_date",
    "bot_events_to_date",
    "prs_opened_to_date",
    "bot_share_to_date",
    "is_draft",
    "is_bot_author",
    "opened_day_of_week",
    "opened_hour",
]


@dataclass(frozen=True)
class TrainResult:
    model: LGBMRegressor
    baseline: NaiveBaseline
    model_mae: float
    baseline_mae: float
    beats_baseline: bool


def train_model(
    frame: pd.DataFrame,
    *,
    random_state: int = 42,
    test_size: float = 0.3,
    **lgbm_params: object,
) -> TrainResult:
    train, test = train_test_split(frame, test_size=test_size, random_state=random_state)

    baseline = fit_naive_baseline(train, label_col=LABEL_COLUMN, segment_col=SEGMENT_COLUMN)
    baseline_predictions = baseline.predict(test[[SEGMENT_COLUMN]])
    baseline_mae = float(mean_absolute_error(test[LABEL_COLUMN], baseline_predictions))

    model = LGBMRegressor(random_state=random_state, verbosity=-1, **lgbm_params)
    x_train = train[FEATURE_COLUMNS].astype("float64")
    model.fit(x_train, train[LABEL_COLUMN])
    x_test = test[FEATURE_COLUMNS].astype("float64")
    model_predictions = model.predict(x_test)
    model_mae = float(mean_absolute_error(test[LABEL_COLUMN], model_predictions))

    return TrainResult(
        model=model,
        baseline=baseline,
        model_mae=model_mae,
        baseline_mae=baseline_mae,
        beats_baseline=model_mae < baseline_mae,
    )
```

Feature columns are cast to `float64` before fitting: pandas booleans
(`is_draft`, `is_bot_author`) convert cleanly to `1.0`/`0.0`, and any
future null in a numeric feature becomes `NaN`, which LightGBM handles
natively as a missing value rather than raising.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_model_train.py -v`
Expected: PASS. If the no-signal test is flaky (LightGBM finding weak
spurious structure in 200 noise rows), that is itself a real,
worth-recording finding — increase `n` or use a coarser assertion (e.g.
`model_mae > 0.9 * baseline_mae` rather than requiring an exact `False`)
and document which was needed and why in this task's STATUS.md row, the
same as every other real behavior this project has found via a failing
test rather than assumed away.

- [ ] **Step 5: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean

- [ ] **Step 6: Commit**

```bash
git add src/almanac/model/train.py tests/unit/test_model_train.py
git commit -m "feat: Phase 4 Task 4 -- train_model, LightGBM against the naive baseline"
```

---

## Task 5: MLflow run logging

**Files:**
- Modify: `src/almanac/model/train.py`
- Test: `tests/unit/test_model_train_logging.py`

**Interfaces:**
- Consumes: `TrainResult`, `FEATURE_COLUMNS` (Task 4).
- Produces: `almanac.model.train.log_training_run(result: TrainResult, *, experiment_name: str, tracking_uri: str) -> str` — logs params/metrics/the model artifact to MLflow, returns the run's model URI (`runs:/<run_id>/model`).

This is the one function in this task genuinely doing I/O, tested against
a real local `file://` MLflow tracking store (no Databricks connection
needed to prove the logging contract) — the same "test the real object,
never mock it" discipline as everywhere else in this codebase.

- [ ] **Step 1: Write the failing test**

```python
"""log_training_run: against a real local MLflow file-store, never
mocked -- the same discipline as everywhere else in this codebase.
"""

from pathlib import Path

import mlflow
import pandas as pd
import pytest

from almanac.model.train import log_training_run, train_model


def _frame(n: int = 60) -> pd.DataFrame:
    import numpy as np

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
    assert len(runs) == 1
    assert runs.iloc[0]["metrics.model_mae"] == pytest.approx(result.model_mae)
    assert runs.iloc[0]["metrics.baseline_mae"] == pytest.approx(result.baseline_mae)
    assert bool(runs.iloc[0]["metrics.beats_baseline"]) == result.beats_baseline
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_model_train_logging.py -v`
Expected: FAIL — `ImportError: cannot import name 'log_training_run'`

- [ ] **Step 3: Implement `log_training_run`**

Append to `src/almanac/model/train.py`:

```python
import mlflow
import mlflow.lightgbm


def log_training_run(result: TrainResult, *, experiment_name: str, tracking_uri: str) -> str:
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run() as run:
        mlflow.log_param("feature_columns", FEATURE_COLUMNS)
        mlflow.log_param("random_state", result.model.get_params()["random_state"])
        mlflow.log_metric("model_mae", result.model_mae)
        mlflow.log_metric("baseline_mae", result.baseline_mae)
        mlflow.log_metric("beats_baseline", float(result.beats_baseline))
        mlflow.lightgbm.log_model(result.model, name="model")
        return f"runs:/{run.info.run_id}/model"
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_model_train_logging.py -v`
Expected: PASS

- [ ] **Step 5: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean

- [ ] **Step 6: Commit**

```bash
git add src/almanac/model/train.py tests/unit/test_model_train_logging.py
git commit -m "feat: Phase 4 Task 5 -- MLflow run logging"
```

---

## Task 6: Unity Catalog registration

**Files:**
- Create: `src/almanac/model/registry.py`
- Test: `tests/unit/test_model_registry.py`

**Interfaces:**
- Produces: `almanac.model.registry.registered_model_name(catalog: str, schema: str, model_name: str = "pr_review_sla_risk") -> str`; `almanac.model.registry.register_champion(model_uri: str, *, name: str, registry_uri: str = "databricks-uc") -> str` — registers `model_uri`, aliases the new version `@champion`, returns `f"models:/{name}@champion"`.

Only `registered_model_name` is unit-tested here — pure string
composition, no network. `register_champion` genuinely needs a live UC
metastore to exercise; real execution is Task 8's cloud-verification
step, the same deferral Phase 3 Task 5 used for `primary_key_sql`'s
`ALTER TABLE` statements (string-built and unit-tested; execution against
a live target deferred and recorded separately).

- [ ] **Step 1: Write the failing test**

```python
"""registered_model_name: pure string composition, no network -- the
live-registration half (register_champion) is exercised for real only
during Phase 4's cloud verification step, mirroring how Phase 3 Task 5
deferred primary_key_sql's live execution the same way.
"""

from almanac.model.registry import registered_model_name


def test_composes_the_three_part_uc_name() -> None:
    assert registered_model_name("almanac", "models") == "almanac.models.pr_review_sla_risk"


def test_the_model_name_is_overridable() -> None:
    assert (
        registered_model_name("almanac", "models", "custom_model") == "almanac.models.custom_model"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_model_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.model.registry'`

- [ ] **Step 3: Implement `registry.py`**

```python
"""Unity Catalog registration for the trained model -- pure naming logic
is unit-tested here; register_champion's live call is exercised for real
only during Phase 4's cloud verification step (design doc §5.2), the same
deferral Phase 3 Task 5 used for its own UC registration SQL.
"""

import mlflow
from mlflow import MlflowClient

CHAMPION_ALIAS = "champion"


def registered_model_name(catalog: str, schema: str, model_name: str = "pr_review_sla_risk") -> str:
    return f"{catalog}.{schema}.{model_name}"


def register_champion(model_uri: str, *, name: str, registry_uri: str = "databricks-uc") -> str:
    mlflow.set_registry_uri(registry_uri)
    version = mlflow.register_model(model_uri, name)
    MlflowClient().set_registered_model_alias(
        name=name, alias=CHAMPION_ALIAS, version=version.version
    )
    return f"models:/{name}@{CHAMPION_ALIAS}"
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_model_registry.py -v`
Expected: PASS

- [ ] **Step 5: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean

- [ ] **Step 6: Commit**

```bash
git add src/almanac/model/registry.py tests/unit/test_model_registry.py
git commit -m "feat: Phase 4 Task 6 -- Unity Catalog registration, champion alias"
```

---

## Task 7: The runner and CLI

**Files:**
- Create: `src/almanac/model/runner.py`
- Create: `scripts/model.py`
- Test: `tests/unit/test_model_runner_cli.py`
- Test: `tests/integration/test_model_runner.py`

**Interfaces:**
- Consumes: `build_training_frame` (Task 2), `train_model`, `log_training_run`, `FEATURE_COLUMNS` (Tasks 4-5), `registered_model_name`, `register_champion` (Task 6), `almanac.spark.local_session`, `almanac.cli.run_cli`.
- Produces: `almanac.model.runner.run_training(spark, *, silver_path, features_path, gold_warehouse, tracking_uri, experiment_name, register, catalog="almanac", schema="models", model_name="pr_review_sla_risk", registry_uri="databricks-uc", silver_version=None, features_version=None, gold_version=None) -> TrainResult`; `_build_parser()`; `main(argv)`.

- [ ] **Step 1: Write the CLI's failing test**

```python
"""_build_parser: argument defaults and required flags, as a pure unit
test -- main()'s real dispatch is tests/integration/test_model_runner.py's
job, exactly like Phase 3's runner tests split the same way.
"""

import pytest

from almanac.model.runner import _build_parser


def test_register_defaults_false_and_names_default(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    args = _build_parser().parse_args(
        [
            "--silver-path",
            "/s",
            "--features-path",
            "/f",
            "--gold-warehouse",
            "/g",
            "--tracking-uri",
            "file:///tmp/mlruns",
            "--experiment-name",
            "pr-review-sla-risk",
        ]
    )

    assert args.register is False
    assert args.catalog == "almanac"
    assert args.schema == "models"
    assert args.model_name == "pr_review_sla_risk"


def test_required_flags_are_enforced() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--features-path", "/f"])
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_model_runner_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.model.runner'`

- [ ] **Step 3: Implement `runner.py`**

```python
"""build_training_frame -> train_model -> log_training_run -> (maybe)
register_champion. The I/O boundary for Phase 4, in the same shape as
almanac/features/runner.py.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from almanac.cli import run_cli
from almanac.model.dataset import build_training_frame
from almanac.model.registry import register_champion, registered_model_name
from almanac.model.train import TrainResult, log_training_run, train_model
from almanac.spark import local_session


def _active_or_local_session() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-model")


def run_training(
    spark: SparkSession,
    *,
    silver_path: str,
    features_path: str,
    gold_warehouse: str,
    tracking_uri: str,
    experiment_name: str,
    register: bool,
    catalog: str = "almanac",
    schema: str = "models",
    model_name: str = "pr_review_sla_risk",
    registry_uri: str = "databricks-uc",
    silver_version: int | None = None,
    features_version: int | None = None,
    gold_version: int | None = None,
) -> TrainResult:
    """Registration only fires when both `register` is set AND the model
    actually beat the baseline (§5.1's gate, enforced here rather than
    left to a human to remember to check).
    """
    frame = build_training_frame(
        spark,
        silver_path=silver_path,
        features_path=features_path,
        gold_warehouse=gold_warehouse,
        silver_version=silver_version,
        features_version=features_version,
        gold_version=gold_version,
    )
    result = train_model(frame)
    model_uri = log_training_run(result, experiment_name=experiment_name, tracking_uri=tracking_uri)
    if register and result.beats_baseline:
        name = registered_model_name(catalog, schema, model_name)
        register_champion(model_uri, name=name, registry_uri=registry_uri)
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the PR review-SLA risk model.")
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--gold-warehouse", required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--catalog", default="almanac")
    parser.add_argument("--schema", default="models")
    parser.add_argument("--model-name", default="pr_review_sla_risk")
    parser.add_argument("--registry-uri", default="databricks-uc")
    parser.add_argument(
        "--register",
        action="store_true",
        help=(
            "Register as @champion in Unity Catalog if the model beats the "
            "baseline. Needs a live UC metastore -- off unless explicitly "
            "requested, same convention as almanac.features.runner's --register."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_training(
        _active_or_local_session(),
        silver_path=args.silver_path,
        features_path=args.features_path,
        gold_warehouse=args.gold_warehouse,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
        register=args.register,
        catalog=args.catalog,
        schema=args.schema,
        model_name=args.model_name,
        registry_uri=args.registry_uri,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
```

- [ ] **Step 4: Run the CLI test to verify it passes**

Run: `uv run pytest tests/unit/test_model_runner_cli.py -v`
Expected: PASS

- [ ] **Step 5: Create the launch shim**

```python
"""Launch shim for the model runner on a job cluster; the CLI itself is
almanac.model.runner.
"""

from almanac.cli import run_cli
from almanac.model.runner import main

if __name__ == "__main__":
    run_cli(main)
```

- [ ] **Step 6: Write the failing end-to-end integration test**

```python
"""run_training / main against real Delta I/O and a real local MLflow
file-store -- offline and hermetic, exactly like every other integration
test in this repo. The live Databricks/UC path is exercised for real only
during Phase 4's cloud verification step (this plan's own next task).
"""

from datetime import UTC, datetime
from pathlib import Path

import mlflow
import pytest
from pyspark.sql import SparkSession

from almanac.model.runner import main, run_training

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)
_GOLD_SCHEMA = (
    "repo_id long, pr_number long, time_to_first_response_seconds long, label_exclusion string"
)


def _write_silver_and_gold(spark: SparkSession, tmp_path: Path) -> tuple[Path, Path, Path]:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    gold_warehouse = tmp_path / "warehouse"

    rows = [
        (
            1,
            n,
            datetime(2025, 8, 13, 9, tzinfo=UTC),
            "PullRequestEvent",
            "opened",
            "alice",
            None,
            False,
            None,
            datetime(2025, 8, 13, 9, 5, tzinfo=UTC),
        )
        for n in range(1, 40)
    ]
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").save(
        str(silver_path / "clean")
    )

    from almanac.features.runner import run_features

    run_features(
        spark, silver_path=str(silver_path), features_path=str(features_path), register=False
    )

    gold_rows = [(1, n, 1000 + n * 10, None) for n in range(1, 40)]
    spark.createDataFrame(gold_rows, _GOLD_SCHEMA).write.format("delta").save(
        str(gold_warehouse / "gold.db" / "fact_pull_request")
    )
    return silver_path, features_path, gold_warehouse


def test_run_training_logs_a_real_mlflow_run(spark: SparkSession, tmp_path: Path) -> None:
    silver_path, features_path, gold_warehouse = _write_silver_and_gold(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"

    result = run_training(
        spark,
        silver_path=str(silver_path),
        features_path=str(features_path),
        gold_warehouse=str(gold_warehouse),
        tracking_uri=tracking_uri,
        experiment_name="pr-review-sla-risk-test",
        register=False,
    )

    assert result.model_mae >= 0
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-risk-test"])
    assert len(runs) == 1


def test_main_wires_the_parsed_arguments_through_to_a_real_run(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_path, features_path, gold_warehouse = _write_silver_and_gold(spark, tmp_path)
    tracking_uri = f"file://{tmp_path}/mlruns"

    code = main(
        [
            "--silver-path",
            str(silver_path),
            "--features-path",
            str(features_path),
            "--gold-warehouse",
            str(gold_warehouse),
            "--tracking-uri",
            tracking_uri,
            "--experiment-name",
            "pr-review-sla-risk-cli-test",
        ]
    )

    assert code == 0
    mlflow.set_tracking_uri(tracking_uri)
    runs = mlflow.search_runs(experiment_names=["pr-review-sla-risk-cli-test"])
    assert len(runs) == 1
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/integration/test_model_runner.py -v`
Expected: PASS

- [ ] **Step 8: Lint, format, typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src tests`
Expected: clean

- [ ] **Step 9: Commit**

```bash
git add src/almanac/model/runner.py scripts/model.py tests/unit/test_model_runner_cli.py tests/integration/test_model_runner.py
git commit -m "feat: Phase 4 Task 7 -- the runner and CLI"
```

---

## Task 8: Terraform — the UC registry catalog, the training job, the serving endpoint

**Files:**
- Modify: `infra/terraform/unity_catalog.tf` (new UC catalog/schema for the model registry)
- Modify: `infra/terraform/databricks.tf` (new training job; new `databricks_model_serving` resource)
- Modify: `infra/terraform/variables.tf` (new variables)
- Modify: `infra/terraform/outputs.tf` (endpoint URL output)

Mirrors the existing `databricks_job.gold` shape exactly for the training
job, and adds the one genuinely new resource type this phase needs:
`databricks_model_serving`, confirmed live today against the current
provider docs (`scale_to_zero_enabled` on the served-model block, per
design doc §5.2).

- [ ] **Step 1: New variables**

Append to `infra/terraform/variables.tf`:

```hcl
variable "model_registry_catalog" {
  type        = string
  description = "Unity Catalog catalog holding the trained model (Phase 4, design doc §5.2)."
  default     = "almanac"
}

variable "model_registry_schema" {
  type        = string
  description = "Unity Catalog schema, under model_registry_catalog, holding the trained model."
  default     = "models"
}

variable "model_python_file" {
  type        = string
  description = "Workspace path of scripts/model.py, the job entrypoint for almanac.model.runner."
  default     = "/Workspace/Shared/almanac/scripts/model.py"
}

variable "model_pip_dependencies" {
  type = list(string)
  # Keep in sync with pyproject.toml's [project.optional-dependencies] ml
  # group -- a raw databricks_job cannot derive them; a bundle would.
  description = "The ml extra's runtime deps, installed on the training job's cluster."
  default = [
    "mlflow>=3.15.2",
    "lightgbm>=4.7.0",
    "scikit-learn>=1.9.0",
    "pandas>=3.0.5",
  ]
}

variable "model_serving_workers" {
  type        = number
  description = "Fixed workload size for the served model. Scale-to-zero governs idle cost, not this."
  default     = 1
}
```

- [ ] **Step 2: The Unity Catalog registry catalog and schema**

Append to `infra/terraform/unity_catalog.tf`:

```hcl
# The model registry (Phase 4, design doc §5.2): a UC catalog/schema
# distinct from the lake's external locations above -- this holds a
# managed table (the registered model), not a pointer at Delta files a
# job writes by path.
resource "databricks_catalog" "models" {
  name    = var.model_registry_catalog
  comment = "Trained models (Phase 4): mlflow.set_registry_uri(\"databricks-uc\") targets this."
}

resource "databricks_schema" "models" {
  catalog_name = databricks_catalog.models.name
  name         = var.model_registry_schema
  comment      = "pr_review_sla_risk lives here, aliased @champion."
}
```

- [ ] **Step 3: The training job**

Append to `infra/terraform/databricks.tf`:

```hcl
# Phase 4's training run (design doc §5.2): single-node model fit, so one
# worker is enough -- unlike the medallion jobs above, this is not a Spark
# compute-bound workload.
resource "databricks_job" "train_model" {
  name        = "${var.prefix}-train-model"
  description = "Phase 4: build the training frame, train, log to MLflow, register @champion. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "train"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      # Single-node: dataset.py's Spark reads are small and training itself
      # never distributes. `is_single_node = true` is the current (2026)
      # mechanism -- it sets num_workers/spark_conf/tags correctly on its
      # own; manually setting num_workers = 0 plus
      # spark.databricks.cluster.profile by hand is the old pattern and is
      # now rejected under some access modes. Confirmed live 2026-09-03.
      is_single_node     = true
      runtime_engine     = "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags
    }
  }

  task {
    task_key        = "train"
    job_cluster_key = "train"

    spark_python_task {
      python_file = var.model_python_file
      source      = "WORKSPACE"
      parameters = [
        "--silver-path", "${local.lake.silver}/events",
        "--features-path", "${local.lake.gold}/../features/events",
        "--gold-warehouse", "${local.lake.gold}/warehouse",
        "--tracking-uri", "databricks",
        "--experiment-name", "/Shared/almanac/pr-review-sla-risk",
        "--register",
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.model_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }
}

output "train_model_job_url" {
  description = "Databricks Workflows URL for Phase 4's training job."
  value       = databricks_job.train_model.url
}

# The live endpoint (design doc §8.1, §5.2): Databricks Model Serving's
# own REST API is the interface -- no custom service in front of it.
# scale_to_zero_enabled confirmed current on the served-model block
# against the provider's docs, 2026-09-03.
resource "databricks_model_serving" "pr_review_sla_risk" {
  name = "${var.prefix}-pr-review-sla-risk"

  config {
    served_entities {
      entity_name           = "${databricks_catalog.models.name}.${databricks_schema.models.name}.pr_review_sla_risk"
      entity_version        = "1"
      workload_size         = "Small"
      scale_to_zero_enabled = true
    }
  }

  tags {
    key   = "project"
    value = var.prefix
  }
}

output "model_serving_endpoint_url" {
  description = "Databricks Model Serving endpoint for pr_review_sla_risk."
  value       = databricks_model_serving.pr_review_sla_risk.serving_endpoint_id
}
```

The training job's `--features-path` composes a features container path
alongside the existing `local.lake.gold` map entry rather than adding a
fourth `local.lake` tier — `features` is provisioned since Phase 0 (design
doc §4.4a) but was never added to that map because Phase 3 ran locally
against fixtures, not this live path; fix the relative `../features`
composition to a proper fourth `local.lake.features` entry alongside
`bronze`/`silver`/`gold` in `databricks.tf`'s existing `locals` block
instead of leaving the relative-path workaround in place — this is a
one-line change to the existing `for tier in [...]` loop, not a new
resource.

- [ ] **Step 4: Fix the `local.lake` map to include `features`**

In `infra/terraform/databricks.tf`, change:

```hcl
locals {
  lake = {
    for tier in ["bronze", "silver", "gold"] :
    tier => "abfss://${tier}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  }
}
```

to:

```hcl
locals {
  lake = {
    for tier in ["bronze", "silver", "gold", "features"] :
    tier => "abfss://${tier}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  }
}
```

and change the training job's `--features-path` parameter to
`"${local.lake.features}/events"`, matching `--silver-path`'s shape
exactly rather than the relative-path composition above.

- [ ] **Step 5: `terraform fmt` and `terraform validate`**

Run: `cd infra/terraform && terraform fmt -check && terraform validate`
Expected: clean. (`terraform plan` is not run here — that requires the
live backend and is exercised for real in Task 9's cloud verification
step, not as this task's own gate.)

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/unity_catalog.tf infra/terraform/databricks.tf infra/terraform/variables.tf
git commit -m "feat: Phase 4 Task 8 -- Terraform for the training job and serving endpoint"
```

---

## Task 9: The real cloud verification step

Not a code task — the live run this plan's Global Constraints and design
doc §5.2 commit to doing now, against the still-live workspace, before
the credit expires. Recorded here so it is not skipped silently.

- [ ] **Step 1: Apply the new Terraform**

Run: `cd infra/terraform && terraform plan` (review the diff — a new
catalog, schema, training job, and serving endpoint, nothing destructive
to the existing medallion infrastructure), then `terraform apply`.

- [ ] **Step 2: Sync the wheel and scripts, run the training job**

Build and upload the wheel (`uv build`, then sync to
`/Workspace/Shared/almanac/dist/`, matching the existing Gold/backfill
deploy path), sync `scripts/model.py`, and start
`databricks_job.train_model` from the Databricks UI or
`databricks jobs run-now`.

- [ ] **Step 3: Confirm the MLflow experiment and UC registration**

In the workspace: confirm `/Shared/almanac/pr-review-sla-risk` has one
run with `model_mae`/`baseline_mae`/`beats_baseline` logged, and (if
`beats_baseline` was true) confirm
`almanac.models.pr_review_sla_risk` exists with a version aliased
`@champion` (`SHOW ALIASES ON MODEL almanac.models.pr_review_sla_risk` or
the Catalog UI). **If `beats_baseline` was false, this is a real,
documented null result** (§5.1) — record it in STATUS.md exactly as
measured, not retried until it flips.

- [ ] **Step 4: Invoke the serving endpoint for real, measure cold start and p50/p95**

Once `databricks_model_serving.pr_review_sla_risk` is `READY`, invoke it
directly (`curl` or `httpx` against its invocations URL) several times:
the first call after a cold endpoint measures cold start, a burst of
subsequent calls measures warm p50/p95. Record both, replacing §8.1's
community-sourced "10-20 seconds, occasionally minutes" figure with a
measured one — the same "verify before quoting" discipline §13 already
applies to everything else in the design doc.

- [ ] **Step 5: Record findings and cost**

Write `docs/findings/2026-09-0X-model-serving-measured.md` (mirroring
`2026-09-01-measured-dbus.md`'s shape): the measured cold start/p50/p95,
whether the model beat the baseline and by how much, and the dollar cost
of this verification window (Azure Cost Management or `system.billing`,
same source as the Phase 2 DBU findings). **Also measure and record the
SLA threshold** design doc §5.2 deferred to this step — a percentile
(e.g. p75) of `time_to_first_response_seconds` over the real trainable
population, computed once from the same training frame `build_training_
frame` produced. This number is not consumed by any code in this plan
(the threshold is applied by whatever later reads the endpoint's raw
predicted-seconds output, e.g. a future reporting layer) — recording it
here closes the "not assumed here" commitment in §5.2 without inventing
a consumer for it that doesn't exist yet.

- [ ] **Step 6: STATUS.md and commit**

Add a verification-log row stating what was actually run and its actual
result (the measured numbers, not estimates), following this file's
existing convention exactly.

```bash
git add docs/findings docs/STATUS.md
git commit -m "docs: Phase 4 cloud verification -- measured cold start, MLflow run, UC registration"
```

---

## Task 10: Wrap-up — exit gate, README, STATUS.md

**Files:**
- Modify: `README.md` (architecture diagram)
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the full suite**

Run: `make check`
Expected: ruff, ruff format --check, mypy --strict, and the full pytest
suite (`-m "not network"`) all green, including every test this plan
added.

- [ ] **Step 2: Update the README's Mermaid diagram**

Add an `ML[Model + MLflow<br/>LightGBM, UC registry]` node reading from
`F` (Features), parallel to the existing `R`/`E` nodes it replaces or
feeds — mark `ML` done, and mark `R` (MLflow registry) and `E` (Model
Serving) done per §9's Phase 4 gate, per CLAUDE.md's same-commit diagram
rule. Read the current diagram from the file before editing it (Phase 3's
own wrap-up found a wrong edge — `G --> F` instead of `S --> F` — that
had gone unnoticed since Task 3 specifically because it was edited from
memory instead of re-read).

- [ ] **Step 3: Update the README's status block**

Replace the "Phase 3 built the offline feature platform, still no model"
paragraph with what Phase 4 actually measured: whether the model beat the
baseline and by how much, the measured cold start/p50/p95, and the
dollar cost of the cloud verification window — real numbers from Task
9's findings doc, not estimates.

- [ ] **Step 4: Add STATUS.md's verification-log row for this plan's execution**

One row stating what actually ran (`make check`'s real test count) and
the actual result, following the same convention as every prior row.

- [ ] **Step 5: Update STATUS.md's "Next" section**

Point it at Phase 5 (embeddings + vector index, design doc §9), and
record any items this phase leaves open (e.g. if `beats_baseline` came
back false, that the model has not been iterated on beyond the v1
feature set — a real open item, not silently closed).

- [ ] **Step 6: Commit**

```bash
git add README.md docs/STATUS.md
git commit -m "docs: Phase 4 exit gate -- README diagram, STATUS.md verification row"
```

---

## Exit Gate

| Gate | How it's verified |
|---|---|
| `make check` green | Task 10, Step 1 — the whole suite |
| The label join only keeps rows with a defined outcome | `test_joins_the_label_and_drops_rows_with_no_defined_outcome` (Task 1) |
| The label attaches at a pinned Delta version, reproducibly | `test_pinning_every_version_reproduces_the_frame_after_a_later_label_update` (Task 2) |
| No model ships without beating a measured baseline | `train_model`'s `beats_baseline` gate + its two directional tests (Task 4); `run_training`'s conditional registration (Task 7) |
| Training is deterministic under a fixed seed | `test_the_same_seed_produces_the_same_result_twice` (Task 4) |
| UC registration uses the current, non-deprecated alias mechanism | `register_champion` (Task 6), confirmed live against Databricks' docs 2026-09-03 |
| The CLI has no mocking anywhere | `test_model_runner_cli.py` (pure `argparse.Namespace`), `test_model_runner.py` (real Spark + real local MLflow) |
| Serving is Databricks Model Serving's own endpoint, no custom API | `databricks_model_serving.pr_review_sla_risk` (Task 8); no FastAPI/HTTP service added anywhere in this plan |
| The real cloud step ran before the credit deadline | Task 9, dated in STATUS.md against the Sep 24 expiry |
| README / STATUS.md updated same-commit | Task 10 |

## Deferred out of Phase 4, on purpose

- **The secondary bot-classifier upgrade** (§5's "measure the heuristic,
  train, measure lift" arc) — real and cheap, but its own label/feature
  set; kept independent so this plan stays about one pipeline end to end.
- **The `@challenger` retraining workflow** — v1 registers one model
  version; a real champion/challenger comparison needs a second trained
  candidate, which belongs to a future retraining story.
- **`GET /features/{id}?as_of=` and `POST /similar-prs`** as custom API
  endpoints — the first is feature-retrieval, not model-serving; the
  second needs Phase 5's vector index, which does not exist yet.
- **Drift and training/serving-skew monitoring beyond what's measurable
  from the model's own logged predictions** against a later feature
  re-pull — a full monitoring pipeline is not built here.
- **Incremental retraining / a scheduled retraining job** — Task 9 runs
  the training job once, by hand, matching the "attended runs only"
  convention every other Databricks job in this project already uses.
- **Tearing down the serving endpoint immediately after measurement** —
  `scale_to_zero_enabled` keeps idle cost near zero, so the endpoint is
  left live rather than destroyed, consistent with the already-carried-
  forward Phase 2/3 `terraform destroy` open item; a final teardown
  decision is made once, at the point this project's real cloud demos are
  done, not piecemeal per phase.
