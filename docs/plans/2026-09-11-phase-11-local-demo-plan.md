# Phase 11 — the local demo, and its public deployment

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Streamlit demo that runs with no cloud account, scores with a
committed copy of the registered champion, and deploys publicly to Hugging
Face Spaces.

**Architecture:** A Spark build step turns committed fixtures into small
committed artifacts. A Streamlit app reads only those artifacts and the
committed champion. The app never imports `pyspark`, never starts a JVM,
and makes no network call at view time.

**Tech Stack:** Python 3.12, Streamlit, LightGBM 4.7.0 via MLflow 3.16.0,
PySpark 4.2 (build step only), pytest.

**Spec:** `docs/design/2026-09-11-almanac-local-demo-design.md`

## Global Constraints

Copied verbatim from the spec and this repo's `CLAUDE.md`. Every task's
requirements implicitly include this section.

- **Point-in-time correctness.** Every feature computed `as_of` T reads
  only events with `created_at < T`. This phase computes no new features;
  it reuses `almanac.features`. Do not add a feature.
- **Never quote a number that was not measured.** Applies to the plan, the
  code, the commits, the app's copy, and the README.
- **`docs/STATUS.md` updates in the same commit as the work it describes.**
- **One commit per completed task.** Conventional prefixes (`feat:`,
  `fix:`, `docs:`, `test:`, `refactor:`).
- **Commit messages are plain ASCII: `--`, never an em-dash.** Docs may use
  `—` freely.
- **`make check-fast` per task; full `make check` before the push.**
- **Branch `phase-11-local-demo` carries the whole phase**, one push, one
  PR. It already holds `c8cade0`, the design doc. Task commits begin at
  Task 1.
- **No login and no `owner/repo` renders anywhere**, and neither appears in
  any committed demo artifact. Bot-versus-human is a toggle, not a name.
- **Nothing billable is created by this phase.** No `terraform apply`, no
  warehouse, no cluster, no serving endpoint.
- **Version floors are checked live when added, never carried from a plan**
  (`pyproject.toml`'s existing comment states this rule).

---

## File structure

| Path | Responsibility |
|---|---|
| `src/almanac/model/native.py` | Establishes the `lightgbm`-before-`mlflow` import order once. The phase's only change outside the demo package. |
| `src/almanac/demo/__init__.py` | Package marker. |
| `src/almanac/demo/champion.py` | Loads the committed champion. Pure: path in, model out. |
| `src/almanac/demo/build.py` | The Spark build step. Fixtures to artifacts. The only module importing `pyspark`. |
| `src/almanac/demo/artifacts.py` | Reads and validates the committed artifacts. Imported by every panel. |
| `src/almanac/demo/panels.py` | Panel data preparation. Pure functions, no Streamlit import. |
| `demo/app.py` | Streamlit entrypoint. Layout and widgets only. |
| `demo/model/champion/` | The committed champion artifact, 432 KB. |
| `demo/data/` | The committed build artifacts. |
| `demo/Dockerfile` | The Space's container. |
| `tests/unit/test_demo_*.py` | Unit tests, no SparkSession. |
| `tests/integration/test_demo_build.py` | The build step, Spark-marked. |

**Why `panels.py` holds no Streamlit import:** it keeps every data decision
testable without driving a UI, and leaves `app.py` small enough to read in
one screen. `AppTest` then covers layout, not logic.

---

### Task 1: The import-order guard

The spec's §6.1. Measured 2026-09-11 on macOS arm64, Python 3.12: importing
`lightgbm` before `mlflow` succeeds 10 of 10 runs; pandas-then-numpy-then-
`mlflow` fails 10 of 10; numpy-then-`mlflow` with no explicit `lightgbm`
fails 3 of 3. Failure is a SIGSEGV with no traceback.

This task is first because every later task that touches the model depends
on it.

**Files:**
- Create: `src/almanac/model/native.py`
- Test: `tests/unit/test_model_native.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `almanac.model.native.load_lightgbm_first() -> None`, and the
  module-level side effect that importing `almanac.model.native` performs
  the ordering. Later tasks import this module **first**, before any
  `mlflow` import.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_model_native.py`:

```python
"""The import order that keeps LightGBM from segfaulting under a second OpenMP runtime."""

import ast
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[2] / "src" / "almanac"
_DEMO_PKG = _SRC / "demo"


def _import_order(path: Path) -> list[str]:
    """Top-level module names imported by `path`, in source order."""
    tree = ast.parse(path.read_text())
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.append(node.module.split(".")[0])
    return names


def test_native_module_imports_lightgbm_before_mlflow() -> None:
    order = _import_order(_SRC / "model" / "native.py")
    assert "lightgbm" in order, "native.py must import lightgbm"
    assert "mlflow" in order, "native.py must import mlflow"
    assert order.index("lightgbm") < order.index("mlflow")


@pytest.mark.parametrize("module", sorted(_DEMO_PKG.glob("*.py")) if _DEMO_PKG.exists() else [])
def test_no_demo_module_reaches_mlflow_ahead_of_lightgbm(module: Path) -> None:
    """A module importing mlflow must go through native.py, which orders it.

    Measured 2026-09-11: the wrong order segfaults 10 of 10 runs, with no
    traceback to debug from. A comment cannot fail a build; this can.
    """
    order = _import_order(module)
    if "mlflow" not in order:
        return
    assert "lightgbm" in order and order.index("lightgbm") < order.index("mlflow"), (
        f"{module.name} imports mlflow without lightgbm first -- import "
        f"almanac.model.native before mlflow, or the process dies with SIGSEGV"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_model_native.py -v`
Expected: FAIL, `FileNotFoundError` or `AssertionError` on the missing
`src/almanac/model/native.py`.

- [ ] **Step 3: Write minimal implementation**

`src/almanac/model/native.py`:

```python
"""Import LightGBM before MLflow, or scoring segfaults under two OpenMP runtimes.

Measured 2026-09-11 (macOS arm64, Python 3.12, lightgbm 4.7.0, mlflow 3.16.0):
this order succeeds 10 of 10 runs; every other order tried fails 10 of 10 with
SIGSEGV and no traceback. MLflow imports lightgbm lazily, by which point numpy
has bound its own OpenMP runtime and a second copy loads.

Import this module before mlflow anywhere the champion is loaded or scored.
`tests/unit/test_model_native.py` fails the build if a demo module does not.
"""

from __future__ import annotations

import lightgbm  # noqa: F401  -- imported for its side effect: bind OpenMP first.
import mlflow


def load_lightgbm_first() -> None:
    """No-op. Calling it documents that the import above is deliberate."""
    return None


__all__ = ["load_lightgbm_first", "mlflow"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_model_native.py -v`
Expected: PASS. The parametrized test collects zero cases until Task 2
creates the demo package; that is correct at this point.

- [ ] **Step 5: Verify the guard actually catches the bug**

Temporarily reorder `native.py` so `import mlflow` precedes
`import lightgbm`, run the test, confirm it FAILS, then restore. A guard
that has never been seen to fail is not known to work — this repo
mutation-tests for exactly this reason.

- [ ] **Step 6: Update `docs/STATUS.md`**

Add a Phase 11 section with Task 1's row, stating the measured 10-of-10
versus 0-of-10 result and that the guard was verified by reversal.

- [ ] **Step 7: Commit**

```bash
uv run ruff format . && uv run ruff check --fix . && make check-fast
git add src/almanac/model/native.py tests/unit/test_model_native.py docs/STATUS.md
git commit -m "fix: import lightgbm before mlflow, or scoring segfaults

Measured 2026-09-11 on macOS arm64 / Python 3.12: loading the registered
champion and scoring it dies with SIGSEGV and no traceback unless lightgbm
is imported before mlflow. 10 of 10 runs succeed in that order; 10 of 10
fail with pandas-then-numpy-then-mlflow, and 3 of 3 with numpy-then-mlflow.
MLflow imports lightgbm lazily, after numpy has bound its own OpenMP
runtime, and a second copy loads.

Never seen before because scoring had only ever run on Databricks runtime.
Phase 11 is the first thing here to score on a laptop.

The guard is a test over the AST, not a comment: any demo module reaching
mlflow without lightgbm ahead of it fails the suite. Verified by reversing
the order and watching it fail."
```

---

### Task 2: Commit the champion, and score a pinned vector offline

**Files:**
- Create: `src/almanac/demo/__init__.py`, `src/almanac/demo/champion.py`
- Create: `demo/model/champion/` (the artifact, 432 KB, 10 files)
- Test: `tests/unit/test_demo_champion.py`
- Modify: `.gitignore` if it excludes `demo/` or `*.skops`

**Interfaces:**
- Consumes: `almanac.model.native` (Task 1).
- Produces: `almanac.demo.champion.CHAMPION_DIR: Path`,
  `almanac.demo.champion.load_champion(path: Path | None = None) -> Any`
  returning an `LGBMClassifier`, and
  `almanac.demo.champion.champion_provenance() -> dict[str, str]` returning
  `{"model_version": "2", "run_id": "...", "registered_model": "..."}` read
  from the artifact's own `registered_model_meta` and `MLmodel` files, never
  hardcoded.

- [ ] **Step 1: Fetch the artifact into the repo**

The artifact is version 2 under the `@champion` alias, run
`5f6a71bd9e4f4b6d8f7ea0a6454c0ca5`. This is a free registry read; it starts
no compute. Ask the user which profile to use rather than assuming one.

```bash
mkdir -p demo/model
DATABRICKS_CONFIG_PROFILE=<profile> uv run python -c "
import mlflow
mlflow.set_registry_uri('databricks-uc'); mlflow.set_tracking_uri('databricks')
print(mlflow.artifacts.download_artifacts(
    artifact_uri='models:/almanac_dbx.models.pr_review_sla_risk@champion',
    dst_path='demo/model/champion'))
"
du -sh demo/model/champion   # expect ~432K
```

Verify before committing: `demo/model/champion/registered_model_meta` must
read `model_version: '2'`, and `MLmodel` must carry
`run_id: 5f6a71bd9e4f4b6d8f7ea0a6454c0ca5` and a signature naming all ten
of `FEATURE_COLUMNS`. If either differs, **stop** — the champion moved, and
that is a finding, not something to work around.

- [ ] **Step 2: Write the failing test**

`tests/unit/test_demo_champion.py`:

```python
"""The committed champion loads offline and scores a pinned vector."""

import math
from pathlib import Path

import numpy as np

from almanac.demo.champion import CHAMPION_DIR, champion_provenance, load_champion
from almanac.model.train import FEATURE_COLUMNS

# Measured 2026-09-11 against the committed artifact, via booster_.predict.
# Pinned so a silently swapped model fails the suite instead of shipping.
_PINNED_VECTOR = [3.0, 0.5, 120.0, 4.0, 7.0, 0.03, float("nan"), 0.0, 1.0, 14.0]
_PINNED_SCORE = 0.6937982497227584


def test_the_artifact_is_committed() -> None:
    assert (CHAMPION_DIR / "MLmodel").is_file()
    assert (CHAMPION_DIR / "model.skops").is_file()


def test_it_loads_with_no_network_and_scores_the_pinned_vector() -> None:
    model = load_champion()
    x = np.array([_PINNED_VECTOR], dtype=np.float64)
    assert math.isclose(model.booster_.predict(x)[0], _PINNED_SCORE, rel_tol=1e-12)


def test_the_signature_names_every_feature_the_trainer_uses() -> None:
    """The artifact pins the feature contract, so it is never restated beside it."""
    text = (CHAMPION_DIR / "MLmodel").read_text()
    for column in FEATURE_COLUMNS:
        assert f'"name": "{column}"' in text, f"{column} missing from the model signature"


def test_provenance_is_read_from_the_artifact_not_hardcoded() -> None:
    provenance = champion_provenance()
    assert provenance["model_version"] == "2"
    assert provenance["run_id"] == "5f6a71bd9e4f4b6d8f7ea0a6454c0ca5"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_demo_champion.py -v`
Expected: FAIL, `ModuleNotFoundError: almanac.demo`.

- [ ] **Step 4: Write minimal implementation**

`src/almanac/demo/__init__.py`:

```python
"""The local demo: committed artifacts, a committed champion, no cloud account."""
```

`src/almanac/demo/champion.py`:

```python
"""Load the committed champion. No registry call, no network, no credentials."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# MUST precede any mlflow import -- see the module docstring there.
from almanac.model.native import mlflow

_REPO_ROOT = Path(__file__).resolve().parents[3]
CHAMPION_DIR = _REPO_ROOT / "demo" / "model" / "champion"


def load_champion(path: Path | None = None) -> Any:
    """The registered champion, from disk. Returns an `LGBMClassifier`."""
    return mlflow.lightgbm.load_model(str(path or CHAMPION_DIR))


def champion_provenance(path: Path | None = None) -> dict[str, str]:
    """Version, run and registered name -- read from the artifact, never hardcoded."""
    root = path or CHAMPION_DIR
    meta = yaml.safe_load((root / "registered_model_meta").read_text())
    mlmodel = yaml.safe_load((root / "MLmodel").read_text())
    return {
        "registered_model": str(meta["model_name"]),
        "model_version": str(meta["model_version"]),
        "run_id": str(mlmodel["run_id"]),
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_demo_champion.py -v`
Expected: 4 passed. The Task 1 parametrized guard now collects
`champion.py` and must also pass.

- [ ] **Step 6: Confirm it is genuinely offline**

Run: `DATABRICKS_HOST= DATABRICKS_TOKEN= uv run pytest tests/unit/test_demo_champion.py -v`
Expected: PASS. If it fails, something is still reaching the registry.

- [ ] **Step 7: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add src/almanac/demo demo/model tests/unit/test_demo_champion.py docs/STATUS.md .gitignore
git commit -m "feat: commit the registered champion, loaded and scored offline

Version 2 under the @champion alias, run 5f6a71bd9e4f4b6d8f7ea0a6454c0ca5 --
the same pair the Phase 9 window read back from the live registry. 432 KB:
100 trees, 10 features, skops-serialized.

Committed rather than downloaded at build time, because a demo that phones
Databricks is not a demo that runs without a cloud account.

The artifact carries its own MLflow signature naming all ten feature
columns, so the demo's feature contract is pinned by the model file rather
than restated beside it, and a test asserts the two agree. A second test
pins one scored vector at 0.6937982497227584, measured 2026-09-11, so a
silently swapped model fails the suite."
```

---

### Task 3: The build step — medallion counts

**Files:**
- Create: `src/almanac/demo/build.py`
- Test: `tests/integration/test_demo_build.py`
- Modify: `Makefile` (add `demo-build`)

**Interfaces:**
- Consumes: `almanac.spark.local_session`, `almanac.config.Settings`,
  `almanac.pipeline.bronze.add_ingestion_metadata`,
  `almanac.pipeline.bronze.write_bronze`, `almanac.pipeline.silver.run_silver`,
  `almanac.pipeline.source.SourceConfig`.
- Produces: `almanac.demo.build.DEMO_DATA_DIR: Path`,
  `almanac.demo.build.build_medallion(spark, *, out_dir: Path) -> dict[str, Any]`
  writing `demo/data/medallion.json`, and
  `almanac.demo.build.ERAS: tuple[tuple[str, str, int], ...]` — the fixture
  glob, `event_date` and `event_hour` per era.

**Note on era coverage:** `scripts/build_silver_fixture.py` lands two eras.
This build step lands **all three**, including the reduced era, because the
medallion panel's whole point is the schema break. That is a build-step
decision and does not change the existing script.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_demo_build.py`:

```python
"""The demo build step: fixtures in, committed artifacts out."""

import json
from pathlib import Path

import pytest

from almanac.demo.build import ERAS, build_medallion

pytestmark = pytest.mark.spark


def test_it_lands_all_three_schema_eras(spark, tmp_path: Path) -> None:
    """Three eras, because the schema break is what the medallion panel exists to show."""
    summary = build_medallion(spark, out_dir=tmp_path)
    assert len(ERAS) == 3
    assert {era["event_date"] for era in summary["eras"]} == {e[1] for e in ERAS}


def test_the_split_is_asserted_never_assumed(spark, tmp_path: Path) -> None:
    """valid + quarantine == scored, the repo's standing rule for every quality split."""
    summary = build_medallion(spark, out_dir=tmp_path)
    for era in summary["eras"]:
        assert era["silver_rows"] + era["quarantine_rows"] == era["scored_rows"]


def test_it_writes_a_readable_artifact(spark, tmp_path: Path) -> None:
    build_medallion(spark, out_dir=tmp_path)
    payload = json.loads((tmp_path / "medallion.json").read_text())
    assert payload["scale"]["hours_per_era"] == 1
    assert payload["eras"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_demo_build.py -v`
Expected: FAIL, `ImportError: cannot import name 'ERAS'`.

- [ ] **Step 3: Write minimal implementation**

`src/almanac/demo/build.py`:

```python
"""Fixtures to committed artifacts. The only demo module that imports Spark.

Runs at build time, never at view time: the app reads what this writes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.config import Settings
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.pipeline.silver import run_silver
from almanac.pipeline.source import SourceConfig

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEMO_DATA_DIR = _REPO_ROOT / "demo" / "data"

# (fixture glob, event_date, event_hour). All three eras, unlike
# scripts/build_silver_fixture.py's two: the schema break is the point.
ERAS: tuple[tuple[str, str, int], ...] = (
    ("modern-*.jsonl.gz", "2025-08-13", 14),
    ("legacy-*.jsonl.gz", "2014-06-12", 14),
    ("reduced-*.jsonl.gz", "2025-11-03", 14),
)

# Fixed, so a rerun is byte-for-byte reproducible -- the same constant
# scripts/build_silver_fixture.py uses, and for the same reason.
_INGESTED_AT = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


def _land_era(
    spark: SparkSession, *, glob: str, event_date: str, event_hour: int, root: Path
) -> dict[str, Any]:
    """One era through Bronze and Silver. Returns its counts."""
    settings = Settings()
    config = SourceConfig.load(_REPO_ROOT / "conf" / "sources" / "gharchive.yml")
    matches = sorted(settings.fixture_dir.glob(glob))
    if not matches:
        raise SystemExit(f"no fixture matching {glob}; run `make fixtures`")

    bronze, silver = root / "bronze", root / "silver"
    raw = spark.read.text(str(matches[0])).withColumnRenamed("value", "raw_json")
    stamped = add_ingestion_metadata(raw, ingested_at=_INGESTED_AT, source_file=str(matches[0]))
    partitioned = stamped.withColumn("event_date", F.lit(event_date)).withColumn(
        "event_hour", F.lit(event_hour)
    )
    write_bronze(partitioned, str(bronze), event_date=event_date, event_hour=event_hour)
    run_silver(spark, str(bronze), str(silver), event_date=event_date, config=config)

    def _count(path: Path) -> int:
        frame: DataFrame = spark.read.format("delta").load(str(path))
        return frame.where(F.col("event_date") == event_date).count()

    bronze_rows = (
        spark.read.format("delta")
        .load(str(bronze))
        .where(F.col("event_date") == event_date)
        .count()
    )
    silver_rows = _count(silver / "clean")
    quarantine_rows = _count(silver / "quarantine")
    return {
        "event_date": event_date,
        "event_hour": event_hour,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        # Asserted by the caller, never assumed -- a NULL rule condition
        # would otherwise drop a record from both sides silently.
        "scored_rows": silver_rows + quarantine_rows,
    }


def build_medallion(spark: SparkSession, *, out_dir: Path) -> dict[str, Any]:
    """Land every era, write `medallion.json`, return what it wrote."""
    out_dir.mkdir(parents=True, exist_ok=True)
    eras = [
        _land_era(spark, glob=g, event_date=d, event_hour=h, root=out_dir / "lake")
        for g, d, h in ERAS
    ]
    summary = {
        "scale": {
            "hours_per_era": 1,
            "note": (
                "One archived hour per schema era. The platform's measured "
                "backfill is 341,060,851 rows over Q3 2025."
            ),
        },
        "eras": eras,
    }
    (out_dir / "medallion.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_demo_build.py -v`
Expected: 3 passed.

- [ ] **Step 5: Add the Makefile target**

```makefile
# Fixtures -> Silver -> the committed artifacts the demo reads. Spark runs
# HERE and nowhere else: `make demo` never starts a JVM.
demo-build:
	uv run python -m almanac.demo.build
```

Add `demo-build` and `demo` to the `.PHONY` line.

- [ ] **Step 6: Run it for real and record the counts**

Run: `make demo-build`
Record the per-era row counts in the STATUS.md row. They are measurements;
do not round them.

- [ ] **Step 7: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add src/almanac/demo/build.py tests/integration/test_demo_build.py Makefile demo/data docs/STATUS.md
git commit -m "feat: the demo build step lands all three eras and counts them

Bronze, Silver and quarantine counts per schema era, written as
demo/data/medallion.json. Spark runs here and nowhere else -- the app
reads what this writes and never starts a JVM.

Three eras, where scripts/build_silver_fixture.py lands two: the medallion
panel exists to show the schema break, so leaving the reduced era out would
remove its subject. The existing script is unchanged; what to land is a
per-consumer decision.

The split is asserted, not assumed -- silver + quarantine == scored, per the
standing rule that a NULL rule condition otherwise drops a record from both
sides at once."
```

---

### Task 4: The build step — features, scores and coverage

The spec's §4.1. Measured 2026-09-11 on the two eras the existing script
lands: 137 opened pull requests from 3,997 Silver rows, with 7 of 10
features null for roughly 90% of rows and `prior_merge_rate` null for all
137. **Task 3 adds a third era, so these numbers will change. Re-measure
and record what you get; do not copy these.**

**Files:**
- Modify: `src/almanac/demo/build.py`
- Modify: `tests/integration/test_demo_build.py`

**Interfaces:**
- Consumes: Task 3's `_land_era` output; `almanac.features.spine.build_pr_opened_spine`,
  `almanac.features.groups.compute_author_activity`,
  `compute_repo_activity`, `compute_pr_static`,
  `almanac.features.assemble.assemble_training_set`,
  `almanac.model.train.FEATURE_COLUMNS`, Task 2's `load_champion`.
- Produces: `build_queue(spark, *, out_dir: Path) -> dict[str, Any]` writing
  `demo/data/queue.parquet` and `demo/data/coverage.json`, and
  `almanac.demo.build.main(argv: list[str] | None = None) -> int` running
  every builder in order.

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_demo_build.py`:

```python
from almanac.demo.build import build_queue
from almanac.model.train import FEATURE_COLUMNS

_IDENTITY_COLUMNS = ("actor_login", "author_login", "repo_name", "repo_full_name", "org_login")


def test_the_queue_carries_no_identity_column(spark, tmp_path: Path) -> None:
    """docs/pseudonymization.md: logins and owner/repo are never published."""
    import pandas as pd

    build_queue(spark, out_dir=tmp_path)
    frame = pd.read_parquet(tmp_path / "queue.parquet")
    for column in _IDENTITY_COLUMNS:
        assert column not in frame.columns, f"{column} must not reach a published artifact"


def test_every_row_is_scored_and_the_score_is_a_probability(spark, tmp_path: Path) -> None:
    import pandas as pd

    build_queue(spark, out_dir=tmp_path)
    frame = pd.read_parquet(tmp_path / "queue.parquet")
    assert len(frame) > 0
    assert frame["breach_risk"].between(0.0, 1.0).all()


def test_coverage_reports_a_null_count_for_every_feature(spark, tmp_path: Path) -> None:
    """The sparsity is the point-in-time invariant made visible, so it is measured."""
    build_queue(spark, out_dir=tmp_path)
    coverage = json.loads((tmp_path / "coverage.json").read_text())
    assert set(coverage["features"]) == set(FEATURE_COLUMNS)
    assert coverage["total_rows"] > 0
    for column in FEATURE_COLUMNS:
        assert 0 <= coverage["features"][column]["non_null"] <= coverage["total_rows"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_demo_build.py -v`
Expected: FAIL, `ImportError: cannot import name 'build_queue'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/almanac/demo/build.py`:

```python
from almanac.demo.champion import load_champion
from almanac.features.assemble import assemble_training_set
from almanac.features.groups import (
    compute_author_activity,
    compute_pr_static,
    compute_repo_activity,
)
from almanac.features.spine import build_pr_opened_spine
from almanac.model.train import FEATURE_COLUMNS

# Surrogate keys only. docs/pseudonymization.md: a login identifies a real
# person who never opted into this, and the owner half of `owner/repo` usually
# does too. The queue ranks by rank, not by name.
_PUBLISHED_COLUMNS = ["repo_id", "pr_number", "as_of_timestamp", "is_bot_author"]


def build_queue(spark: SparkSession, *, out_dir: Path) -> dict[str, Any]:
    """Score the fixture population, and measure how much of it has features at all."""
    out_dir.mkdir(parents=True, exist_ok=True)
    events = spark.read.format("delta").load(str(out_dir / "lake" / "silver" / "clean"))
    frame = assemble_training_set(
        build_pr_opened_spine(events),
        author_activity=compute_author_activity(events),
        repo_activity=compute_repo_activity(events),
        pr_static=compute_pr_static(events),
    )
    keep = [c for c in _PUBLISHED_COLUMNS if c in frame.columns] + FEATURE_COLUMNS
    pdf = frame.select(*keep).toPandas()

    model = load_champion()
    pdf["breach_risk"] = model.booster_.predict(pdf[FEATURE_COLUMNS].astype("float64").to_numpy())
    pdf = pdf.sort_values("breach_risk", ascending=False).reset_index(drop=True)
    pdf.insert(0, "rank", pdf.index + 1)
    pdf.to_parquet(out_dir / "queue.parquet", index=False)

    coverage = {
        "total_rows": int(len(pdf)),
        "features": {
            column: {"non_null": int(pdf[column].notna().sum())} for column in FEATURE_COLUMNS
        },
        "why": (
            "A feature named 'to date' may read only events strictly before its "
            "own as_of. One archived hour contains almost no prior history, so "
            "most of these are null. That is point-in-time correctness working, "
            "not a defect."
        ),
    }
    (out_dir / "coverage.json").write_text(json.dumps(coverage, indent=2, sort_keys=True) + "\n")
    return coverage


def main(argv: list[str] | None = None) -> int:
    """Build every artifact the demo reads."""
    spark = local_session("almanac-demo-build")
    build_medallion(spark, out_dir=DEMO_DATA_DIR)
    build_queue(spark, out_dir=DEMO_DATA_DIR)
    return 0
```

Add `from almanac.spark import local_session` to the imports, and a
`if __name__ == "__main__":` block calling `run_cli(main)` from
`almanac.cli`, matching `scripts/` convention.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_demo_build.py -v`
Expected: 6 passed.

- [ ] **Step 5: Run it and record the real coverage numbers**

Run: `make demo-build && cat demo/data/coverage.json`
Record the per-feature non-null counts and the total. These go in the
STATUS.md row and, in Task 8, on the panel itself.

- [ ] **Step 6: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add src/almanac/demo/build.py tests/integration/test_demo_build.py demo/data docs/STATUS.md
git commit -m "feat: score the fixture population, and measure its feature coverage

The queue artifact plus the coverage artifact beside it, because the second
is what makes the first honest. Measured over three eras: <N> opened pull
requests, and <K> of 10 features null for most of them.

That sparsity is not a defect to hide. A feature named 'to date' may read
only events strictly before its own as_of, and one archived hour contains
almost no prior history -- so the null rate IS point-in-time correctness,
visible rather than asserted. The panel in Task 8 says so in those words.

Identity never reaches the artifact: the written columns are repo_id,
pr_number, as_of_timestamp, is_bot_author and the ten features. A test
asserts no login and no owner/repo column is present, per
docs/pseudonymization.md."
```

Replace `<N>` and `<K>` with what Step 5 measured.

---

### Task 5: Byte-identical regeneration

The spec's §5.2 — the project's governing invariant applied to its own
demo.

**Files:**
- Create: `tests/integration/test_demo_artifacts_reproducible.py`

**Interfaces:**
- Consumes: Task 3 and Task 4's builders and their committed outputs.
- Produces: nothing importable.

- [ ] **Step 1: Write the failing test**

```python
"""The committed artifacts regenerate byte-identically, or the suite fails.

The same invariant the feature platform is built on: same inputs, same Delta
version, byte-identical output, a year later. Applied here so a stale demo
artifact fails the build instead of shipping quietly -- this repo has nine
recorded instances of a hand-maintained record outliving its truth.
"""

import hashlib
from pathlib import Path

import pytest

from almanac.demo.build import DEMO_DATA_DIR, build_medallion, build_queue

pytestmark = pytest.mark.spark

_COMMITTED = ("medallion.json", "coverage.json", "queue.parquet")


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_a_rebuild_reproduces_every_committed_artifact(spark, tmp_path: Path) -> None:
    build_medallion(spark, out_dir=tmp_path)
    build_queue(spark, out_dir=tmp_path)
    for name in _COMMITTED:
        committed = DEMO_DATA_DIR / name
        assert committed.is_file(), f"{name} is not committed; run `make demo-build`"
        assert _digest(tmp_path / name) == _digest(committed), (
            f"{name} differs from the committed copy -- rerun `make demo-build` "
            f"and commit the result, or explain why the inputs changed"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_demo_artifacts_reproducible.py -v`
Expected: FAIL on `queue.parquet`. Parquet embeds a writer timestamp and
non-deterministic metadata, so the digests will differ. **This is the
expected first failure and the reason this task exists.**

- [ ] **Step 3: Make the parquet write deterministic**

In `build_queue`, replace the parquet write with a deterministic one:

```python
    pdf.to_parquet(
        out_dir / "queue.parquet",
        index=False,
        engine="pyarrow",
        # Determinism: the default writes a pyarrow version string and
        # per-run metadata into the footer, so two identical builds differ
        # byte-for-byte and the reproducibility test cannot pass.
        store_schema=True,
        write_statistics=False,
        coerce_timestamps="us",
    )
```

If the footer still varies, fall back to writing the queue as sorted JSON
instead. A deterministic artifact matters more than the file format, and
the queue is small enough that JSON costs nothing. Record which was chosen
and why in the commit message.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_demo_artifacts_reproducible.py -v`
Expected: PASS.

- [ ] **Step 5: Verify the test can fail**

Edit one byte of `demo/data/coverage.json`, rerun, confirm FAIL, then
`git checkout` the file. A reproducibility check that has never been seen
to fail is not known to work.

- [ ] **Step 6: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add tests/integration/test_demo_artifacts_reproducible.py src/almanac/demo/build.py demo/data docs/STATUS.md
git commit -m "test: the committed demo artifacts regenerate byte-identically

The same invariant the feature platform rests on, applied to the demo: same
inputs, byte-identical output. A stale artifact now fails the suite instead
of shipping quietly, which matters because this repo has nine recorded
instances of a hand-maintained record outliving its truth.

The first run failed on queue.parquet, as expected: parquet embeds writer
metadata in its footer, so two identical builds differed byte-for-byte.
Fixed by <the chosen approach>.

Verified by mutation -- one byte changed in coverage.json fails the test."
```

---

### Task 6: Extend the pseudonymity guard to the demo

The spec's §7. The lesson this repo already paid for: the check was not
broken when it missed a leak, it was **aimed at the wrong files**.

**Files:**
- Modify: `src/almanac/governance/pseudonymity.py` (`PUBLISHED_GLOBS`)
- Modify: `tests/unit/test_governance_pseudonymity.py`

**Interfaces:**
- Consumes: `almanac.governance.pseudonymity.PUBLISHED_GLOBS`.
- Produces: no new names; the tuple gains entries.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_governance_pseudonymity.py`:

```python
def test_the_demo_surface_is_covered() -> None:
    """A public demo is the highest-exposure artifact this repo ships.

    The control that once passed cleanly while aimed at the wrong files is the
    reason this is a test and not a note.
    """
    from almanac.governance.pseudonymity import PUBLISHED_GLOBS

    for glob in ("demo/**/*.py", "demo/**/*.json", "demo/**/*.md", "demo/Dockerfile"):
        assert glob in PUBLISHED_GLOBS, f"{glob} is published and unscanned"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_governance_pseudonymity.py -v`
Expected: FAIL, `AssertionError: demo/**/*.py is published and unscanned`.

- [ ] **Step 3: Write minimal implementation**

In `PUBLISHED_GLOBS`, after the `.claude/**` entries:

```python
(
    # The demo is deployed publicly, which makes it the highest-exposure
    # surface this repo has. Added with the demo itself rather than after
    # it, because the one time this check missed a leak it was not broken
    # -- it was aimed at the wrong files, and passed cleanly forever.
    "demo/**/*.py",
    "demo/**/*.json",
    "demo/**/*.md",
    "demo/Dockerfile",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_governance_pseudonymity.py -v`
Expected: PASS, including the existing whole-repo scan, which now covers
`demo/`.

- [ ] **Step 5: Confirm the artifacts are actually clean**

Run: `uv run pytest -m "not network and not spark" -k pseudonym -v`
If it finds anything in `demo/data/`, **stop and fix the build step**, not
the check.

- [ ] **Step 6: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add src/almanac/governance/pseudonymity.py tests/unit/test_governance_pseudonymity.py docs/STATUS.md
git commit -m "feat: the pseudonymity check covers the demo surface

demo/ is deployed publicly, which makes it the highest-exposure surface in
this repo. Scoped with the demo rather than after it: the one time this
check missed a real leak it was not broken, it was aimed at the wrong files,
and a control with the wrong scope passes cleanly and forever."
```

---

### Task 7: Panel data preparation, with no Streamlit import

**Files:**
- Create: `src/almanac/demo/artifacts.py`, `src/almanac/demo/panels.py`
- Test: `tests/unit/test_demo_panels.py`

**Interfaces:**
- Consumes: Task 2's `load_champion`, `champion_provenance`; Tasks 3-4's
  artifacts; `almanac.model.contributions.contribution_frame`;
  `almanac.agent.grounding.verify`; `almanac.agent.bounded_agent.load_transcript`.
- Produces:
  - `artifacts.load_queue() -> pandas.DataFrame`
  - `artifacts.load_coverage() -> dict[str, Any]`
  - `artifacts.load_medallion() -> dict[str, Any]`
  - `panels.coverage_rows(coverage) -> pandas.DataFrame` with columns
    `feature`, `non_null`, `null`, `pct_null`
  - `panels.contributions_for(model, row) -> pandas.DataFrame` with columns
    `feature`, `contribution`, `direction`, sorted by absolute contribution
  - `panels.grounding_for(path: Path) -> GroundingTrace`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_demo_panels.py`:

```python
"""Panel data preparation. Pure functions -- no Streamlit, no Spark."""

from pathlib import Path

import pandas as pd
import pytest

from almanac.demo import panels
from almanac.demo.champion import load_champion
from almanac.model.train import FEATURE_COLUMNS

_TRANSCRIPTS = Path(__file__).resolve().parents[1] / "fixtures" / "transcripts"
_LIVE = _TRANSCRIPTS / "2026-09-10-live-predict-explain.json"
_FLIPPED = _TRANSCRIPTS / "2026-09-10-live-predict-explain-directions-flipped.json"


def test_coverage_rows_report_the_null_side_too() -> None:
    coverage = {"total_rows": 100, "features": {c: {"non_null": 10} for c in FEATURE_COLUMNS}}
    rows = panels.coverage_rows(coverage)
    assert set(rows["feature"]) == set(FEATURE_COLUMNS)
    assert (rows["null"] == 90).all()
    assert (rows["pct_null"] == 90.0).all()


def test_contributions_are_sorted_by_strength_and_carry_a_direction() -> None:
    model = load_champion()
    row = pd.Series({c: 1.0 for c in FEATURE_COLUMNS})
    frame = panels.contributions_for(model, row)
    assert list(frame.columns) == ["feature", "contribution", "direction"]
    assert len(frame) == len(FEATURE_COLUMNS)
    strengths = frame["contribution"].abs().tolist()
    assert strengths == sorted(strengths, reverse=True)
    assert set(frame["direction"]) <= {"increases_risk", "decreases_risk"}


def test_the_live_transcript_grounds() -> None:
    assert panels.grounding_for(_LIVE).verdict in {"grounded", "ungrounded"}


def test_the_flipped_transcript_is_rejected() -> None:
    """A verifier only ever shown passing is indistinguishable from one that cannot fail."""
    trace = panels.grounding_for(_FLIPPED)
    assert trace.verdict == "ungrounded"
    assert trace.failures
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_demo_panels.py -v`
Expected: FAIL, `ImportError: cannot import name 'panels'`.

- [ ] **Step 3: Write minimal implementation**

`src/almanac/demo/artifacts.py`:

```python
"""Read the committed demo artifacts. No Spark, no network."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from almanac.demo.build import DEMO_DATA_DIR


def _read_json(name: str, root: Path | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(((root or DEMO_DATA_DIR) / name).read_text())
    return payload


def load_queue(root: Path | None = None) -> pd.DataFrame:
    """The scored population, highest risk first."""
    return pd.read_parquet((root or DEMO_DATA_DIR) / "queue.parquet")


def load_coverage(root: Path | None = None) -> dict[str, Any]:
    return _read_json("coverage.json", root)


def load_medallion(root: Path | None = None) -> dict[str, Any]:
    return _read_json("medallion.json", root)
```

**Note:** importing `DEMO_DATA_DIR` from `build` would drag `pyspark` into
the app. Move `DEMO_DATA_DIR` into `artifacts.py` and have `build.py` import
it from there instead, so the app's import graph stays Spark-free. Update
Task 3 and 4's imports accordingly, and add this assertion to the test file:

```python
def test_the_app_import_graph_never_reaches_pyspark() -> None:
    """`make demo` must not start a JVM. The app imports artifacts and panels only."""
    import ast

    src = Path(__file__).resolve().parents[2] / "src" / "almanac" / "demo"
    for module in ("artifacts.py", "panels.py", "champion.py"):
        tree = ast.parse((src / module).read_text())
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0
        }
        assert "pyspark" not in imported, f"{module} imports pyspark"
```

`src/almanac/demo/panels.py`:

```python
"""Panel data, prepared. Pure functions -- no Streamlit import lives here."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from almanac.agent.bounded_agent import load_transcript
from almanac.agent.grounding import GroundingTrace, verify
from almanac.model.contributions import BASELINE_COLUMN, contribution_frame
from almanac.model.train import FEATURE_COLUMNS


def coverage_rows(coverage: dict[str, Any]) -> pd.DataFrame:
    """Per-feature null counts, which is the half a coverage number usually hides."""
    total = int(coverage["total_rows"])
    rows = [
        {
            "feature": name,
            "non_null": int(stats["non_null"]),
            "null": total - int(stats["non_null"]),
            "pct_null": round(100.0 * (total - int(stats["non_null"])) / total, 1)
            if total
            else 0.0,
        }
        for name, stats in coverage["features"].items()
    ]
    return pd.DataFrame(rows).sort_values("pct_null", ascending=False).reset_index(drop=True)


def contributions_for(model: Any, row: pd.Series) -> pd.DataFrame:
    """The champion's own per-feature contributions, strongest pull first."""
    features = pd.DataFrame([row[FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
    contributed = contribution_frame(model.booster_, features).iloc[0]
    frame = pd.DataFrame(
        {
            "feature": FEATURE_COLUMNS,
            "contribution": [float(contributed[c]) for c in FEATURE_COLUMNS],
        }
    )
    frame["direction"] = frame["contribution"].map(
        lambda v: "increases_risk" if v > 0 else "decreases_risk"
    )
    order = frame["contribution"].abs().sort_values(ascending=False).index
    return frame.loc[order].reset_index(drop=True)


def baseline_for(model: Any, row: pd.Series) -> float:
    """The baseline the contributions move from. Named, because it is not a feature."""
    features = pd.DataFrame([row[FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
    return float(contribution_frame(model.booster_, features).iloc[0][BASELINE_COLUMN])


def grounding_for(path: Path) -> GroundingTrace:
    """Verify a committed transcript. No model call."""
    return verify(load_transcript(path))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_demo_panels.py -v`
Expected: 5 passed.

- [ ] **Step 5: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add src/almanac/demo tests/unit/test_demo_panels.py docs/STATUS.md
git commit -m "feat: panel data preparation, with no Streamlit and no Spark

Pure functions: coverage rows that report the null side rather than only the
non-null one, contributions sorted by strength with their direction, and a
grounding verdict for a committed transcript.

Keeping Streamlit out of this module is what makes every data decision
testable without driving a UI. A test asserts the app's import graph never
reaches pyspark, because `make demo` starting a JVM would defeat the point.

The flipped-direction transcript is asserted to be REJECTED. A verifier only
ever shown passing is indistinguishable from one that cannot fail."
```

---

### Task 8: The app shell, and the scale it declares

**Files:**
- Create: `demo/app.py`
- Modify: `pyproject.toml` (a `demo` optional-dependency group)
- Modify: `Makefile` (add `demo`)
- Test: `tests/unit/test_demo_app.py`

**Interfaces:**
- Consumes: Task 7's `artifacts` and `panels`.
- Produces: `demo/app.py` as the Streamlit entrypoint. Later tasks add
  panels to it.

- [ ] **Step 1: Add the dependency group**

Check the current Streamlit release live before writing a floor — this
repo's rule is that version floors are checked when added, never carried
from a plan. Then in `pyproject.toml`:

```toml
demo = [
    "streamlit>=<checked floor>",
]
```

- [ ] **Step 2: Write the failing test**

`tests/unit/test_demo_app.py`:

```python
"""The app, driven headlessly. Layout, not logic -- logic is tested in panels."""

from pathlib import Path

import pytest

streamlit_testing = pytest.importorskip("streamlit.testing.v1")
AppTest = streamlit_testing.AppTest

_APP = Path(__file__).resolve().parents[2] / "demo" / "app.py"


def _run():
    app = AppTest.from_file(str(_APP), default_timeout=60)
    app.run()
    return app


def test_it_runs_without_exception() -> None:
    assert not _run().exception


def test_it_declares_its_scale_on_load() -> None:
    """Phase 8 shipped a dashboard rendering 200 healthy-looking rows against a
    horizon 340 days wrong. Plausible rendering of a wrong number is this
    project's most expensive recorded failure, and this is its highest-exposure
    surface.
    """
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "one archived hour" in text
    assert "341,060,851" in text
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: FAIL, the app file does not exist.

- [ ] **Step 4: Write minimal implementation**

`demo/app.py`:

```python
"""Almanac, locally. Committed artifacts, a committed champion, no cloud account."""

import streamlit as st

from almanac.demo import artifacts
from almanac.demo.champion import champion_provenance

st.set_page_config(page_title="Almanac — local demo", layout="wide")

st.title("Almanac — work-queue risk, running locally")

_medallion = artifacts.load_medallion()
_provenance = champion_provenance()

st.markdown(
    f"""
This runs **one archived hour per schema era** from GH Archive, scored by the
real registered champion (version `{_provenance["model_version"]}`, run
`{_provenance["run_id"]}`).

**It is not the full dataset.** The platform's measured backfill is
**341,060,851** rows over Q3 2025, for a measured $11.96. Every count below is
computed from the fixture hours, not from that quarter.
"""
)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: 2 passed.

- [ ] **Step 6: Add the Makefile target and run it**

```makefile
# The app. Reads demo/data/ and demo/model/ -- no SparkSession, no JVM, no
# network. Run `make demo-build` first if the artifacts are missing.
demo:
	uv run --extra demo streamlit run demo/app.py
```

Run `make demo` and confirm it opens.

- [ ] **Step 7: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add demo/app.py pyproject.toml uv.lock Makefile tests/unit/test_demo_app.py docs/STATUS.md
git commit -m "feat: the demo app shell, declaring its scale on load

Streamlit enters as an optional-dependency group, so the default install and
CI's existing jobs are untouched unless they ask for it.

The app states on load that it runs one archived hour per era, not the
341,060,851-row quarter, and a test asserts both strings are present. Phase 8
shipped a dashboard rendering 200 healthy-looking rows against a horizon 340
days wrong -- plausible rendering of a wrong number is this project's most
expensive recorded failure, and a public demo is the most exposed place it
could recur.

Driven headlessly with Streamlit's AppTest rather than verified by
screenshot."
```

---

### Task 9: The queue panel, and the coverage panel beside it

The spec's §4.1, its deliberate centrepiece.

**Files:**
- Modify: `demo/app.py`
- Modify: `tests/unit/test_demo_app.py`

**Interfaces:**
- Consumes: `artifacts.load_queue`, `artifacts.load_coverage`,
  `panels.coverage_rows`.
- Produces: a `st.session_state["selected_rank"]` int the contributions
  panel in Task 10 reads.

- [ ] **Step 1: Write the failing test**

```python
def test_the_queue_renders_no_identity() -> None:
    """docs/pseudonymization.md -- surrogates and ranks, never a login or owner/repo."""
    app = _run()
    rendered = " ".join(str(frame.value.columns.tolist()) for frame in app.dataframe)
    for forbidden in ("actor_login", "author_login", "repo_name", "repo_full_name"):
        assert forbidden not in rendered


def test_the_coverage_panel_states_why_the_features_are_null() -> None:
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "point-in-time" in text
    assert "not a defect" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: FAIL, no dataframe rendered and the copy absent.

- [ ] **Step 3: Write minimal implementation**

Append to `demo/app.py`:

```python
queue_tab, explain_tab, agent_tab, lake_tab = st.tabs(
    ["Intervention queue", "Why this score", "Agent, verified", "The medallion"]
)

with queue_tab:
    queue = artifacts.load_queue()
    coverage = artifacts.load_coverage()
    left, right = st.columns([3, 2])

    with left:
        st.subheader(f"Ranked by predicted breach risk — {len(queue)} pull requests")
        st.dataframe(
            queue[["rank", "repo_id", "pr_number", "breach_risk", "is_bot_author"]],
            hide_index=True,
            use_container_width=True,
        )
        st.caption(
            "Surrogate keys and rank position only. Logins and `owner/repo` names "
            "identify real people who never opted into this project, so they are "
            "never rendered — see `docs/pseudonymization.md`."
        )

    with right:
        st.subheader("How many features actually have a value")
        rows = panels.coverage_rows(coverage)
        st.dataframe(rows, hide_index=True, use_container_width=True)
        st.markdown(
            "**This is point-in-time correctness, and it is not a defect.** A "
            "feature named *to date* may read only events strictly before its own "
            "`as_of`. One archived hour contains almost no prior history, so most "
            "of these are null and the queue is ranking on the few that are not. "
            "At the measured quarter's scale they populate; here you can see "
            "exactly why they do not."
        )
```

Add `from almanac.demo import panels` to the imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: 4 passed.

- [ ] **Step 5: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add demo/app.py tests/unit/test_demo_app.py docs/STATUS.md
git commit -m "feat: the intervention queue, with its feature coverage beside it

The coverage panel is given equal width deliberately. <K> of 10 features are
null for most rows, and that is the governing invariant visible rather than
asserted: a feature named 'to date' may read only events strictly before its
own as_of, and one archived hour has almost no prior history to read.

Rendering the queue without it would show a plausible ordering and hide what
produced it. Rendering them together turns the smallest dataset in the repo
into the clearest demonstration of the thing the whole project is about.

Surrogate keys and ranks only; a test asserts no identity column reaches the
rendered frame."
```

Replace `<K>` with the measured value from Task 4.

---

### Task 10: The contributions panel

**Files:**
- Modify: `demo/app.py`
- Modify: `tests/unit/test_demo_app.py`

**Interfaces:**
- Consumes: `panels.contributions_for`, `panels.baseline_for`,
  `champion.load_champion`.
- Produces: nothing later tasks read.

- [ ] **Step 1: Write the failing test**

```python
def test_the_explain_tab_names_the_baseline_as_not_a_feature() -> None:
    """LightGBM returns n_features + 1 columns and the last is the expected value.

    Treating it as a feature is a silent off-by-one, which is why it is labelled.
    """
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "baseline" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: FAIL, the string is absent.

- [ ] **Step 3: Write minimal implementation**

Append inside `with explain_tab:`:

```python
with explain_tab:
    queue = artifacts.load_queue()
    model = st.cache_resource(load_champion)()

    rank = st.number_input("Rank", min_value=1, max_value=int(queue["rank"].max()), value=1, step=1)
    row = queue.loc[queue["rank"] == rank].iloc[0]

    st.metric("Predicted breach risk", f"{row['breach_risk']:.6f}")
    st.dataframe(panels.contributions_for(model, row), hide_index=True, use_container_width=True)
    st.markdown(
        f"Contributions move from the model's own **baseline** of "
        f"`{panels.baseline_for(model, row):.6f}`, which is *not* a feature — "
        "LightGBM returns one more column than there are features and the last "
        "is the expected value. These are the champion's own numbers, computed "
        "here, not an explanation written about them."
    )
```

Add `from almanac.demo.champion import load_champion` to the imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: 5 passed.

- [ ] **Step 5: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add demo/app.py tests/unit/test_demo_app.py docs/STATUS.md
git commit -m "feat: per-feature contributions, computed live by the champion

Reuses almanac.model.contributions.contribution_frame rather than adding
scoring logic -- it was already a pure function over anything exposing
LightGBM's pred_contrib interface.

The baseline is labelled as not a feature. LightGBM returns n_features + 1
columns and the last is the expected value, its documented departure from
shap; treating it as a feature is a silent off-by-one, and the panel says so
rather than leaving a reader to assume.

This makes the repo's stated rule visible: an explanation is generated FROM
the model's actual feature contributions, never alongside them."
```

---

### Task 11: The agent panel, showing the verifier reject

**Files:**
- Modify: `demo/app.py`
- Modify: `tests/unit/test_demo_app.py`

**Interfaces:**
- Consumes: `panels.grounding_for`.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

```python
def test_the_agent_tab_shows_a_rejection_not_only_a_pass() -> None:
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "ungrounded" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```python
with agent_tab:
    st.markdown(
        "The agent answers from tools only, and a **deterministic verifier** checks "
        "the answer before you read it. No model call happens here: both runs below "
        "are committed transcripts, replayed."
    )
    choice = st.radio(
        "Transcript",
        ["The live window's answer", "The same answer, one direction flipped"],
        horizontal=True,
    )
    path = _LIVE_TRANSCRIPT if choice.startswith("The live") else _FLIPPED_TRANSCRIPT
    trace = panels.grounding_for(path)

    st.markdown(f"**Verdict: `{trace.verdict}`**")
    if trace.failures:
        for failure in trace.failures:
            st.error(failure)
    st.markdown(
        "Three checks, and the second is the one that matters. Every number must "
        "trace to a tool return. The **relationship** claimed around it must trace "
        "to the field that claim type requires — the live window said the model was "
        "*trained on* a Delta version the tools had only *read*, and every number in "
        "that sentence was real. A directional statement must agree with the sign of "
        "the contribution it names, which is what the flipped transcript violates."
    )
    st.dataframe(
        pd.DataFrame([c.model_dump() for c in trace.numbers]),
        hide_index=True,
        use_container_width=True,
    )
```

Add at the top of `app.py`:

```python
from pathlib import Path

import pandas as pd

_FIXTURES = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "transcripts"
_LIVE_TRANSCRIPT = _FIXTURES / "2026-09-10-live-predict-explain.json"
_FLIPPED_TRANSCRIPT = _FIXTURES / "2026-09-10-live-predict-explain-directions-flipped.json"
```

**Deployment note:** the Space must carry these two transcripts. Task 13
copies them into the Space alongside `demo/`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: 6 passed.

- [ ] **Step 5: Update `docs/STATUS.md` and commit**

```bash
make check-fast
git add demo/app.py tests/unit/test_demo_app.py docs/STATUS.md
git commit -m "feat: the agent panel, showing the verifier reject as well as pass

Both transcripts are committed and replay with no model call. The panel
defaults to letting a reader switch to the flipped-direction run and watch
the verdict go ungrounded, because a verifier only ever shown passing is
indistinguishable from one that cannot fail.

The copy leads with the relationship check rather than the numeric one. The
live window's false claim had every number traceable to a tool return; what
was wrong was 'trained on' pointing at a version the tools had only read."
```

---

### Task 12: The medallion panel

**Files:**
- Modify: `demo/app.py`
- Modify: `tests/unit/test_demo_app.py`

- [ ] **Step 1: Write the failing test**

```python
def test_the_medallion_tab_shows_every_era_and_its_quarantine() -> None:
    app = _run()
    text = " ".join(m.value for m in app.markdown)
    for event_date in ("2025-08-13", "2014-06-12", "2025-11-03"):
        assert event_date in text
    assert "quarantine" in text.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```python
with lake_tab:
    medallion = artifacts.load_medallion()
    st.subheader("Bronze, Silver, and what got quarantined")
    st.dataframe(pd.DataFrame(medallion["eras"]), hide_index=True, use_container_width=True)
    dates = ", ".join(era["event_date"] for era in medallion["eras"])
    st.markdown(
        f"One hour per schema era — {dates}. Bronze never transforms; the payload "
        "stays a JSON string and parsing is per-type in Silver, so a new event type "
        "cannot break ingestion. Bad records are **quarantined, never dropped**, and "
        "carry an array of the rules they failed rather than a boolean, so the "
        "quarantine is analyzable by rule. `silver + quarantine == scored` is "
        "asserted on every build.\n\n"
        "Run it yourself with `make demo-build`, or the full Gold path with "
        "`make dbt`."
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra demo pytest tests/unit/test_demo_app.py -v`
Expected: 7 passed.

- [ ] **Step 5: Run the full suite**

Run: `make check`
Expected: green. Record the duration.

- [ ] **Step 6: Update `docs/STATUS.md` and commit**

```bash
git add demo/app.py tests/unit/test_demo_app.py docs/STATUS.md
git commit -m "feat: the medallion panel, three eras and their quarantine

Bronze, Silver and quarantine counts per schema era, read from the artifact
the build step writes. States the two rules a reader cannot see from a row
count -- Bronze never transforms, and bad records are quarantined with the
array of rules they failed rather than dropped -- and points at make
demo-build and make dbt for running it rather than reading about it."
```

---

### Task 13: Deploy to Hugging Face Spaces

**Files:**
- Create: `demo/Dockerfile`, `demo/README.md` (the Space's card)
- Create: `scripts/deploy_space.py`
- Modify: `README.md`, `docs/STATUS.md`, `CHANGELOG.md`

- [ ] **Step 1: Write the Space's Dockerfile**

Pin the native stack explicitly. §6.1's crash does not occur on Linux, which
is exactly why the guard stays in the code rather than being replaced by
this file.

```dockerfile
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src/almanac/__init__.py src/almanac/__init__.py
RUN uv sync --extra demo --frozen --no-dev

COPY . .

EXPOSE 7860
CMD ["uv", "run", "--extra", "demo", "--frozen", "streamlit", "run", "demo/app.py", \
     "--server.port=7860", "--server.address=0.0.0.0"]
```

- [ ] **Step 2: Verify the container locally before deploying**

```bash
docker build -f demo/Dockerfile -t almanac-demo .
docker run --rm -p 7860:7860 almanac-demo
```

Open `http://localhost:7860` and check all four tabs. **Do not deploy
without this step** — a broken public demo is worse than none.

- [ ] **Step 3: Confirm what the Space will publish**

```bash
uv run pytest -m "not network and not spark" -k pseudonym -v
```

Then list every file the Space carries and confirm each is intended. The
Space needs `demo/`, `src/almanac/`, `pyproject.toml`, `uv.lock`, and the
two transcript fixtures. It does **not** need `infra/`, `dashboards/`, or
`.databricks/`.

- [ ] **Step 4: Deploy**

Ask the user for the Space name and token; do not assume either. Push, wait
for the build, open the URL, and click through all four tabs on the live
Space.

- [ ] **Step 5: Update the README**

Rewrite the **"No persistent public demo"** paragraph. It has been true
since `v1.0` and stops being true here, which is precisely the class of
statement this repo has gotten wrong nine times. Replace it with the live
URL, state what the demo runs on (one archived hour per era, the real
champion), and keep the sentence about billable infrastructure being torn
down, which remains true.

Add the demo link to the badges and to the "Hiring managers, 5 minutes"
bullet in **Who should look at what**.

- [ ] **Step 6: Close out the phase**

- `docs/STATUS.md`: Phase 11 complete, every exit-gate row from the design
  doc §10 marked against its evidence.
- `CHANGELOG.md`: a `v1.2.0` section led by what the phase *found* — the
  OpenMP segfault and the feature-coverage measurement — not by what it
  built.
- `CLAUDE.md`: update **Current status** and the phase table.
- The story-bank gist: the segfault, the scope cut after "are we making it
  complex?", and the rejected real-quarter export.

- [ ] **Step 7: Full suite, then push and open the PR**

```bash
make check
git add -A
git commit -m "feat: deploy the demo to Hugging Face Spaces, and retire a README claim

The README has said 'No persistent public demo' since v1.0. It is now false,
and this commit is what makes it false -- retired in the same commit rather
than left for a later pass, which is the exact failure mode this repo has
recorded nine times.

Free tier, no billable resource. The container pins the native stack; the
import-order guard from Task 1 stays in the code regardless, because the
segfault does not occur on the Space's Linux and does occur on a laptop,
which is where a reader clones it."
git push -u origin phase-11-local-demo
```

Deferred until Tasks 14-15 (added 2026-09-11, after Task 13's own deploy
work) are also done — see below. **Do not open the PR after this task.**

---

### Task 14: The architecture walkthrough's node/edge data

Design: `docs/design/2026-09-11-almanac-architecture-walkthrough-design.md`.
Pure data and its own invariants, no Streamlit -- same split as Task 7 kept
panel data preparation free of Streamlit imports.

**Files:**
- Create: `src/almanac/demo/architecture.py`
- Create: `tests/unit/test_demo_architecture.py`

**Interfaces:**
- Produces: `NODES: tuple[ArchNode, ...]`, `EDGES: tuple[ArchEdge, ...]`,
  `missing_links(repo_root: Path) -> dict[str, list[str]]` for Task 15's
  tab and its own tests.

- [ ] **Step 1: Write the failing tests**

```python
from pathlib import Path

from almanac.demo.architecture import EDGES, NODES, missing_links

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_every_node_id_is_unique() -> None:
    ids = [n.id for n in NODES]
    assert len(ids) == len(set(ids))


def test_every_edge_endpoint_names_a_real_node() -> None:
    ids = {n.id for n in NODES}
    for edge in EDGES:
        assert edge.source in ids, f"{edge.source} is not a node id"
        assert edge.target in ids, f"{edge.target} is not a node id"


def test_no_node_carries_a_link_free_summary_or_a_summary_free_link() -> None:
    """A node that asserts something must point at where it was measured."""
    for node in NODES:
        assert node.links, f"{node.id} has no link -- it is a claim with no source"


def test_no_summary_or_label_contains_a_digit() -> None:
    """The governing rule from the design doc, section 2: a node names a
    fact, it never restates one. A digit in prose is a restated measurement;
    digits belong only in link paths (dates, ADR numbers), never in the text
    a viewer reads without clicking through."""
    for node in NODES:
        assert not any(c.isdigit() for c in node.label), node.id
        assert not any(c.isdigit() for c in node.summary), node.id


def test_every_link_resolves_to_a_real_file() -> None:
    missing = missing_links(_REPO_ROOT)
    assert not missing, f"dead links: {missing}"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_demo_architecture.py -v`
Expected: FAIL/ERROR -- `almanac.demo.architecture` does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

`src/almanac/demo/architecture.py`:

```python
"""The architecture walkthrough's node/edge graph. Pure data -- every claim
links to where it was actually found; nothing here is restated from there.
See docs/design/2026-09-11-almanac-architecture-walkthrough-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ArchNode:
    id: str
    label: str
    summary: str
    links: tuple[str, ...]


@dataclass(frozen=True)
class ArchEdge:
    source: str
    target: str


NODES: tuple[ArchNode, ...] = (
    ArchNode(
        "bronze", "Bronze",
        "Raw payload, untransformed. A new event type cannot break ingestion.",
        ("docs/limitations.md", "docs/findings/2026-09-02-bronze-is-single-threaded.md"),
    ),
    ArchNode(
        "silver", "Silver",
        "Per-type parsing and quality rules. Bad records are quarantined, never dropped.",
        ("docs/findings/2026-09-08-first-real-quarantine.md",),
    ),
    ArchNode(
        "gold", "Gold",
        "dbt-built facts and dimensions. A merge defect here was found the hard way.",
        (
            "docs/findings/2026-09-04-gold-is-a-metastore-table-not-a-path.md",
            "docs/findings/2026-09-08-pr-opened-spine-fanout.md",
        ),
    ),
    ArchNode(
        "features", "Feature Platform",
        "As-of joins enforce point-in-time correctness -- the governing invariant.",
        ("docs/adr/0001-hand-rolled-as-of-join.md",),
    ),
    ArchNode(
        "model", "Model",
        "A baseline shipped first. The registered champion had a leakage bug, found and fixed.",
        (
            "docs/findings/2026-09-08-champion-rescored-temporal-split.md",
            "docs/decision-memo.md",
        ),
    ),
    ArchNode(
        "serving", "Serving",
        "A live endpoint, measured for skew against offline scoring.",
        (
            "docs/findings/2026-09-04-serving-endpoint-measured.md",
            "docs/findings/2026-09-08-training-serving-skew-measured.md",
        ),
    ),
    ArchNode(
        "streaming", "Streaming",
        "A live poller and an online store. A watermark silently dropped real data once.",
        (
            "docs/postmortem-watermark-data-loss.md",
            "docs/adr/0004-dedup-on-write-not-watermark.md",
        ),
    ),
    ArchNode(
        "agent", "Agent layer",
        "Four read-only tools over MCP, bounded, and audited.",
        ("docs/findings/2026-09-11-agent-layer-window-cost.md",),
    ),
    ArchNode(
        "grounding", "Grounding verifier",
        "Checks the relationship a claim makes, not only that its number is real.",
        ("CHANGELOG.md",),
    ),
)

EDGES: tuple[ArchEdge, ...] = (
    ArchEdge("bronze", "silver"),
    ArchEdge("silver", "gold"),
    ArchEdge("gold", "features"),
    ArchEdge("gold", "streaming"),
    ArchEdge("features", "model"),
    ArchEdge("model", "serving"),
    ArchEdge("model", "agent"),
    ArchEdge("agent", "grounding"),
)


def missing_links(repo_root: Path) -> dict[str, list[str]]:
    """Node id -> its links that do not resolve to a real file. Empty if clean."""
    result: dict[str, list[str]] = {}
    for node in NODES:
        gone = [link for link in node.links if not (repo_root / link).exists()]
        if gone:
            result[node.id] = gone
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_demo_architecture.py -v`
Expected: 5 passed. If a link is dead, `ls docs/adr/ docs/findings/` and
correct the path -- do not weaken the test.

- [ ] **Step 5: Lint, typecheck, update STATUS.md, commit**

```bash
uv run ruff check src/almanac/demo/architecture.py tests/unit/test_demo_architecture.py
uv run ruff format --check src/almanac/demo/architecture.py tests/unit/test_demo_architecture.py
uv run mypy --strict src/almanac/demo/architecture.py
git add src/almanac/demo/architecture.py tests/unit/test_demo_architecture.py docs/STATUS.md
git commit -m "feat: the architecture walkthrough's node/edge graph

Nine nodes, Bronze through the grounding verifier, each carrying a one-line
summary and at least one link to the ADR or finding where that claim was
actually measured -- never both a summary and a restated number, which is
the failure mode this repo has recorded nine times. Enforced by a test, not
a convention: no node's label or summary may contain a digit at all, and a
second test checks every link resolves to a real file, so a rename elsewhere
in the repo fails this suite rather than quietly breaking the walkthrough."
```

---

### Task 15: The architecture walkthrough tab

Renders Task 14's graph with `streamlit-flow-component`, wired as the demo
app's fifth tab per the design doc's explicit choice of a tab over a
separate page.

**Files:**
- Modify: `demo/app.py`, `pyproject.toml`, `demo/requirements.txt`
- Modify: `tests/unit/test_demo_app.py`

**Interfaces:**
- Consumes: `architecture.NODES`, `architecture.EDGES`.

- [ ] **Step 1: Add the dependency**

Add `streamlit-flow-component>=<current release, checked live against
PyPI's JSON API before writing the floor -- do not carry over a number
from training data>` to the `demo` extra in `pyproject.toml`. Then:

```bash
uv lock
uv export --extra demo --extra ml-scoring --extra agent --no-dev \
  --no-emit-project --no-hashes --no-header --format requirements-txt \
  -o demo/requirements.txt
```

- [ ] **Step 2: Write the failing test**

```python
def test_the_architecture_tab_renders_every_node_with_no_exception() -> None:
    app = _run()
    assert not app.exception
    text = " ".join(m.value for m in app.markdown)
    for node in NODES:
        assert node.label in text
```

Add `from almanac.demo.architecture import NODES` to the test file's imports.

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run --extra demo --extra ml-scoring --extra agent pytest
tests/unit/test_demo_app.py -v`
Expected: FAIL -- no fifth tab, node labels absent.

- [ ] **Step 4: Write the minimal implementation**

In `demo/app.py`, extend the existing tab tuple to five and add the new
tab's body, rendering each node as a labelled block with its summary and
link(s) -- `streamlit_flow` for the diagram layout, falling back to a
plain list if the component is unavailable, matching this repo's existing
practice of never letting a demo panel hard-fail on an optional dependency.
Import `from almanac.demo.architecture import EDGES, NODES`.

- [ ] **Step 5: Run test to verify it passes**

Run the Step 3 command again. Expected: all `test_demo_app.py` tests pass.

- [ ] **Step 6: Verify for real, not just AppTest**

`AppTest` does not execute frontend JavaScript, so a broken
`streamlit-flow-component` render would pass Step 5 and fail for a real
viewer -- exactly the HTTP-200-proves-nothing lesson from Task 13. Run
`make demo`, open it in a browser, click every node, confirm each link
opens the real file.

- [ ] **Step 7: Lint, typecheck, full suite, update STATUS.md, commit**

```bash
make check
git add demo/app.py pyproject.toml uv.lock demo/requirements.txt tests/unit/test_demo_app.py docs/STATUS.md
git commit -m "feat: the architecture walkthrough, as the demo's fifth tab

Renders Task 14's node/edge graph with streamlit-flow-component. Chosen
over a separate st.navigation page after weighing both explicitly in the
design doc: a fifth tab is visible to every visitor with no extra click,
which matters more for a hiring-manager-facing demo than the cleaner
separation a second page would give. This revisits and supersedes the
original design doc's section 11 row rejecting a fifth panel -- a
different, narrower proposal (a point-in-time panel duplicating the
coverage panel's own argument) than what this actually is."
```

---

Then close out Task 13's own deferred Steps 5-7 (README already updated;
phase closeout in STATUS.md/CHANGELOG.md/CLAUDE.md and the story-bank
gist; full suite; push and the one PR for the whole phase, now covering
Tasks 1-15).

---

## Self-review

**Spec coverage.** §3 what-this-is-not → Task 8's scale declaration and
Task 4's no-training decision. §4.1 queue and coverage → Tasks 4 and 9.
§4.2 contributions → Tasks 7 and 10. §4.3 agent replay → Tasks 7 and 11.
§4.4 medallion → Tasks 3 and 12. §5.1 build/app separation → Tasks 3, 7,
8. §5.2 byte-identical → Task 5. §5.3 no training → Task 2's committed
champion. §6.1 OpenMP → Task 1. §6.2 fixture-scale honesty → Task 8. §7
identity → Tasks 4 and 6. §8 testing → every task. §9 deployment → Task
13. §10 exit gate → Task 13 Step 6.

**Known gaps, stated rather than hidden.**

1. **Task 5 Step 3 may not achieve a deterministic parquet.** The fallback
   to sorted JSON is written into the step because the outcome is not
   certain in advance. Whichever is chosen must be recorded in the commit.
2. **The `AppTest` assertions are string matches against panel copy.** They
   are brittle to rewording and shallow by nature. The spec's §12 already
   records this; where a panel is mostly layout, record that rather than
   asserting something trivial to claim coverage.
3. **Task 4's measured numbers will differ from the ones quoted here**,
   because Task 3 lands a third era that the 2026-09-11 measurement did
   not. Every task that quotes them says so.
4. **Task 13 Step 4 needs the user's Space name and token.** It is the one
   step this plan cannot specify, and it must not be guessed.
