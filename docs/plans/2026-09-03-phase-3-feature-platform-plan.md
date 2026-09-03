# Phase 3 — Feature Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the feature platform's offline store — a hand-rolled,
point-in-time-correct as-of join over Silver, three v1 feature tables
scoped to Phase 4's SLA-risk model, and a leakage suite that tests
CLAUDE.md's governing invariant directly rather than by convention.

**Architecture:** A new `almanac/features/` package, structured exactly like
`almanac/pipeline/`: pure `DataFrame -> DataFrame` transforms (spine,
feature-group computation, the as-of join itself, assembly) with all I/O —
reading Silver, writing Delta, registering Unity Catalog metadata — pushed
to one boundary module (`runner.py`), the same separation `pipeline/silver.py`
already established. Every feature table is computed straight from Silver,
never from Gold (design doc §3.1), and is an append-only, full-recompute
Delta table under the `features` container Phase 0's Terraform already
provisioned.

**Tech Stack:** PySpark + Delta Lake (matching the rest of the repo);
`pytest` with the existing `spark` / `integration` markers; no new
dependency — nothing here needs `databricks-feature-engineering` or Feast
(design doc §4.4a).

**Spec:** `docs/design/2026-09-01-almanac-system-design.md` §4.4 and §4.4a
(the Phase 3 concretization committed `6feaf75`). §3.1 (peer-of-Gold), §5.1
(the label and its era-bound components), and §9's phase table are also
load-bearing constraints this plan inherits.

## Global Constraints

- **Point-in-time correctness is the one invariant that cannot be relaxed
  anywhere in this plan** (CLAUDE.md). Every as-of computation uses strict
  `<`, never `<=`, on event time vs. the as-of timestamp.
- **The feature platform reads Silver, never Gold** (§3.1). No task in this
  plan imports from `almanac.gold` for data — `almanac.gold.sources.table_location`
  is reused for its Delta-path-vs-URI logic only, not for reading Gold's tables.
- **Offline-only.** No online store, no vector index — both deferred to
  Phase 4/5 per §9 and §4.4a. No task in this plan stands up online
  infrastructure.
- **The as-of join is hand-rolled** — `almanac.features` owns the join
  logic; nothing calls Databricks Feature Engineering's
  `create_training_set` or a Feast client (§4.4a).
- **UC `TIMESERIES` registration is governance-only, not correctness**, and
  is pure SQL-string-building, unit-tested without a live Unity Catalog
  target — the local Derby metastore this repo tests against does not
  support it, so live execution is verified during Phase 3's cloud burn,
  the same way Photon A/B and the backfill were (STATUS.md's verification
  log, not CI).
- **Full recompute, not incremental.** v1's feature tables are rebuilt
  from the whole of Silver on every run and the Delta path is overwritten.
  This is stated as a known, deferred limitation (see Deferred, below),
  not silently accepted.
- **`make check` (ruff, ruff format --check, mypy --strict, pytest -m "not network")
  must be green before every commit**, per CLAUDE.md's testing policy —
  the whole suite, not just this plan's new tests.
- **One commit per task**, `docs/STATUS.md` updated in the same commit as
  the work it describes (CLAUDE.md; `.claude/hooks/check-status-md-commit.sh`
  enforces this mechanically).

---

## File Structure

```
src/almanac/features/
    __init__.py            # empty, matches almanac/gold/__init__.py
    spine.py                # Task 1 — build_pr_opened_spine
    join.py                  # Task 2 — as_of_join (the core correctness engine)
    bot.py                    # Task 3 — is_bot_column (shared Spark-native bot check)
    groups.py                # Task 3 — compute_author_activity / compute_repo_activity / compute_pr_static
    assemble.py              # Task 4 — assemble_training_set
    registration.py          # Task 5 — primary_key_sql, register_feature_table
    runner.py                # Task 6 — the I/O boundary + CLI

scripts/
    features.py               # Task 6 — launch shim, mirrors scripts/gold.py

tests/unit/
    test_features_spine.py
    test_features_join.py
    test_features_bot.py
    test_features_groups.py
    test_features_assemble.py
    test_features_registration.py

tests/integration/
    test_features_leakage.py   # Task 7 — the CLAUDE.md invariant, tested directly
    test_features_runner.py    # Task 7 — end-to-end + idempotency
```

Each module owns one responsibility, following the existing
`pipeline/{bronze,dedup,eras,payloads,quality,silver}.py` split: small,
pure, independently testable files, with `runner.py` the only place that
touches Spark I/O or the metastore — exactly `gold/sources.py`'s and
`pipeline/silver.py`'s own division of labor.

---

### Task 1: The PR-opened spine

**Files:**
- Create: `src/almanac/features/__init__.py` (empty)
- Create: `src/almanac/features/spine.py`
- Test: `tests/unit/test_features_spine.py`

**Interfaces:**
- Produces: `build_pr_opened_spine(events: DataFrame) -> DataFrame`, columns
  `repo_id: long, pr_number: long, author_login: string, as_of_timestamp: timestamp`.
  Every later task's `spine` input has exactly this shape.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_features_spine.py
"""build_pr_opened_spine: the PR-opened population, re-derived from Silver
-- not read from fact_pull_request, per §3.1's peer-of-Gold boundary
(design doc §4.4a).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.spine import build_pr_opened_spine

pytestmark = pytest.mark.spark

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


def test_only_opened_pull_request_events_become_spine_rows(spark: SparkSession) -> None:
    rows = [
        (1, 10, datetime(2025, 8, 13, 9, tzinfo=UTC), "PullRequestEvent", "opened", "alice",
         None, False, None, datetime(2025, 8, 13, 9, 5, tzinfo=UTC)),
        # A closed event for the same PR must not also become a spine row.
        (1, 10, datetime(2025, 8, 14, 9, tzinfo=UTC), "PullRequestEvent", "closed", "bob",
         True, False, None, datetime(2025, 8, 14, 9, 5, tzinfo=UTC)),
        # A review event is not an "opened" event at all.
        (1, 10, datetime(2025, 8, 13, 12, tzinfo=UTC), "PullRequestReviewEvent", None, "carol",
         None, None, None, datetime(2025, 8, 13, 12, 5, tzinfo=UTC)),
    ]
    events = spark.createDataFrame(rows, _SCHEMA)

    spine = build_pr_opened_spine(events)

    assert spine.columns == ["repo_id", "pr_number", "author_login", "as_of_timestamp"]
    result = spine.collect()
    assert len(result) == 1
    assert result[0]["author_login"] == "alice"
    assert result[0]["as_of_timestamp"] == datetime(2025, 8, 13, 9, tzinfo=UTC)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_features_spine.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.spine'`

- [ ] **Step 3: Write the implementation**

```python
# src/almanac/features/spine.py
"""The PR-opened event population -- the entity list every v1 feature
group's as-of join runs against.

Re-derived from Silver directly, not read from `fact_pull_request`: §3.1
makes the feature platform a peer of Gold, not a consumer of it, and
reading Gold's own fact here would turn that boundary into a claim rather
than a structural fact (design doc §4.4a).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_pr_opened_spine(events: DataFrame) -> DataFrame:
    """One row per PR-opened event: who opened it, and when.

    `as_of_timestamp` is the opening event's own `created_at` -- the
    instant every as-of join in this package treats as "now" for that PR.
    """
    return (
        events.where(
            (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "opened")
        )
        .select(
            "repo_id",
            "pr_number",
            F.col("actor_login").alias("author_login"),
            F.col("created_at").alias("as_of_timestamp"),
        )
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_features_spine.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/almanac/features/__init__.py src/almanac/features/spine.py tests/unit/test_features_spine.py
git commit -m "feat: the PR-opened spine, re-derived from Silver"
```

---

### Task 2: The as-of join engine

This is the core correctness mechanism the whole subsystem exists for. It
is built through three TDD cycles in one task because each cycle's test is
what forces the next refinement — a reviewer approving the first
implementation without the second and third tests would be approving a
join that silently leaks.

**Files:**
- Create: `src/almanac/features/join.py`
- Test: `tests/unit/test_features_join.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (works on any two DataFrames sharing
  `on` columns).
- Produces: `as_of_join(spine: DataFrame, feature_table: DataFrame, *, on: list[str], spine_time_col: str = "as_of_timestamp", event_time_col: str = "event_time") -> DataFrame`.
  Every later task that assembles features calls this.

- [ ] **Step 1: Write the first failing test — the ordinary case**

```python
# tests/unit/test_features_join.py
"""as_of_join: the point-in-time-correct engine every feature group's
lookup runs through. Three cases, each forcing the next refinement:
ordinary (latest prior row wins), boundary (a row timestamped exactly at
the as-of time must not count as prior), cold start (no qualifying row
leaves nulls, never drops the spine row).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.join import as_of_join

pytestmark = pytest.mark.spark


def test_the_latest_row_strictly_before_the_as_of_time_wins(spark: SparkSession) -> None:
    spine = spark.createDataFrame(
        [(1, datetime(2025, 8, 13, tzinfo=UTC))],
        "repo_id long, as_of_timestamp timestamp",
    )
    feature_table = spark.createDataFrame(
        [
            (1, datetime(2025, 8, 10, tzinfo=UTC), 1),
            (1, datetime(2025, 8, 12, tzinfo=UTC), 2),  # latest prior row
            (1, datetime(2025, 8, 14, tzinfo=UTC), 99),  # after the as-of time
        ],
        "repo_id long, event_time timestamp, value long",
    )

    result = as_of_join(spine, feature_table, on=["repo_id"]).collect()

    assert len(result) == 1
    assert result[0]["value"] == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_features_join.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.join'`

- [ ] **Step 3: Write a first implementation (ordinary case only)**

```python
# src/almanac/features/join.py
"""The point-in-time join every feature lookup in this package runs
through. Its correctness is the whole reason the feature platform exists
as its own subsystem (design doc §4.4a) -- see
tests/integration/test_features_leakage.py for the invariant this is
built to satisfy.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def as_of_join(
    spine: DataFrame,
    feature_table: DataFrame,
    *,
    on: list[str],
    spine_time_col: str = "as_of_timestamp",
    event_time_col: str = "event_time",
) -> DataFrame:
    row_id = "_as_of_row_id"
    feature_cols = [c for c in feature_table.columns if c not in on]
    ft = feature_table.select(*on, *[F.col(c).alias(f"__ft_{c}") for c in feature_cols])
    ft_time = f"__ft_{event_time_col}"

    candidates = (
        spine.withColumn(row_id, F.monotonically_increasing_id())
        .join(ft, on=on, how="left")
        .where(F.col(ft_time) < F.col(spine_time_col))
    )
    window = Window.partitionBy(row_id).orderBy(F.col(ft_time).desc())
    result = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop(row_id, "_rn")
    )
    for c in feature_cols:
        prefixed = f"__ft_{c}"
        if c == event_time_col:
            result = result.drop(prefixed)
        else:
            result = result.withColumnRenamed(prefixed, c)
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_features_join.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Add the boundary test — this is the leakage guard itself**

```python
def test_a_row_timestamped_exactly_at_the_as_of_time_is_not_prior(spark: SparkSession) -> None:
    """The strict `<` this project's governing invariant depends on
    (CLAUDE.md): a feature row at exactly T has not happened yet from a
    spine row asking "as of T", and must not leak in.
    """
    t = datetime(2025, 8, 13, tzinfo=UTC)
    spine = spark.createDataFrame([(1, t)], "repo_id long, as_of_timestamp timestamp")
    feature_table = spark.createDataFrame(
        [(1, t, 999)],  # exactly at the as-of time -- must be excluded
        "repo_id long, event_time timestamp, value long",
    )

    result = as_of_join(spine, feature_table, on=["repo_id"]).collect()

    assert len(result) == 1
    assert result[0]["value"] is None
```

Run: `uv run pytest tests/unit/test_features_join.py -v`
Expected: PASS — the existing `where(ft_time < spine_time)` already
excludes the boundary row, but it also currently *drops the spine row
entirely* rather than keeping it with a null value, since the `where`
filters out the only candidate. Confirm this by inspection: the current
implementation returns **0 rows**, not 1 with `value = None`. This is the
next test to add, and it is the one that forces the real fix.

- [ ] **Step 6: Confirm the drop, then add the cold-start test**

```python
def test_no_qualifying_row_leaves_features_null_rather_than_dropping_the_spine_row(
    spark: SparkSession,
) -> None:
    """Cold start is a fact about an entity's history, not an error --
    the same discipline `label_exclusion` already applies to the label
    (design doc §5.1). A first-time author, or a repo with only future
    rows relative to this spine row, must still appear in the assembled
    training set.
    """
    spine = spark.createDataFrame(
        [(1, datetime(2025, 8, 13, tzinfo=UTC)), (2, datetime(2025, 8, 13, tzinfo=UTC))],
        "repo_id long, as_of_timestamp timestamp",
    )
    feature_table = spark.createDataFrame(
        [(1, datetime(2025, 8, 14, tzinfo=UTC), 999)],  # repo 1's only row is in the future; repo 2 has none at all
        "repo_id long, event_time timestamp, value long",
    )

    result = {r["repo_id"]: r["value"] for r in as_of_join(spine, feature_table, on=["repo_id"]).collect()}

    assert result == {1: None, 2: None}
```

Run: `uv run pytest tests/unit/test_features_join.py -v`
Expected: FAIL — both the boundary test and this test currently return
too few rows, since the `where` clause removes non-qualifying spine rows
instead of nulling their features.

- [ ] **Step 7: Fix the implementation to keep every spine row**

Replace the filter-then-join approach with a qualifies-flag that nulls
features on the losing candidate instead of dropping the row:

```python
# src/almanac/features/join.py  (replaces Step 3's version in full)
"""The point-in-time join every feature lookup in this package runs
through. Its correctness is the whole reason the feature platform exists
as its own subsystem (design doc §4.4a) -- see
tests/integration/test_features_leakage.py for the invariant this is
built to satisfy.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def as_of_join(
    spine: DataFrame,
    feature_table: DataFrame,
    *,
    on: list[str],
    spine_time_col: str = "as_of_timestamp",
    event_time_col: str = "event_time",
) -> DataFrame:
    """Left join spine to the latest feature_table row per `on`, strictly
    before spine_time_col.

    Every spine row survives. "No qualifying row" -- no match at all, or
    every match lies at or after the as-of time -- nulls every feature
    column rather than dropping the row (the boundary and cold-start
    cases below) or leaking a future value.
    """
    row_id = "_as_of_row_id"
    feature_cols = [c for c in feature_table.columns if c not in on]
    ft = feature_table.select(*on, *[F.col(c).alias(f"__ft_{c}") for c in feature_cols])
    ft_time = f"__ft_{event_time_col}"

    candidates = (
        spine.withColumn(row_id, F.monotonically_increasing_id())
        .join(ft, on=on, how="left")
        .withColumn(
            "_qualifies",
            F.coalesce(F.col(ft_time) < F.col(spine_time_col), F.lit(False)),
        )
    )
    window = Window.partitionBy(row_id).orderBy(
        F.col("_qualifies").desc(), F.col(ft_time).desc_nulls_last()
    )
    ranked = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop(row_id, "_rn")
    )

    result = ranked
    for c in feature_cols:
        prefixed = f"__ft_{c}"
        result = result.withColumn(
            prefixed, F.when(F.col("_qualifies"), F.col(prefixed)).otherwise(F.lit(None))
        )
        if c == event_time_col:
            result = result.drop(prefixed)
        else:
            result = result.withColumnRenamed(prefixed, c)
    return result.drop("_qualifies")
```

- [ ] **Step 8: Run all three tests, and the full file**

Run: `uv run pytest tests/unit/test_features_join.py -v`
Expected: PASS (3 passed) — ordinary, boundary, and cold-start all hold
together.

- [ ] **Step 9: Commit**

```bash
git add src/almanac/features/join.py tests/unit/test_features_join.py
git commit -m "feat: the as-of join, correct on the boundary and on cold start"
```

---

### Task 3: The bot classifier and the v1 feature groups

**Files:**
- Create: `src/almanac/features/bot.py`
- Create: `src/almanac/features/groups.py`
- Test: `tests/unit/test_features_bot.py`
- Test: `tests/unit/test_features_groups.py`

**Interfaces:**
- Consumes: nothing beyond a raw Silver `events` DataFrame (same shape as
  Task 1's fixture schema).
- Produces:
  - `is_bot_column(login: Column) -> Column` (boolean, nullable)
  - `compute_author_activity(events: DataFrame) -> DataFrame` — columns
    `author_login: string, event_time: timestamp, prior_pr_count: long, prior_merge_rate: double`
  - `compute_repo_activity(events: DataFrame) -> DataFrame` — columns
    `repo_id: long, event_time: timestamp, events_total_to_date: long, bot_events_to_date: long, prs_opened_to_date: long, bot_share_to_date: double`
  - `compute_pr_static(events: DataFrame) -> DataFrame` — columns
    `repo_id: long, pr_number: long, is_draft: boolean, is_bot_author: boolean, opened_day_of_week: int, opened_hour: int`

  Task 4 (`assemble.py`) calls all three `compute_*` functions and joins
  their output through `as_of_join` (the two temporal groups) or a plain
  join (`pr_static`) — these exact column names and types are what it
  relies on.

**A stated v1 scope cut.** The approved design (§4.4a) named "prior PR
count, merge rate, and average response latency" for `author_activity`.
Response latency is cut from v1: computing it correctly needs the same
multi-event-type classification `int_pr_events.sql` already carries
(opened/closed/review/review-comment/issue-comment, with the legacy
`is_pr_comment`-null fallback) — duplicating ~150 lines of that logic on
the Features side for one signal is disproportionate for a v1 whose scope
is deliberately minimal (§4.4a: "not a speculative general framework").
`prior_pr_count` and `prior_merge_rate` are both kept, in full — the
worked example below is what `prior_merge_rate` actually has to get
right.

**The subtlety `prior_merge_rate` cannot skip.** A prior PR that has not
closed yet by the time a new PR opens is not "not merged" — its outcome is
unknown, and treating an unknown as a zero teaches the model something
that was not true at the time. Worked example: alice opens PR1 on day 1;
it merges on day 3. Alice opens PR2 on day 5 — `prior_pr_count = 1`,
`prior_merge_rate = 1.0`. Alice opens PR3 on day 6, *before* PR2 has
closed. PR3's `prior_pr_count` must be **1**, not 2, and
`prior_merge_rate` must still be **1.0** (from PR1 only) — PR2 cannot
contribute an outcome it does not have yet. This needs a self-join: a
prior PR counts as "known" only if `prior.closed_at < this_pr.opened_at`,
not merely `prior.opened_at < this_pr.opened_at`.

- [ ] **Step 1: Write the failing test for `is_bot_column`**

```python
# tests/unit/test_features_bot.py
"""is_bot_column: the Spark-native mirror of explore.measure.classify_bot
and the almanac_is_bot dbt macro. All three must agree -- test_bot_macro.py
already proves the Python regex and the SQL macro do; this proves the
PySpark column expression is the same rule a third way.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from almanac.features.bot import is_bot_column

pytestmark = pytest.mark.spark


def test_agrees_with_classify_bot_on_known_cases(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("dependabot[bot]",), ("renovate",), ("robotframework",), ("Abbott",), ("alice",)],
        "login string",
    )
    result = df.withColumn("is_bot", is_bot_column(F.col("login"))).collect()
    flags = {r["login"]: r["is_bot"] for r in result}

    assert flags["dependabot[bot]"] is True
    assert flags["renovate"] is True
    assert flags["robotframework"] is False  # documented false-positive case (§12 trap 6 origin)
    assert flags["Abbott"] is False
    assert flags["alice"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_features_bot.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.bot'`

- [ ] **Step 3: Write the implementation**

```python
# src/almanac/features/bot.py
"""The bot heuristic as a Spark column expression -- the third of three
required-identical restatements alongside explore.measure.classify_bot
(Python) and the almanac_is_bot dbt macro (SQL). Reuses the same pattern
object rather than a fourth hand-copied regex string; test_bot_macro.py
already proves Java `rlike` and Python `re` agree on this exact pattern.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from almanac.explore.measure import BOT_REGEX, CURATED_BOTS


def is_bot_column(login: Column) -> Column:
    return (
        login.endswith("[bot]")
        | F.lower(login).isin(*CURATED_BOTS)
        | login.rlike(BOT_REGEX.pattern)
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_features_bot.py -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for `compute_repo_activity` and `compute_pr_static`**

```python
# tests/unit/test_features_groups.py (part 1 of 2 -- author_activity follows in Step 7)
"""compute_repo_activity / compute_pr_static: cumulative-to-date repo
signals and PR-open-time-known attributes, both Silver-native (§3.1 --
never a read of agg_repo_daily or fact_pull_request).
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.groups import compute_pr_static, compute_repo_activity

pytestmark = pytest.mark.spark

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


def _row(repo_id, pr_number, created_at, event_type, action=None, actor=None, merged=None, draft=None):
    return (repo_id, pr_number, created_at, event_type, action, actor, merged, draft, None,
            created_at)


def test_repo_activity_is_cumulative_and_keyed_on_the_events_own_timestamp(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            _row(1, None, datetime(2025, 8, 10, tzinfo=UTC), "WatchEvent", actor="alice"),
            _row(1, None, datetime(2025, 8, 11, tzinfo=UTC), "WatchEvent", actor="dependabot[bot]"),
            _row(1, 5, datetime(2025, 8, 12, tzinfo=UTC), "PullRequestEvent", "opened", "bob"),
        ],
        _SCHEMA,
    )

    result = {r["event_time"]: r for r in compute_repo_activity(events).collect()}

    day1 = result[datetime(2025, 8, 10, tzinfo=UTC)]
    assert (day1["events_total_to_date"], day1["bot_events_to_date"], day1["prs_opened_to_date"]) == (1, 0, 0)

    day3 = result[datetime(2025, 8, 12, tzinfo=UTC)]
    assert (day3["events_total_to_date"], day3["bot_events_to_date"], day3["prs_opened_to_date"]) == (3, 1, 1)
    assert day3["bot_share_to_date"] == pytest.approx(1 / 3)


def test_pr_static_carries_open_time_attributes_with_no_temporal_join(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [_row(1, 5, datetime(2025, 8, 12, tzinfo=UTC), "PullRequestEvent", "opened", "dependabot[bot]", draft=True)],
        _SCHEMA,
    )

    result = compute_pr_static(events).collect()

    assert len(result) == 1
    assert result[0]["is_draft"] is True
    assert result[0]["is_bot_author"] is True
    assert result[0]["opened_hour"] == 0
```

- [ ] **Step 6: Run test to verify it fails, then implement `compute_repo_activity` and `compute_pr_static`**

Run: `uv run pytest tests/unit/test_features_groups.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.groups'`

```python
# src/almanac/features/groups.py (part 1 of 2 -- compute_author_activity in Step 8)
"""The v1 feature groups, each Silver-native (§3.1). `author_activity`
and `repo_activity` are event logs an as-of join reads through; `pr_static`
is already known at PR-open time and is joined directly (assemble.py),
never through as_of_join.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from almanac.features.bot import is_bot_column

_OPENED = (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "opened")
_CLOSED = (F.col("event_type") == "PullRequestEvent") & (F.col("event_action") == "closed")


def compute_repo_activity(events: DataFrame) -> DataFrame:
    """One row per Silver event carrying a repo_id: that repo's cumulative
    event volume, bot share, and PR-open count, inclusive of this event's
    own timestamp -- `as_of_join`'s strict `<` is what excludes an event
    from seeing its own contribution when it is itself a spine cutoff.
    """
    window = (
        Window.partitionBy("repo_id")
        .orderBy("created_at")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    scoped = events.where(F.col("repo_id").isNotNull()).withColumn(
        "_is_bot", F.coalesce(is_bot_column(F.col("actor_login")), F.lit(False)).cast("int")
    ).withColumn("_is_pr_open", _OPENED.cast("int"))

    events_total = F.count(F.lit(1)).over(window)
    bot_events = F.sum("_is_bot").over(window)
    prs_opened = F.sum("_is_pr_open").over(window)

    return scoped.select(
        "repo_id",
        F.col("created_at").alias("event_time"),
        events_total.alias("events_total_to_date"),
        bot_events.alias("bot_events_to_date"),
        prs_opened.alias("prs_opened_to_date"),
        (bot_events / events_total).alias("bot_share_to_date"),
    )


def compute_pr_static(events: DataFrame) -> DataFrame:
    """Attributes already known the instant a PR opens -- assemble.py
    joins this on (repo_id, pr_number) directly, not through as_of_join,
    since there is nothing temporal to look up.
    """
    return events.where(_OPENED).select(
        "repo_id",
        "pr_number",
        F.col("pr_draft").alias("is_draft"),
        is_bot_column(F.col("actor_login")).alias("is_bot_author"),
        F.dayofweek("created_at").alias("opened_day_of_week"),
        F.hour("created_at").alias("opened_hour"),
    )
```

- [ ] **Step 7: Run to verify both new tests pass**

Run: `uv run pytest tests/unit/test_features_groups.py -v`
Expected: PASS (2 passed)

- [ ] **Step 8: Write the failing test for `compute_author_activity` — including the "unknown, not zero" case**

```python
# tests/unit/test_features_groups.py (append -- part 2 of 2)
def test_author_activity_prior_pr_count_and_merge_rate(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            # PR1: alice opens day 1, merges day 3.
            _row(1, 1, datetime(2025, 8, 1, tzinfo=UTC), "PullRequestEvent", "opened", "alice"),
            _row(1, 1, datetime(2025, 8, 3, tzinfo=UTC), "PullRequestEvent", "closed", "alice", merged=True),
            # PR2: alice opens day 5, still open.
            _row(1, 2, datetime(2025, 8, 5, tzinfo=UTC), "PullRequestEvent", "opened", "alice"),
            # PR3: alice opens day 6 -- PR2 has not closed yet.
            _row(1, 3, datetime(2025, 8, 6, tzinfo=UTC), "PullRequestEvent", "opened", "alice"),
        ],
        _SCHEMA,
    )

    result = {
        r["event_time"]: r for r in compute_author_activity(events).collect()
    }

    pr2 = result[datetime(2025, 8, 5, tzinfo=UTC)]
    assert (pr2["prior_pr_count"], pr2["prior_merge_rate"]) == (1, 1.0)

    pr3 = result[datetime(2025, 8, 6, tzinfo=UTC)]
    # PR2 opened before PR3 but has not closed -- its outcome is unknown,
    # not a non-merge, so it must not appear in either the count or the rate.
    assert (pr3["prior_pr_count"], pr3["prior_merge_rate"]) == (1, 1.0)
```

Also add the import to the top of `tests/unit/test_features_groups.py`:
`from almanac.features.groups import compute_author_activity, compute_pr_static, compute_repo_activity`

- [ ] **Step 9: Run to verify it fails, then implement `compute_author_activity`**

Run: `uv run pytest tests/unit/test_features_groups.py -v`
Expected: FAIL — `ImportError: cannot import name 'compute_author_activity'`

```python
# src/almanac/features/groups.py -- append compute_author_activity
def compute_author_activity(events: DataFrame) -> DataFrame:
    """One row per PR ever opened: the author's prior PR count and merge
    rate, as known strictly before this PR's own open time.

    A prior PR counts as "known" only if it had already closed before this
    PR opened (`prior.closed_at < this.opened_at`) -- an open or
    not-yet-observed prior PR is not "not merged", it is unknown, and
    folding it into the denominator would teach the model an outcome it
    could not have had. See this task's worked example.
    """
    opened = events.where(_OPENED).select(
        "repo_id",
        "pr_number",
        F.col("actor_login").alias("author_login"),
        F.col("created_at").alias("opened_at"),
    )
    closed = events.where(_CLOSED).select(
        "repo_id",
        "pr_number",
        F.col("created_at").alias("closed_at"),
        F.col("pr_merged").alias("merged"),
    )
    prs = opened.join(closed, on=["repo_id", "pr_number"], how="left")

    this, prior = prs.alias("this"), prs.alias("prior")
    known_priors = this.join(
        prior,
        on=(
            (F.col("this.author_login") == F.col("prior.author_login"))
            & (F.col("prior.opened_at") < F.col("this.opened_at"))
            & F.col("prior.closed_at").isNotNull()
            & (F.col("prior.closed_at") < F.col("this.opened_at"))
        ),
        how="left",
    )

    return known_priors.groupBy(
        F.col("this.author_login").alias("author_login"),
        F.col("this.opened_at").alias("event_time"),
    ).agg(
        F.count(F.col("prior.pr_number")).alias("prior_pr_count"),
        F.avg(
            F.when(F.col("prior.pr_number").isNotNull(), F.when(F.col("prior.merged"), 1.0).otherwise(0.0))
        ).alias("prior_merge_rate"),
    )
```

- [ ] **Step 10: Run the full test file**

Run: `uv run pytest tests/unit/test_features_groups.py tests/unit/test_features_bot.py -v`
Expected: PASS (3 passed)

- [ ] **Step 11: Commit**

```bash
git add src/almanac/features/bot.py src/almanac/features/groups.py \
        tests/unit/test_features_bot.py tests/unit/test_features_groups.py
git commit -m "feat: v1 feature groups -- author activity, repo activity, PR-static"
```

---

### Task 4: Assembling the training set

**Files:**
- Create: `src/almanac/features/assemble.py`
- Test: `tests/unit/test_features_assemble.py`

**Interfaces:**
- Consumes: `as_of_join` (Task 2), `build_pr_opened_spine` (Task 1),
  `compute_author_activity` / `compute_repo_activity` / `compute_pr_static`
  (Task 3).
- Produces: `assemble_training_set(spine: DataFrame, *, author_activity: DataFrame, repo_activity: DataFrame, pr_static: DataFrame) -> DataFrame`.
  This is what Phase 4 will call to build one point-in-time-correct
  training set; the leakage suite (Task 7) also builds on it.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_features_assemble.py
"""assemble_training_set: chains as_of_join across both temporal groups
and joins pr_static directly, producing one wide, point-in-time-correct
frame -- the "as_of demo" §9's Phase 3 gate names.
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.features.assemble import assemble_training_set

pytestmark = pytest.mark.spark


def test_assembles_one_row_per_spine_entry_with_every_group_joined(spark: SparkSession) -> None:
    spine = spark.createDataFrame(
        [(1, 5, "alice", datetime(2025, 8, 13, tzinfo=UTC))],
        "repo_id long, pr_number long, author_login string, as_of_timestamp timestamp",
    )
    author_activity = spark.createDataFrame(
        [("alice", datetime(2025, 8, 10, tzinfo=UTC), 3, 0.5)],
        "author_login string, event_time timestamp, prior_pr_count long, prior_merge_rate double",
    )
    repo_activity = spark.createDataFrame(
        [(1, datetime(2025, 8, 12, tzinfo=UTC), 10, 1, 2, 0.1)],
        "repo_id long, event_time timestamp, events_total_to_date long, bot_events_to_date long, "
        "prs_opened_to_date long, bot_share_to_date double",
    )
    pr_static = spark.createDataFrame(
        [(1, 5, False, False, 4, 9)],
        "repo_id long, pr_number long, is_draft boolean, is_bot_author boolean, "
        "opened_day_of_week int, opened_hour int",
    )

    result = assemble_training_set(
        spine, author_activity=author_activity, repo_activity=repo_activity, pr_static=pr_static
    ).collect()

    assert len(result) == 1
    row = result[0]
    assert (row["prior_pr_count"], row["prior_merge_rate"]) == (3, 0.5)
    assert row["events_total_to_date"] == 10
    assert row["is_draft"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_features_assemble.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.assemble'`

- [ ] **Step 3: Write the implementation**

```python
# src/almanac/features/assemble.py
"""One wide, point-in-time-correct training frame: the as_of demo §9's
Phase 3 gate names. `pr_static` is joined directly -- it is not a
temporal lookup, so routing it through as_of_join would only add an
unnecessary time comparison against a spine row's own PR.
"""

from pyspark.sql import DataFrame

from almanac.features.join import as_of_join


def assemble_training_set(
    spine: DataFrame,
    *,
    author_activity: DataFrame,
    repo_activity: DataFrame,
    pr_static: DataFrame,
) -> DataFrame:
    result = as_of_join(spine, author_activity, on=["author_login"])
    result = as_of_join(result, repo_activity, on=["repo_id"])
    return result.join(pr_static, on=["repo_id", "pr_number"], how="left")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_features_assemble.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/almanac/features/assemble.py tests/unit/test_features_assemble.py
git commit -m "feat: assemble one point-in-time-correct training set from the v1 groups"
```

---

### Task 5: Unity Catalog registration SQL

**Files:**
- Create: `src/almanac/features/registration.py`
- Test: `tests/unit/test_features_registration.py`

**Interfaces:**
- Consumes: `almanac.gold.sources.table_location` (existing).
- Produces:
  - `register_feature_table(spark: SparkSession, *, table: str, path: str, schema: str = "features") -> None`
  - `primary_key_sql(*, schema: str, table: str, entity_cols: list[str], event_time_col: str | None = None) -> tuple[str, str]`
    — returns `(drop_if_exists_sql, add_constraint_sql)`.

  Task 6's runner calls both; no other task depends on this one.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_features_registration.py
"""primary_key_sql: pure SQL-string building for the UC TIMESERIES
constraint. Execution against a real Unity Catalog metastore is verified
live during Phase 3's cloud burn (design doc §4.4a) -- the local Derby
metastore this repo's other tests run against does not support
TIMESERIES, so nothing here calls spark.sql.
"""

from almanac.features.registration import primary_key_sql


def test_a_temporal_table_gets_a_timeseries_primary_key() -> None:
    drop_sql, add_sql = primary_key_sql(
        schema="features", table="author_activity", entity_cols=["author_login"], event_time_col="event_time"
    )
    assert drop_sql == "ALTER TABLE features.author_activity DROP CONSTRAINT IF EXISTS author_activity_pk"
    assert add_sql == (
        "ALTER TABLE features.author_activity ADD CONSTRAINT author_activity_pk "
        "PRIMARY KEY (author_login, event_time TIMESERIES)"
    )


def test_a_non_temporal_table_gets_a_plain_composite_primary_key() -> None:
    """pr_static carries no event_time -- registering it as TIMESERIES
    would misrepresent it as a temporal lookup table it is not.
    """
    _, add_sql = primary_key_sql(schema="features", table="pr_static", entity_cols=["repo_id", "pr_number"])
    assert add_sql == (
        "ALTER TABLE features.pr_static ADD CONSTRAINT pr_static_pk PRIMARY KEY (repo_id, pr_number)"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_features_registration.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.registration'`

- [ ] **Step 3: Write the implementation**

```python
# src/almanac/features/registration.py
"""Unity Catalog registration for feature tables -- governance metadata
only, never the correctness mechanism (design doc §4.4a: the as-of join
in join.py owns correctness; this module owns nothing but a CREATE TABLE
and a PRIMARY KEY constraint).
"""

from pyspark.sql import SparkSession

from almanac.gold.sources import table_location


def register_feature_table(spark: SparkSession, *, table: str, path: str, schema: str = "features") -> None:
    location = table_location(path)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} USING DELTA LOCATION '{location}'")


def primary_key_sql(
    *, schema: str, table: str, entity_cols: list[str], event_time_col: str | None = None
) -> tuple[str, str]:
    """(drop_if_exists_sql, add_constraint_sql). Dropped before being
    re-added on every run, since Task 6's runner fully overwrites each
    table's data on every call -- without the drop, a second run's ADD
    CONSTRAINT would collide with the first run's still-live constraint.
    """
    constraint = f"{table}_pk"
    keys = list(entity_cols)
    if event_time_col is not None:
        keys.append(f"{event_time_col} TIMESERIES")
    drop_sql = f"ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {constraint}"
    add_sql = f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint} PRIMARY KEY ({', '.join(keys)})"
    return drop_sql, add_sql
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_features_registration.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/almanac/features/registration.py tests/unit/test_features_registration.py
git commit -m "feat: UC TIMESERIES registration SQL, governance only"
```

---

### Task 6: The runner — the I/O boundary and CLI

**Files:**
- Create: `src/almanac/features/runner.py`
- Create: `scripts/features.py`
- Test: `tests/unit/test_features_runner_cli.py`

**Interfaces:**
- Consumes: `compute_author_activity` / `compute_repo_activity` /
  `compute_pr_static` (Task 3), `register_feature_table` / `primary_key_sql`
  (Task 5).
- Produces: `run_features(spark: SparkSession, *, silver_path: str, features_path: str, register: bool, schema: str = "features") -> None`,
  `_build_parser() -> argparse.ArgumentParser`, and
  `main(argv: list[str] | None = None) -> int`. Task 7's end-to-end
  integration test calls both `run_features` directly and `main()` for
  real, exercising the CLI wiring with no mocking — this codebase has no
  precedent for `unittest.mock`/`monkeypatch` anywhere (`GoldTarget.cli_flags()`
  and `RestSession`'s injectable `sleep`/`now` are both tested as real
  objects, not mocked), so this task doesn't start one.

- [ ] **Step 1: Write the failing test — argument parsing, as a pure unit test**

Following `tests/unit/test_gold_runner.py`'s precedent: test the parser's
own output directly, the same way `GoldTarget.cli_flags()` is tested,
rather than mocking what `main()` dispatches to.

```python
# tests/unit/test_features_runner_cli.py
"""_build_parser: argument defaults and required flags, as a pure unit
test. The CLI's real dispatch (main() calling run_features against a
live SparkSession) is tests/integration/test_features_runner.py's job --
argparse.Namespace needs no Spark and no mocking to test directly.
"""

import pytest

from almanac.features.runner import _build_parser


def test_register_defaults_false_and_schema_defaults_features() -> None:
    args = _build_parser().parse_args(["--silver-path", "/s", "--features-path", "/f"])

    assert args.register is False
    assert args.schema == "features"


def test_register_flag_and_explicit_schema_are_both_honored() -> None:
    args = _build_parser().parse_args(
        ["--silver-path", "/s", "--features-path", "/f", "--register", "--schema", "custom"]
    )

    assert args.register is True
    assert args.schema == "custom"


def test_silver_path_and_features_path_are_required() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--features-path", "/f"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_features_runner_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.features.runner'`

- [ ] **Step 3: Write the implementation**

```python
# src/almanac/features/runner.py
"""Silver -> feature tables. The I/O boundary for the feature platform,
in the same shape as gold/runner.py and gold/sources.py's own split
between pure transforms and the one module that touches Spark I/O and
the metastore.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from almanac.cli import run_cli
from almanac.features.groups import compute_author_activity, compute_pr_static, compute_repo_activity
from almanac.features.registration import primary_key_sql, register_feature_table
from almanac.spark import local_session


@dataclass(frozen=True)
class FeatureTableSpec:
    name: str
    compute: Callable[[DataFrame], DataFrame]
    entity_cols: list[str]
    event_time_col: str | None


FEATURE_TABLES: list[FeatureTableSpec] = [
    FeatureTableSpec("author_activity", compute_author_activity, ["author_login"], "event_time"),
    FeatureTableSpec("repo_activity", compute_repo_activity, ["repo_id"], "event_time"),
    FeatureTableSpec("pr_static", compute_pr_static, ["repo_id", "pr_number"], None),
]


def _active_or_local_session() -> SparkSession:
    active = SparkSession.getActiveSession()
    return active if active is not None else local_session("almanac-features")


def run_features(
    spark: SparkSession, *, silver_path: str, features_path: str, register: bool, schema: str = "features"
) -> None:
    """Recompute every feature table from the whole of Silver, overwriting each.

    Full recompute, not incremental: v1's cumulative aggregates (running
    counts, author_activity's self-join merge rate) would need Gold's
    recompute-touched incremental-merge machinery to update correctly in
    place, and nothing here builds that yet -- stated in this plan's
    Deferred section, not silently accepted. Correct and fine at this
    project's data volume; the known cost is a full Silver scan per run.
    """
    events = spark.read.format("delta").load(f"{silver_path}/clean")
    for spec in FEATURE_TABLES:
        path = f"{features_path}/{spec.name}"
        spec.compute(events).write.format("delta").mode("overwrite").save(path)
        if register:
            register_feature_table(spark, table=spec.name, path=path, schema=schema)
            drop_sql, add_sql = primary_key_sql(
                schema=schema, table=spec.name, entity_cols=spec.entity_cols, event_time_col=spec.event_time_col
            )
            spark.sql(drop_sql)
            spark.sql(add_sql)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recompute the feature platform's tables from Silver.")
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--schema", default="features")
    parser.add_argument(
        "--register",
        action="store_true",
        help=(
            "Register each table's UC TIMESERIES constraint after writing. "
            "Needs a live Unity Catalog metastore -- the local Derby metastore "
            "does not support it, so this is off unless explicitly requested."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_features(
        _active_or_local_session(),
        silver_path=args.silver_path,
        features_path=args.features_path,
        register=args.register,
        schema=args.schema,
    )
    return 0


if __name__ == "__main__":
    run_cli(main)
```

```python
# scripts/features.py
"""Launch shim for the feature platform on a job cluster; the CLI itself is almanac.features.runner."""

from almanac.cli import run_cli
from almanac.features.runner import main

if __name__ == "__main__":
    run_cli(main)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_features_runner_cli.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/almanac/features/runner.py scripts/features.py tests/unit/test_features_runner_cli.py
git commit -m "feat: the feature-platform runner and CLI"
```

---

### Task 7: The leakage suite and the end-to-end runner test

This task tests two different things, both integration-level: the
runner's real Spark I/O end to end (with idempotency), and — the point of
this whole plan — CLAUDE.md's governing invariant, tested directly rather
than by convention.

**Files:**
- Create: `tests/integration/test_features_runner.py`
- Create: `tests/integration/test_features_leakage.py`

**Interfaces:**
- Consumes: `run_features` (Task 6), `as_of_join` (Task 2),
  `compute_repo_activity` (Task 3).

- [ ] **Step 1: Write the runner end-to-end + idempotency test**

```python
# tests/integration/test_features_runner.py
"""run_features against real Delta I/O: all three tables land, and a
second run overwrites rather than duplicates (full recompute is
idempotent by construction -- this is what proves it).
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.features.runner import run_features

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


def _write_silver(spark: SparkSession, path: Path) -> None:
    rows = [
        (1, 5, datetime(2025, 8, 13, 9, tzinfo=UTC), "PullRequestEvent", "opened", "alice",
         None, False, None, datetime(2025, 8, 13, 9, 5, tzinfo=UTC)),
    ]
    spark.createDataFrame(rows, _SCHEMA).write.format("delta").save(str(path / "clean"))


def test_run_features_writes_all_three_tables_and_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    _write_silver(spark, silver_path)

    run_features(spark, silver_path=str(silver_path), features_path=str(features_path), register=False)

    for name in ("author_activity", "repo_activity", "pr_static"):
        assert spark.read.format("delta").load(str(features_path / name)).count() >= 1

    first_pr_static = spark.read.format("delta").load(str(features_path / "pr_static")).collect()

    run_features(spark, silver_path=str(silver_path), features_path=str(features_path), register=False)
    second_pr_static = spark.read.format("delta").load(str(features_path / "pr_static")).collect()

    assert sorted(map(str, first_pr_static)) == sorted(map(str, second_pr_static))


def test_main_wires_the_parsed_arguments_through_to_a_real_run(spark: SparkSession, tmp_path: Path) -> None:
    """The CLI path, exercised for real -- `SparkSession.getActiveSession()`
    picks up this test's own session, so `main()` never falls back to
    building a new one, and no mocking is needed to prove the wiring works.
    """
    from almanac.features.runner import main

    silver_path = tmp_path / "silver"
    features_path = tmp_path / "features"
    _write_silver(spark, silver_path)

    code = main(
        ["--silver-path", str(silver_path), "--features-path", str(features_path)]
    )

    assert code == 0
    assert spark.read.format("delta").load(str(features_path / "author_activity")).count() >= 0
```

- [ ] **Step 2: Run to verify it passes** (no new production code needed —
  this exercises Tasks 1–6 together)

Run: `uv run pytest tests/integration/test_features_runner.py -v -m integration`
Expected: PASS

- [ ] **Step 3: Write the leakage suite — the CLAUDE.md invariant, directly**

```python
# tests/integration/test_features_leakage.py
"""The governing invariant, tested directly (CLAUDE.md): a feature vector
computed as-of T must be reproducible byte-for-byte from the same Delta
version, a year later -- even after more data, including a late-arriving
correction whose own event_time is before T, has since been appended.

This is not the same property as as_of_join's boundary test
(tests/unit/test_features_join.py): that test proves a single query never
lets a future row leak in. This test proves that recomputing a *specific,
already-built* training set later reproduces it exactly only if the Delta
version is pinned -- a live "filter on event_time" re-query is leak-free
but not reproducible, because more history can arrive between builds.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession

from almanac.features.groups import compute_repo_activity
from almanac.features.join import as_of_join

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)


def _event(repo_id: int, created_at: datetime, ingested_at: datetime, actor: str) -> tuple:
    return (repo_id, None, created_at, "WatchEvent", None, actor, None, None, None, ingested_at)


def _latest_version(spark: SparkSession, path: Path) -> int:
    return DeltaTable.forPath(spark, str(path)).history(1).select("version").collect()[0]["version"]


def test_pinning_the_delta_version_reproduces_the_original_result_after_a_late_arrival(
    spark: SparkSession, tmp_path: Path
) -> None:
    events_path = tmp_path / "events"
    t = datetime(2025, 8, 13, 12, tzinfo=UTC)

    spark.createDataFrame(
        [_event(1, datetime(2025, 8, 10, tzinfo=UTC), datetime(2025, 8, 10, 1, tzinfo=UTC), "alice")],
        _SCHEMA,
    ).write.format("delta").save(str(events_path))
    v1 = _latest_version(spark, events_path)

    spine = spark.createDataFrame(
        [(1, t)], "repo_id long, as_of_timestamp timestamp"
    )

    def as_of_result(version: int) -> list[Row]:
        events = spark.read.format("delta").option("versionAsOf", version).load(str(events_path))
        return as_of_join(spine, compute_repo_activity(events), on=["repo_id"]).collect()

    original = as_of_result(v1)
    assert original[0]["events_total_to_date"] == 1

    # A late-arriving correction: event_time is BEFORE t, but it is
    # ingested and appended as a real second Delta version well after the
    # original build -- not backdated in place.
    spark.createDataFrame(
        [_event(1, datetime(2025, 8, 11, tzinfo=UTC), datetime(2025, 8, 20, tzinfo=UTC), "carol")],
        _SCHEMA,
    ).write.format("delta").mode("append").save(str(events_path))
    v2 = _latest_version(spark, events_path)

    pinned = as_of_result(v1)
    live = as_of_result(v2)

    assert pinned == original  # reproducible: the pinned version is untouched by the append
    assert live[0]["events_total_to_date"] == 2  # the straggler is leak-free (event_time < t) but changes the "live" answer
    assert live != original  # which is exactly why the version gets pinned at build time, not re-derived from "current"
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/integration/test_features_leakage.py -v -m integration`
Expected: PASS (1 passed) — confirms `as_of_join` and `compute_repo_activity`
already satisfy the invariant; if it fails, the bug is in one of those two,
not in this test.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_features_runner.py tests/integration/test_features_leakage.py
git commit -m "test: the runner end to end, and CLAUDE.md's invariant tested directly"
```

---

### Task 8: Wrap-up — exit gate, README, STATUS.md

**Files:**
- Modify: `README.md` (architecture diagram)
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the full suite**

Run: `make check`
Expected: ruff, ruff format --check, mypy --strict, and the full pytest
suite (`-m "not network"`) all green, including every test this plan added.

- [ ] **Step 2: Update the README's Mermaid diagram**

Add a `FEATURE PLATFORM` node reading from Silver (parallel to the
existing `GOLD` node, per §3.1), marked `done` for `author_activity` /
`repo_activity` / `pr_static`, per CLAUDE.md's same-commit diagram rule.

- [ ] **Step 3: Add STATUS.md's verification-log row for this plan's execution**

One row stating what actually ran (`make check`'s real test count, not an
estimate) and the actual result — following the same convention as every
prior row in the file. Write this row from the real, executed output,
not from this plan's expected counts.

- [ ] **Step 4: Update STATUS.md's "Next" section**

Point it at Phase 4 (model + MLflow), and record that Phase 3's live UC
`TIMESERIES` registration is still unverified against a real Databricks
target — an explicit open item, the same way Phase 2's `terraform destroy`
was carried as an open item rather than silently marked done.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/STATUS.md
git commit -m "docs: Phase 3 exit gate — README diagram, STATUS.md verification row"
```

---

## Exit Gate

| Gate | How it's verified |
|---|---|
| `make check` green | Task 8, Step 1 — the whole suite, not just this plan's new tests |
| The as-of join is strict on the boundary | `test_a_row_timestamped_exactly_at_the_as_of_time_is_not_prior` (Task 2) |
| Cold start never drops a spine row | `test_no_qualifying_row_leaves_features_null_rather_than_dropping_the_spine_row` (Task 2) |
| An unclosed prior PR is "unknown", not "not merged" | `test_author_activity_prior_pr_count_and_merge_rate` (Task 3) |
| The feature platform reads Silver, never Gold | No import of `almanac.gold.runner` / `almanac.gold.sources` for data anywhere in `almanac/features/` — only `table_location`, reused for its path logic |
| CLAUDE.md's invariant, tested directly | `test_pinning_the_delta_version_reproduces_the_original_result_after_a_late_arrival` (Task 7) |
| The runner is idempotent | `test_run_features_writes_all_three_tables_and_is_idempotent` (Task 7) |
| UC registration SQL is correct | `test_features_registration.py` (Task 5) — pure string tests; **live execution against a real UC target is explicitly not claimed here** |
| README / STATUS.md updated same-commit | Task 8 |

## Deferred out of Phase 3, on purpose

- **The online store and the vector index** — §9 scopes both to Phase 4/5;
  no online infrastructure is stood up here (§4.4a, this plan's Global
  Constraints).
- **`author_activity`'s "average response latency"** — cut from v1; it
  would duplicate `int_pr_events.sql`'s multi-event-type classification
  for one signal. A natural v2 addition once the model shows it needs it.
- **`repo_activity`'s trailing-N-day windows** — v1 uses cumulative-to-date
  (a full-history running count) instead, which is simpler and still real.
  A true rolling window needs a range-based window spec (`rangeBetween` on
  a time-cast column); deferred as a richness upgrade.
- **Incremental feature-table builds** — v1 fully recomputes and overwrites
  every table on every run. Correct, but does not reuse Gold's
  recompute-touched incremental-merge machinery; a scaling concern only
  once Silver's volume makes a full rescan expensive, not yet measured to
  be a problem.
- **Live UC `TIMESERIES` registration against a real Databricks target** —
  the SQL is unit-tested as pure string-building (Task 5); executing it for
  real happens during Phase 3's cloud verification step, the same
  Terraform/cloud-gated pattern Photon A/B and the backfill already used,
  and is recorded in STATUS.md when it runs, not claimed here.
- **`assemble_training_set` as its own CLI subcommand** for a specific
  `--as-of-date` cutoff — it stays a library function; Phase 4 owns
  training-set-build timing and calls it directly.
