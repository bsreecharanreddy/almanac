# Phase 0 — Exploration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every assumption the design rests on with a measurement
taken against real GH Archive data, and stand up the tooling the rest of
the project builds on — before any pipeline code exists to be invalidated.

**Architecture:** A `uv`-managed Python package (`almanac`) with pure
functions separated from I/O from the very first file. Local Spark runs in
a single container at `local[*]` with Delta enabled. Exploration outputs
are committed findings documents carrying real numbers, not notebooks
whose outputs rot.

**Tech Stack:** Python 3.12+, `uv`, PySpark, `delta-spark`, `pytest`,
`chispa`, `ruff`, `mypy --strict`, Terraform, GitHub Actions.

**Spec:** `docs/design/2026-09-01-almanac-system-design.md`

## Global Constraints

Copied verbatim from the spec and `CLAUDE.md`. Every task's requirements
implicitly include these.

- **Never quote a number that was not measured.** Not file sizes, row
  counts, latencies, or costs. Applies to code comments, findings docs,
  the README, and commit messages.
- **Pure transforms are separated from I/O.** `DataFrame in, DataFrame
  out`; no reads or writes inside transformation functions.
- **`ingested_at` is never conflated with `created_at`.**
- **Spark session timezone is UTC, always.** Event-time correctness
  depends on it.
- **Actor identities are pseudonymized in anything published.** Repos stay
  named; people do not.
- **Raw archive data is never committed.** `.gitignore` already excludes
  `data/` and `*.json.gz`. Only sub-sampled fixtures under
  `tests/fixtures/` may be committed, and only if under 5 MB each.
- **`docs/STATUS.md` updates in the same commit as the work it
  describes.**
- **One commit per completed task**, each with its own green test run.
- Python **3.12+**. `mypy --strict` must pass. `ruff` must pass.
- **If reality contradicts the design doc, reality wins** — correct the
  design doc in the same commit and note it in the STATUS verification
  log.

## File structure created by this phase

| File | Responsibility |
|---|---|
| `pyproject.toml` | Package metadata, deps, ruff/mypy/pytest config |
| `Makefile` | `make test`, `make lint`, `make fixtures` — one entry point each |
| `docker/Dockerfile.spark` | Reproducible local Spark; pinned deps, not `pip install` at runtime |
| `src/almanac/__init__.py` | Package marker |
| `src/almanac/config.py` | Pydantic settings; paths, base URL, timeouts |
| `src/almanac/spark.py` | `SparkSession` construction. Only place Spark is configured |
| `src/almanac/extract/urls.py` | **Pure.** Archive URL construction from date+hour |
| `src/almanac/extract/outcome.py` | `FetchStatus` / `FetchResult` — the absent/empty/failed vocabulary |
| `src/almanac/extract/archive.py` | I/O edge: fetching, retry, classification |
| `src/almanac/explore/schema.py` | **Pure.** Schema-era classification and field-set diffing |
| `src/almanac/explore/measure.py` | **Pure.** Counters over parsed events |
| `tests/conftest.py` | Session-scoped Spark fixture, fixture-path helpers |
| `tests/fixtures/` | Tier 0 committed sub-samples |
| `docs/findings/2026-09-XX-schema-eras.md` | Measured 2014↔2025 diff |
| `docs/findings/2026-09-XX-dataset-measurements.md` | Measured volumes, dup rate, renames, bots |
| `docs/findings/2026-09-XX-embedding-cost.md` | Measured embedding throughput and projection |
| `docs/findings/2026-09-XX-azure-pricing.md` | Verified DBU/VM prices, UC Premium delta |
| `infra/terraform/` | Azure resource group, ADLS, Databricks workspace. Clusters off |
| `.github/workflows/ci.yml` | Lint + typecheck + test on every push |

---

## Task 1: Project scaffold, tooling, and CI

**Files:**
- Create: `pyproject.toml`, `Makefile`, `.python-version`
- Create: `src/almanac/__init__.py`, `src/almanac/config.py`
- Create: `tests/conftest.py`, `tests/unit/test_config.py`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: nothing
- Produces: `almanac.config.Settings` with fields `archive_base_url: str`,
  `data_dir: Path`, `fixture_dir: Path`, `http_timeout_seconds: float`,
  `max_fetch_attempts: int`. Every later task imports `Settings`.

- [ ] **Step 1: Verify current tool versions live — do not assume them**

```bash
uv --version
curl -s https://pypi.org/pypi/pyspark/json      | python3 -c "import sys,json;print('pyspark',      json.load(sys.stdin)['info']['version'])"
curl -s https://pypi.org/pypi/delta-spark/json  | python3 -c "import sys,json;print('delta-spark',  json.load(sys.stdin)['info']['version'])"
curl -s https://pypi.org/pypi/chispa/json       | python3 -c "import sys,json;print('chispa',       json.load(sys.stdin)['info']['version'])"
curl -s https://pypi.org/pypi/ruff/json         | python3 -c "import sys,json;print('ruff',         json.load(sys.stdin)['info']['version'])"
curl -s https://pypi.org/pypi/mypy/json         | python3 -c "import sys,json;print('mypy',         json.load(sys.stdin)['info']['version'])"
```

Record the printed versions. Use them as the `>=` floors below. **Do not
copy version numbers from this plan** — it was written on 2026-09-01 and
the point of this step is that they change.

**Critical compatibility note:** `delta-spark` releases are pinned to
specific PySpark minor versions. Check the `delta-spark` PyPI page's
stated PySpark requirement and pick a compatible pair. A mismatch fails at
`SparkSession` creation, not at install time.

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[project]
name = "almanac"
version = "0.1.0"
description = "An ML platform for work-queue risk, built on the GitHub event firehose"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "httpx>=0.28",
    "pydantic>=2.13",
    "pydantic-settings>=2.15",
]

[project.optional-dependencies]
spark = [
    "pyspark>=VERSION_FROM_STEP_1",
    "delta-spark>=VERSION_FROM_STEP_1",
]

[build-system]
requires = ["uv_build>=0.12,<0.13"]
build-backend = "uv_build"

[dependency-groups]
dev = [
    "chispa>=VERSION_FROM_STEP_1",
    "mypy>=VERSION_FROM_STEP_1",
    "pytest>=8",
    "pytest-cov>=7",
    "ruff>=VERSION_FROM_STEP_1",
]

[tool.ruff]
line-length = 100
target-version = "py312"
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "N", "UP", "B", "SIM", "PL", "RUF", "C90"]

[tool.ruff.lint.mccabe]
max-complexity = 10

[tool.ruff.lint.per-file-ignores]
"tests/**/*.py" = ["PLR2004"]

[tool.mypy]
python_version = "3.12"
strict = true
warn_unreachable = true
mypy_path = "src"

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
  "spark: needs a SparkSession (slow)",
  "network: hits data.gharchive.org (slow, excluded from CI)",
]
```

- [ ] **Step 3: Write the failing test**

```python
# tests/unit/test_config.py
from pathlib import Path

from almanac.config import Settings


def test_settings_have_archive_base_url() -> None:
    s = Settings()
    assert s.archive_base_url == "https://data.gharchive.org"


def test_settings_paths_are_paths() -> None:
    s = Settings()
    assert isinstance(s.data_dir, Path)
    assert isinstance(s.fixture_dir, Path)


def test_fetch_attempts_is_at_least_one() -> None:
    # A zero here would silently disable fetching altogether.
    assert Settings().max_fetch_attempts >= 1
```

- [ ] **Step 4: Run it and confirm it fails for the right reason**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almanac.config'`

- [ ] **Step 5: Write the minimal implementation**

```python
# src/almanac/config.py
"""Runtime settings. The only place defaults live."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Environment-overridable settings, prefix ALMANAC_."""

    model_config = SettingsConfigDict(env_prefix="ALMANAC_", frozen=True)

    archive_base_url: str = "https://data.gharchive.org"
    data_dir: Path = _REPO_ROOT / "data"
    fixture_dir: Path = _REPO_ROOT / "tests" / "fixtures"
    http_timeout_seconds: float = 60.0
    max_fetch_attempts: int = 3
```

```python
# src/almanac/__init__.py
"""Almanac — an ML platform for work-queue risk."""
```

- [ ] **Step 6: Run tests and lint**

```bash
uv run pytest tests/unit/test_config.py -v
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests
```
Expected: tests PASS, all three checks clean.

- [ ] **Step 7: Write the Makefile**

```makefile
.PHONY: test lint fmt typecheck check fixtures

test:
	uv run pytest -m "not network" -v

test-all:
	uv run pytest -v

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy src tests

check: lint typecheck test
```

- [ ] **Step 8: Write the CI workflow**

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      # Verify both action versions are current before committing:
      #   gh api repos/actions/checkout/releases/latest --jq .tag_name
      #   gh api repos/astral-sh/setup-uv/releases/latest --jq .tag_name
      # Pin to whatever those return, not to what this plan happens to say.
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install 3.12
      - run: uv sync --all-extras --dev
      # -m "not network" keeps CI off data.gharchive.org: a third-party
      # host outage must never turn this build red.
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run mypy src tests
      - run: uv run pytest -m "not network" -v
```

- [ ] **Step 9: Verify `make check` passes end to end**

Run: `make check`
Expected: all green. If `mypy --strict` complains about untyped
`pydantic_settings`, add `pydantic-settings` stubs or a targeted
`[[tool.mypy.overrides]]` — **do not** loosen `strict`.

- [ ] **Step 10: Update STATUS.md and commit**

Add a verification-log row stating the tool versions actually installed
and that `make check` passed.

```bash
git add pyproject.toml Makefile .python-version src tests .github docs/STATUS.md
git commit -m "feat: project scaffold, tooling, and CI

uv-managed package with ruff, mypy --strict and pytest wired into a
single make check entry point, plus a CI job that runs all three.

Network-marked tests are excluded from CI so a third-party host outage
cannot turn the build red."
```

---

## Task 2: Local Spark session with Delta

**Files:**
- Create: `src/almanac/spark.py`, `docker/Dockerfile.spark`
- Modify: `tests/conftest.py`
- Test: `tests/unit/test_spark_session.py`

**Interfaces:**
- Consumes: `almanac.config.Settings`
- Produces: `almanac.spark.local_session(app_name: str = "almanac") ->
  SparkSession`. A session-scoped `spark` pytest fixture in `conftest.py`
  that every later Spark test uses.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_spark_session.py
import pytest
from pyspark.sql import SparkSession


@pytest.mark.spark
def test_session_timezone_is_utc(spark: SparkSession) -> None:
    # Non-UTC silently shifts every event timestamp, which would corrupt
    # point-in-time correctness in a way no test downstream would catch.
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


@pytest.mark.spark
def test_delta_round_trip(spark: SparkSession, tmp_path) -> None:
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string") \
        .write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 2


@pytest.mark.spark
def test_delta_replace_where_is_idempotent(spark: SparkSession, tmp_path) -> None:
    # The write mode Bronze depends on. Proving it here means Phase 1 can
    # rely on it rather than rediscovering it.
    path = str(tmp_path / "t")
    df = spark.createDataFrame([(1, "2025-01-01"), (2, "2025-01-02")], "id int, d string")
    df.write.format("delta").partitionBy("d").save(path)

    again = spark.createDataFrame([(1, "2025-01-01")], "id int, d string")
    for _ in range(2):
        again.write.format("delta").mode("overwrite") \
            .option("replaceWhere", "d = '2025-01-01'").save(path)

    rows = {(r.id, r.d) for r in spark.read.format("delta").load(path).collect()}
    assert rows == {(1, "2025-01-01"), (2, "2025-01-02")}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/unit/test_spark_session.py -v`
Expected: FAIL — fixture `spark` not found.

- [ ] **Step 3: Implement the session builder**

```python
# src/almanac/spark.py
"""SparkSession construction. The only place Spark is configured."""

from pyspark.sql import SparkSession


def local_session(app_name: str = "almanac") -> SparkSession:
    """A local-mode SparkSession with Delta enabled.

    Deliberately local[*] and not a Compose master/worker cluster: at this
    data volume a real cluster is slower and costs days on executor OOMs
    that teach nothing (design doc section 8).
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Non-negotiable: event-time correctness depends on it.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
```

- [ ] **Step 4: Add the session-scoped fixture**

```python
# tests/conftest.py
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from almanac.spark import local_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One session for the whole test run. Creating one per test is the
    single most common reason a Spark suite becomes unusably slow."""
    session = local_session("almanac-tests")
    yield session
    session.stop()
```

- [ ] **Step 5: Run and confirm green**

Run: `uv run pytest tests/unit/test_spark_session.py -v`
Expected: 3 PASS. First run downloads Delta JARs — slow once, cached after.

- [ ] **Step 6: Confirm `array_compact` exists in this Spark version**

Design doc §13 lists this as unverified, and Phase 1's quality-rule
engine depends on it to build the `_failed_rules` array.

```bash
uv run python -c "
from almanac.spark import local_session
s = local_session('probe')
r = s.sql(\"SELECT array_compact(array('a', NULL, 'b')) AS c\").collect()[0]['c']
print('array_compact ->', r)
assert r == ['a', 'b'], r
s.stop()
"
```
Expected: `array_compact -> ['a', 'b']`.

**If it raises `UNRESOLVED_ROUTINE`**, the installed Spark predates it.
Record that in the STATUS verification log and note in design doc §13
that Phase 1's rule engine must use
`filter(array(...), x -> x IS NOT NULL)` instead. Do not silently work
around it — Phase 1 needs to know which form to write.

- [ ] **Step 7: Write the Dockerfile**

```dockerfile
# docker/Dockerfile.spark
FROM python:3.12-slim

# Spark needs a JVM. Pin the JDK major version: an unpinned default has
# changed under this image before.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
# Dependencies are installed at build time, not container start, or the
# environment is not reproducible.
COPY pyproject.toml uv.lock ./
RUN uv sync --all-extras --dev --frozen --no-install-project

COPY . .
RUN uv sync --all-extras --dev --frozen
CMD ["uv", "run", "pytest", "-m", "not network", "-v"]
```

**Verify before relying on it:** `JAVA_HOME` differs by architecture
(`arm64` on Apple Silicon, `amd64` on CI). Confirm the real path inside
the built image with
`docker run --rm <img> bash -lc 'ls /usr/lib/jvm'` and correct the `ENV`
line rather than guessing.

- [ ] **Step 8: Build and run the container**

```bash
docker build -f docker/Dockerfile.spark -t almanac-spark .
docker run --rm almanac-spark
```
Expected: the suite runs green inside the container.

- [ ] **Step 9: Update STATUS.md and commit**

```bash
git add src/almanac/spark.py docker tests docs/STATUS.md
git commit -m "feat: local Spark session with Delta, containerized

Session timezone pinned to UTC -- non-UTC would silently shift every
event timestamp and corrupt point-in-time correctness with nothing
downstream catching it.

Tests prove the Delta round trip and that replaceWhere is idempotent,
so Phase 1's Bronze writer can rely on both rather than rediscovering
them."
```

---

## Task 3: Archive URL construction (pure)

**Files:**
- Create: `src/almanac/extract/__init__.py`, `src/almanac/extract/urls.py`
- Test: `tests/unit/test_urls.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `archive_url(day: date, hour: int, base_url: str = ...) -> str`
  - `hours_in_range(start: date, end: date) -> Iterator[tuple[date, int]]`
  Later tasks build fetch plans from `hours_in_range`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_urls.py
from datetime import date

import pytest

from almanac.extract.urls import archive_url, hours_in_range


def test_hour_is_unpadded() -> None:
    # THE trap in this dataset's URL scheme: hour is 3, never 03.
    # Zero-padding yields a 404 for 10 of every 24 files.
    assert archive_url(date(2025, 3, 15), 3).endswith("/2025-03-15-3.json.gz")


def test_midnight_is_zero_not_24() -> None:
    assert archive_url(date(2025, 3, 15), 0).endswith("/2025-03-15-0.json.gz")


def test_double_digit_hour_unchanged() -> None:
    assert archive_url(date(2025, 3, 15), 14).endswith("/2025-03-15-14.json.gz")


def test_date_is_zero_padded_even_though_hour_is_not() -> None:
    # Asymmetric on purpose, and easy to "tidy" into a bug later.
    assert archive_url(date(2014, 6, 2), 7).endswith("/2014-06-02-7.json.gz")


@pytest.mark.parametrize("bad", [-1, 24, 100])
def test_hour_out_of_range_rejected(bad: int) -> None:
    with pytest.raises(ValueError, match="hour"):
        archive_url(date(2025, 3, 15), bad)


def test_hours_in_range_is_inclusive_and_ordered() -> None:
    hours = list(hours_in_range(date(2025, 1, 1), date(2025, 1, 2)))
    assert len(hours) == 48
    assert hours[0] == (date(2025, 1, 1), 0)
    assert hours[-1] == (date(2025, 1, 2), 23)


def test_hours_in_range_rejects_backwards_range() -> None:
    with pytest.raises(ValueError, match="before"):
        list(hours_in_range(date(2025, 1, 2), date(2025, 1, 1)))
```

- [ ] **Step 2: Run and confirm it fails**

Run: `uv run pytest tests/unit/test_urls.py -v`
Expected: FAIL — `ModuleNotFoundError: almanac.extract.urls`

- [ ] **Step 3: Implement**

```python
# src/almanac/extract/urls.py
"""Pure URL construction for GH Archive. No I/O."""

from collections.abc import Iterator
from datetime import date, timedelta

DEFAULT_BASE_URL = "https://data.gharchive.org"
HOURS_PER_DAY = 24


def archive_url(day: date, hour: int, base_url: str = DEFAULT_BASE_URL) -> str:
    """URL for one hourly archive file.

    The hour component is UNPADDED -- ``3``, never ``03``. The date
    component IS zero-padded. That asymmetry is the scheme's own, and
    "fixing" it produces a 404 for ten of every twenty-four files.
    """
    if not 0 <= hour < HOURS_PER_DAY:
        raise ValueError(f"hour must be 0..23, got {hour}")
    return f"{base_url}/{day:%Y-%m-%d}-{hour}.json.gz"


def hours_in_range(start: date, end: date) -> Iterator[tuple[date, int]]:
    """Every (day, hour) from start to end inclusive, in order."""
    if end < start:
        raise ValueError(f"end {end} must not be before start {start}")
    day = start
    while day <= end:
        for hour in range(HOURS_PER_DAY):
            yield day, hour
        day += timedelta(days=1)
```

- [ ] **Step 4: Run and confirm green**

Run: `uv run pytest tests/unit/test_urls.py -v`
Expected: 7 PASS.

- [ ] **Step 5: Verify one URL is real, once**

```bash
curl -sI "$(uv run python -c "
from datetime import date
from almanac.extract.urls import archive_url
print(archive_url(date(2025,3,15), 3))
")" | head -1
```
Expected: `HTTP/2 200`. If 404, the scheme changed — stop and correct the
design doc before continuing.

- [ ] **Step 6: Update STATUS.md and commit**

```bash
git add src/almanac/extract tests/unit/test_urls.py docs/STATUS.md
git commit -m "feat: pure archive URL construction

Hour is unpadded and the date is not; the asymmetry is the scheme's own
and is pinned by test, because tidying it would 404 ten of every
twenty-four files."
```

---

## Task 4: Fetching with absent / empty / failed distinguished

**Files:**
- Create: `src/almanac/extract/outcome.py`, `src/almanac/extract/archive.py`
- Test: `tests/unit/test_archive_fetch.py`

**Interfaces:**
- Consumes: `archive_url`, `Settings`
- Produces:
  - `FetchStatus` enum: `OK`, `ABSENT`, `EMPTY`, `FAILED`
  - `FetchResult` frozen dataclass: `url, status, path, bytes_downloaded,
    attempts, error`
  - `classify_response(status_code: int, content_length: int) ->
    FetchStatus` — **pure**, the whole decision, unit-testable with no I/O
  - `fetch_hour(day, hour, *, client, dest_dir, settings) -> FetchResult`
    — the I/O edge

Phase 1's Bronze ingestion consumes `fetch_hour` unchanged.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_archive_fetch.py
from datetime import date

import httpx
import pytest

from almanac.config import Settings
from almanac.extract.archive import classify_response, fetch_hour
from almanac.extract.outcome import FetchStatus


# --- the pure decision, exhaustively ---

def test_200_with_bytes_is_ok() -> None:
    assert classify_response(200, 1024) is FetchStatus.OK


def test_200_with_zero_bytes_is_empty_not_ok() -> None:
    # A published-but-empty hour is a real, different condition from a
    # missing one. Collapsing them hides collector outages.
    assert classify_response(200, 0) is FetchStatus.EMPTY


def test_404_is_absent_not_failed() -> None:
    # Absent hours are expected in this dataset and must NOT be retried
    # or treated as errors.
    assert classify_response(404, 0) is FetchStatus.ABSENT


@pytest.mark.parametrize("code", [500, 502, 503, 504, 429])
def test_server_errors_are_failed(code: int) -> None:
    assert classify_response(code, 0) is FetchStatus.FAILED


# --- the I/O edge, against a mock transport ---

def _client(handler: object) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_writes_file_and_reports_ok(tmp_path) -> None:
    body = b"\x1f\x8b" + b"0" * 100

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    result = fetch_hour(
        date(2025, 3, 15), 3,
        client=_client(handler), dest_dir=tmp_path, settings=Settings(),
    )
    assert result.status is FetchStatus.OK
    assert result.bytes_downloaded == len(body)
    assert result.path is not None and result.path.read_bytes() == body


def test_absent_hour_is_not_retried(tmp_path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404)

    result = fetch_hour(
        date(2025, 3, 15), 3,
        client=_client(handler), dest_dir=tmp_path, settings=Settings(),
    )
    assert result.status is FetchStatus.ABSENT
    assert calls["n"] == 1, "a 404 is a fact about the data, not a transient error"
    assert result.path is None


def test_server_error_is_retried_then_reported_failed(tmp_path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    settings = Settings(max_fetch_attempts=3)
    result = fetch_hour(
        date(2025, 3, 15), 3,
        client=_client(handler), dest_dir=tmp_path, settings=settings,
    )
    assert result.status is FetchStatus.FAILED
    assert calls["n"] == 3
    assert result.attempts == 3


def test_transient_error_then_success(tmp_path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=b"\x1f\x8bdata")

    result = fetch_hour(
        date(2025, 3, 15), 3,
        client=_client(handler), dest_dir=tmp_path, settings=Settings(),
    )
    assert result.status is FetchStatus.OK
    assert result.attempts == 2


def test_partial_download_leaves_no_file(tmp_path) -> None:
    # A half-written file that looks complete is worse than no file: the
    # next run would skip it and Bronze would silently lose events.
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("connection dropped")

    result = fetch_hour(
        date(2025, 3, 15), 3,
        client=_client(handler), dest_dir=tmp_path, settings=Settings(),
    )
    assert result.status is FetchStatus.FAILED
    assert list(tmp_path.iterdir()) == []
```

- [ ] **Step 2: Run and confirm it fails**

Run: `uv run pytest tests/unit/test_archive_fetch.py -v`
Expected: FAIL — `ModuleNotFoundError: almanac.extract.outcome`

- [ ] **Step 3: Implement the outcome vocabulary**

```python
# src/almanac/extract/outcome.py
"""The vocabulary for how a fetch turned out.

Distinguishing ABSENT from EMPTY from FAILED is a design requirement, not
a nicety (design doc section 12, trap 5): the collector has genuinely
failed at points across fourteen years, and an ingestion layer that
cannot tell "this hour was never published" from "the download broke"
cannot report completeness honestly.
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class FetchStatus(str, Enum):
    OK = "ok"
    ABSENT = "absent"   # 404 -- the hour was never published. Not an error.
    EMPTY = "empty"     # published, zero bytes. A real collector gap.
    FAILED = "failed"   # transport/5xx. Retryable; may succeed later.


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: FetchStatus
    path: Path | None
    bytes_downloaded: int
    attempts: int
    error: str | None = None
```

- [ ] **Step 4: Implement classification and fetching**

```python
# src/almanac/extract/archive.py
"""The I/O edge for GH Archive. Decisions live in classify_response."""

from datetime import date
from pathlib import Path

import httpx

from almanac.config import Settings
from almanac.extract.outcome import FetchResult, FetchStatus
from almanac.extract.urls import archive_url

_HTTP_OK = 200
_HTTP_NOT_FOUND = 404


def classify_response(status_code: int, content_length: int) -> FetchStatus:
    """Pure. Which of the four outcomes a response represents."""
    if status_code == _HTTP_NOT_FOUND:
        return FetchStatus.ABSENT
    if status_code == _HTTP_OK:
        return FetchStatus.OK if content_length > 0 else FetchStatus.EMPTY
    return FetchStatus.FAILED


def fetch_hour(
    day: date,
    hour: int,
    *,
    client: httpx.Client,
    dest_dir: Path,
    settings: Settings,
) -> FetchResult:
    """Fetch one hourly file. Retries only genuinely transient failures."""
    url = archive_url(day, hour, settings.archive_base_url)
    dest_dir.mkdir(parents=True, exist_ok=True)
    final = dest_dir / url.rsplit("/", 1)[-1]
    tmp = final.with_suffix(final.suffix + ".part")

    last_error: str | None = None
    for attempt in range(1, settings.max_fetch_attempts + 1):
        try:
            response = client.get(url, timeout=settings.http_timeout_seconds)
        except httpx.HTTPError as exc:
            last_error = str(exc)
            tmp.unlink(missing_ok=True)
            continue

        status = classify_response(response.status_code, len(response.content))

        # A 404 or an empty file is a fact about the data. Retrying either
        # wastes time and cannot change the answer.
        if status is FetchStatus.ABSENT or status is FetchStatus.EMPTY:
            return FetchResult(url, status, None, 0, attempt)

        if status is FetchStatus.OK:
            # Write to .part then rename: a crash mid-write must never
            # leave a truncated file that a later run mistakes for
            # complete.
            tmp.write_bytes(response.content)
            tmp.replace(final)
            return FetchResult(url, status, final, len(response.content), attempt)

        last_error = f"HTTP {response.status_code}"

    tmp.unlink(missing_ok=True)
    return FetchResult(
        url, FetchStatus.FAILED, None, 0, settings.max_fetch_attempts, last_error
    )
```

- [ ] **Step 5: Run and confirm green**

Run: `uv run pytest tests/unit/test_archive_fetch.py -v`
Expected: 11 PASS.

- [ ] **Step 6: Full check**

Run: `make check`
Expected: all green.

- [ ] **Step 7: Update STATUS.md and commit**

```bash
git add src/almanac/extract tests/unit/test_archive_fetch.py docs/STATUS.md
git commit -m "feat: archive fetching with absent/empty/failed distinguished

The four outcomes are a design requirement, not a nicety: an ingestion
layer that cannot tell 'never published' from 'download broke' cannot
report completeness honestly.

404 and zero-byte responses are facts about the data and are never
retried. Downloads land on .part and are renamed, so a crash mid-write
cannot leave a truncated file a later run mistakes for complete."
```

---

## Task 5: Tier 0 fixtures

**Files:**
- Create: `scripts/build_fixtures.py`
- Create: `tests/fixtures/*.jsonl.gz` (committed, sub-sampled)
- Modify: `tests/conftest.py`, `Makefile`
- Test: `tests/unit/test_fixtures.py`

**Interfaces:**
- Consumes: `fetch_hour`
- Produces: pytest fixtures `modern_events_path` and `legacy_events_path`
  returning `Path` to committed sub-samples. Every later test that needs
  real events uses these and never the network.

- [ ] **Step 1: Write the fixture builder**

```python
# scripts/build_fixtures.py
"""Build committed test fixtures by sub-sampling real archive hours.

Run manually via `make fixtures`, not in CI. Output is committed; raw
downloads are not (see .gitignore).

Sampling is deterministic (every Nth line) rather than random so the
fixture is reproducible from the same source hour.
"""

import gzip
import sys
from datetime import date
from pathlib import Path

import httpx

from almanac.config import Settings
from almanac.extract.archive import fetch_hour
from almanac.extract.outcome import FetchStatus

TARGET_EVENTS = 2_000
SAMPLES: list[tuple[str, date, int]] = [
    ("modern", date(2025, 3, 15), 14),
    ("legacy", date(2014, 6, 12), 14),
]


def build(name: str, day: date, hour: int, settings: Settings) -> Path:
    with httpx.Client(follow_redirects=True) as client:
        result = fetch_hour(
            day, hour, client=client,
            dest_dir=settings.data_dir / "raw", settings=settings,
        )
    if result.status is not FetchStatus.OK or result.path is None:
        raise RuntimeError(f"{name}: fetch returned {result.status}")

    with gzip.open(result.path, "rt", encoding="utf-8") as fh:
        lines = fh.readlines()

    stride = max(1, len(lines) // TARGET_EVENTS)
    sampled = lines[::stride][:TARGET_EVENTS]

    out = settings.fixture_dir / f"{name}-{day:%Y-%m-%d}-{hour}.jsonl.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as fh:
        fh.writelines(sampled)

    size_mb = out.stat().st_size / 1_048_576
    print(f"{name}: {len(lines)} events -> {len(sampled)} sampled, {size_mb:.2f} MB")
    if size_mb > 5:
        raise RuntimeError(f"{out} is {size_mb:.1f} MB; keep fixtures under 5 MB")
    return out


def main() -> int:
    settings = Settings()
    for name, day, hour in SAMPLES:
        build(name, day, hour, settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Add the Make target and run it**

```makefile
fixtures:
	uv run python scripts/build_fixtures.py
```

Run: `make fixtures`
Expected: two files written, each reported well under 5 MB. **If either
exceeds 5 MB, lower `TARGET_EVENTS` and rerun** — do not commit it.

- [ ] **Step 3: Write the failing test**

```python
# tests/unit/test_fixtures.py
import gzip
import json
from pathlib import Path


def test_modern_fixture_parses_and_has_modern_shape(modern_events_path: Path) -> None:
    with gzip.open(modern_events_path, "rt", encoding="utf-8") as fh:
        events = [json.loads(line) for line in fh]
    assert len(events) > 100
    first = events[0]
    assert {"id", "type", "actor", "repo", "created_at"} <= first.keys()
    # The 2015+ shape uses `repo`. Task 6 confirms 2014 differs.
    assert "repo" in first


def test_legacy_fixture_parses(legacy_events_path: Path) -> None:
    with gzip.open(legacy_events_path, "rt", encoding="utf-8") as fh:
        events = [json.loads(line) for line in fh]
    assert len(events) > 100
    assert "type" in events[0]


def test_fixtures_are_small_enough_to_commit(
    modern_events_path: Path, legacy_events_path: Path
) -> None:
    for path in (modern_events_path, legacy_events_path):
        assert path.stat().st_size < 5 * 1_048_576, f"{path} too large to commit"
```

- [ ] **Step 4: Add the fixture accessors**

```python
# tests/conftest.py  (append)
from pathlib import Path

import pytest

from almanac.config import Settings


def _one(pattern: str) -> Path:
    matches = sorted(Settings().fixture_dir.glob(pattern))
    if not matches:
        pytest.skip(f"no fixture matching {pattern}; run `make fixtures`")
    return matches[0]


@pytest.fixture(scope="session")
def modern_events_path() -> Path:
    return _one("modern-*.jsonl.gz")


@pytest.fixture(scope="session")
def legacy_events_path() -> Path:
    return _one("legacy-*.jsonl.gz")
```

- [ ] **Step 5: Run and confirm green**

Run: `uv run pytest tests/unit/test_fixtures.py -v`
Expected: 3 PASS.

- [ ] **Step 6: Confirm fixtures are actually tracked**

```bash
git check-ignore -v tests/fixtures/*.jsonl.gz && echo "IGNORED - fix .gitignore" || echo "tracked, good"
```

`.gitignore` excludes `*.json.gz`; fixtures are named `.jsonl.gz` so they
are unaffected. **Confirm this rather than assume it** — if the pattern
does match, add a `!tests/fixtures/` negation.

- [ ] **Step 7: Update STATUS.md and commit**

```bash
git add scripts/build_fixtures.py tests/fixtures tests/conftest.py tests/unit/test_fixtures.py Makefile docs/STATUS.md
git commit -m "feat: Tier 0 committed fixtures from real archive hours

Deterministic every-Nth-line sub-sampling so the fixture is reproducible
from its source hour. Tests never touch the network; the builder is a
manual make target, never CI."
```

---

## Task 6: Schema-era diff — measured, not assumed

**Files:**
- Create: `src/almanac/explore/__init__.py`, `src/almanac/explore/schema.py`
- Create: `docs/findings/2026-09-XX-schema-eras.md`
- Test: `tests/unit/test_schema_eras.py`

**Interfaces:**
- Consumes: `modern_events_path`, `legacy_events_path`
- Produces:
  - `SchemaEra` enum: `LEGACY_V1`, `MODERN_V2`
  - `era_for(created_at: datetime) -> SchemaEra`
  - `field_paths(event: dict) -> set[str]` — dotted top-two-level paths
  - `diff_field_paths(a, b) -> tuple[set[str], set[str], set[str]]`
    → `(only_in_a, only_in_b, common)`

Phase 1's Silver routing consumes `era_for` unchanged.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_schema_eras.py
from datetime import UTC, datetime

from almanac.explore.schema import (
    SchemaEra,
    diff_field_paths,
    era_for,
    field_paths,
)


def test_era_boundary_is_2015_01_01() -> None:
    assert era_for(datetime(2014, 12, 31, 23, 59, tzinfo=UTC)) is SchemaEra.LEGACY_V1
    assert era_for(datetime(2015, 1, 1, 0, 0, tzinfo=UTC)) is SchemaEra.MODERN_V2


def test_field_paths_are_two_levels_deep() -> None:
    event = {"id": "1", "actor": {"id": 2, "login": "x"}, "payload": {"action": "opened"}}
    assert field_paths(event) == {"id", "actor.id", "actor.login", "payload.action"}


def test_field_paths_handles_null_nested_object() -> None:
    # `org` is absent or null on non-org repos; this must not explode.
    assert field_paths({"id": "1", "org": None}) == {"id", "org"}


def test_diff_reports_all_three_sets() -> None:
    only_a, only_b, common = diff_field_paths({"a", "shared"}, {"b", "shared"})
    assert only_a == {"a"}
    assert only_b == {"b"}
    assert common == {"shared"}
```

- [ ] **Step 2: Run and confirm it fails**

Run: `uv run pytest tests/unit/test_schema_eras.py -v`
Expected: FAIL — `ModuleNotFoundError: almanac.explore.schema`

- [ ] **Step 3: Implement**

```python
# src/almanac/explore/schema.py
"""Schema-era classification and field diffing. Pure; no I/O."""

from datetime import UTC, datetime
from enum import Enum
from typing import Any

# The 2015-01-01 boundary is the design doc's stated hypothesis
# (section 12, trap 7). This task's findings document is where it is
# confirmed or corrected against real data.
ERA_BOUNDARY = datetime(2015, 1, 1, tzinfo=UTC)


class SchemaEra(str, Enum):
    LEGACY_V1 = "legacy_v1"
    MODERN_V2 = "modern_v2"


def era_for(created_at: datetime) -> SchemaEra:
    return SchemaEra.MODERN_V2 if created_at >= ERA_BOUNDARY else SchemaEra.LEGACY_V1


def field_paths(event: dict[str, Any]) -> set[str]:
    """Dotted paths, two levels deep.

    Two levels is deliberate: deeper paths explode combinatorially across
    payload variants and obscure the structural differences this diff
    exists to surface.
    """
    paths: set[str] = set()
    for key, value in event.items():
        if isinstance(value, dict):
            paths.update(f"{key}.{sub}" for sub in value)
        else:
            paths.add(key)
    return paths


def diff_field_paths(a: set[str], b: set[str]) -> tuple[set[str], set[str], set[str]]:
    return a - b, b - a, a & b
```

- [ ] **Step 4: Run and confirm green**

Run: `uv run pytest tests/unit/test_schema_eras.py -v`
Expected: 4 PASS.

- [ ] **Step 5: Produce the real diff and write the findings document**

```bash
uv run python - <<'PY'
import gzip, json, collections
from pathlib import Path
from almanac.config import Settings
from almanac.explore.schema import diff_field_paths, field_paths

def load(pattern):
    p = sorted(Settings().fixture_dir.glob(pattern))[0]
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]

def union(events):
    u = set()
    for e in events:
        u |= field_paths(e)
    return u

legacy, modern = load("legacy-*"), load("modern-*")
only_legacy, only_modern, common = diff_field_paths(union(legacy), union(modern))

print("### top-level keys")
print("legacy:", sorted({k for e in legacy for k in e}))
print("modern:", sorted({k for e in modern for k in e}))
print("\n### ONLY in legacy (", len(only_legacy), ")"); [print(" ", p) for p in sorted(only_legacy)]
print("\n### ONLY in modern (", len(only_modern), ")"); [print(" ", p) for p in sorted(only_modern)]
print("\n### event types by era")
print("legacy:", collections.Counter(e["type"] for e in legacy).most_common())
print("modern:", collections.Counter(e["type"] for e in modern).most_common())
PY
```

Write `docs/findings/2026-09-XX-schema-eras.md` containing the **actual
printed output**. It must state:
- The real top-level key sets for each era
- Every field present in one era and not the other
- Event types that exist only in the legacy era
- **Whether the design doc's claims in §12 trap 7 held.** The doc claims
  `repository` instead of `repo` and a different actor representation.
  Confirm or correct it.

- [ ] **Step 6: Correct the design doc if reality disagreed**

If the real field names differ from §12 trap 7, edit
`docs/design/2026-09-01-almanac-system-design.md` in this same commit and
note the correction in the STATUS verification log. Reality wins.

- [ ] **Step 7: Update STATUS.md and commit**

```bash
git add src/almanac/explore tests/unit/test_schema_eras.py docs/findings docs/design docs/STATUS.md
git commit -m "feat: schema-era classification, with the real 2014/2025 diff measured

Field-level diff taken against real fixtures from both eras rather than
trusting the design doc's directional description. Findings document
records the actual key sets and era-exclusive event types."
```

---

## Task 7: Dataset measurements — the three that gate the design

**Files:**
- Create: `src/almanac/explore/measure.py`
- Create: `scripts/measure_dataset.py`
- Create: `docs/findings/2026-09-XX-dataset-measurements.md`
- Test: `tests/unit/test_measure.py`

**Interfaces:**
- Consumes: `fetch_hour`, fixtures
- Produces:
  - `duplicate_ratio(event_ids: Iterable[str]) -> float`
  - `classify_bot(login: str) -> BotMatch` where `BotMatch` is an enum
    `SUFFIX`, `CURATED`, `REGEX`, `NONE`
  - `rename_events(observations: Iterable[tuple[int, str, datetime]]) ->
    list[RepoRename]`

**Why this task matters most:** if `rename_events` finds too few renames
in the candidate window, `dim_repo`'s SCD2 has nothing to demonstrate and
the window must move. That is cheap now and expensive after Phase 3.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_measure.py
from datetime import UTC, datetime

import pytest

from almanac.explore.measure import (
    BotMatch,
    classify_bot,
    duplicate_ratio,
    rename_events,
)


def test_duplicate_ratio_counts_extra_copies_not_distinct_ids() -> None:
    # 4 events, 3 distinct -> 1 extra copy out of 4 = 0.25
    assert duplicate_ratio(["a", "b", "c", "a"]) == pytest.approx(0.25)


def test_duplicate_ratio_of_unique_stream_is_zero() -> None:
    assert duplicate_ratio(["a", "b", "c"]) == 0.0


def test_duplicate_ratio_of_empty_stream_is_zero_not_error() -> None:
    assert duplicate_ratio([]) == 0.0


def test_bot_suffix_match() -> None:
    assert classify_bot("dependabot[bot]") is BotMatch.SUFFIX


def test_bot_regex_match_is_reported_separately_from_suffix() -> None:
    # Reported separately precisely so its false-positive rate can be
    # measured rather than assumed (design doc section 12, trap 6).
    assert classify_bot("nightly-ci") is BotMatch.REGEX


def test_known_regex_false_positives_are_visible() -> None:
    # These are humans/projects, not bots. The rule matches them anyway.
    # The test pins the behaviour so the FP rate is a measured number.
    assert classify_bot("robotframework") is BotMatch.REGEX
    assert classify_bot("Abbott") is BotMatch.REGEX


def test_ordinary_login_is_not_a_bot() -> None:
    assert classify_bot("octocat") is BotMatch.NONE


def test_rename_detected_when_same_repo_id_changes_name() -> None:
    obs = [
        (1, "old/name", datetime(2025, 1, 1, tzinfo=UTC)),
        (1, "new/name", datetime(2025, 2, 1, tzinfo=UTC)),
    ]
    renames = rename_events(obs)
    assert len(renames) == 1
    assert renames[0].repo_id == 1
    assert renames[0].from_name == "old/name"
    assert renames[0].to_name == "new/name"


def test_repeated_same_name_is_not_a_rename() -> None:
    obs = [
        (1, "same/name", datetime(2025, 1, 1, tzinfo=UTC)),
        (1, "same/name", datetime(2025, 2, 1, tzinfo=UTC)),
    ]
    assert rename_events(obs) == []


def test_observations_are_ordered_before_comparison() -> None:
    # Out-of-order input must not fabricate a reversed rename.
    obs = [
        (1, "new/name", datetime(2025, 2, 1, tzinfo=UTC)),
        (1, "old/name", datetime(2025, 1, 1, tzinfo=UTC)),
    ]
    renames = rename_events(obs)
    assert len(renames) == 1
    assert renames[0].from_name == "old/name"
```

- [ ] **Step 2: Run and confirm it fails**

Run: `uv run pytest tests/unit/test_measure.py -v`
Expected: FAIL — `ModuleNotFoundError: almanac.explore.measure`

- [ ] **Step 3: Implement**

```python
# src/almanac/explore/measure.py
"""Pure counters over parsed events. No I/O."""

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

# Curated exact logins, extended as real ones are found.
CURATED_BOTS = frozenset({"dependabot", "renovate", "github-actions"})

# Deliberately kept as its own clause so its false positives can be
# counted (design doc section 12, trap 6). "robotframework" and "Abbott"
# match this and are not bots.
BOT_REGEX = re.compile(r"(?i)(bot|automation|ci)$")


class BotMatch(str, Enum):
    SUFFIX = "suffix"     # login ends with [bot] -- authoritative
    CURATED = "curated"   # exact match on a known list
    REGEX = "regex"       # heuristic; HAS false positives
    NONE = "none"


@dataclass(frozen=True)
class RepoRename:
    repo_id: int
    from_name: str
    to_name: str
    observed_at: datetime


def duplicate_ratio(event_ids: Iterable[str]) -> float:
    """Fraction of the stream that is a redundant copy.

    Not 1 - distinct/total by accident: this counts extra copies, so a
    stream with no duplicates scores 0.0.
    """
    ids = list(event_ids)
    if not ids:
        return 0.0
    return (len(ids) - len(set(ids))) / len(ids)


def classify_bot(login: str) -> BotMatch:
    """Which clause, if any, marks this login as a bot."""
    if login.endswith("[bot]"):
        return BotMatch.SUFFIX
    if login.lower() in CURATED_BOTS:
        return BotMatch.CURATED
    if BOT_REGEX.search(login):
        return BotMatch.REGEX
    return BotMatch.NONE


def rename_events(
    observations: Iterable[tuple[int, str, datetime]],
) -> list[RepoRename]:
    """Name changes for a repo id, in observation order.

    Input is (repo_id, repo_name, observed_at). Observations are sorted
    by time first: unsorted input would fabricate reversed renames.
    """
    by_repo: dict[int, list[tuple[datetime, str]]] = defaultdict(list)
    for repo_id, name, at in observations:
        by_repo[repo_id].append((at, name))

    renames: list[RepoRename] = []
    for repo_id, seen in by_repo.items():
        seen.sort()
        for (_, previous), (at, current) in zip(seen, seen[1:], strict=False):
            if previous != current:
                renames.append(RepoRename(repo_id, previous, current, at))
    return renames
```

- [ ] **Step 4: Run and confirm green**

Run: `uv run pytest tests/unit/test_measure.py -v`
Expected: 10 PASS.

- [ ] **Step 5: Write the measurement script**

```python
# scripts/measure_dataset.py
"""Measure the dataset properties the design depends on.

Samples three non-adjacent days across the candidate quarter. Three days
rather than one because a rename can only be seen as the same repo id
carrying different names at different times -- a single day cannot show
one at all.

Downloads roughly 72 files. Run once; the numbers go into a findings doc.
"""

import collections
import gzip
import json
import sys
from datetime import UTC, date, datetime

import httpx

from almanac.config import Settings
from almanac.explore.measure import BotMatch, classify_bot, duplicate_ratio, rename_events
from almanac.extract.archive import fetch_hour
from almanac.extract.outcome import FetchStatus

SAMPLE_DAYS = [date(2025, 1, 8), date(2025, 2, 12), date(2025, 3, 19)]


def main() -> int:
    settings = Settings()
    ids: list[str] = []
    repo_obs: list[tuple[int, str, datetime]] = []
    bots: collections.Counter[BotMatch] = collections.Counter()
    regex_only_logins: collections.Counter[str] = collections.Counter()
    outcomes: collections.Counter[FetchStatus] = collections.Counter()
    events = 0

    with httpx.Client(follow_redirects=True) as client:
        for day in SAMPLE_DAYS:
            for hour in range(24):
                res = fetch_hour(
                    day, hour, client=client,
                    dest_dir=settings.data_dir / "raw", settings=settings,
                )
                outcomes[res.status] += 1
                if res.status is not FetchStatus.OK or res.path is None:
                    print(f"  {day} {hour:02d}: {res.status.value}")
                    continue
                with gzip.open(res.path, "rt", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            e = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        events += 1
                        ids.append(e["id"])
                        login = (e.get("actor") or {}).get("login", "")
                        match = classify_bot(login)
                        bots[match] += 1
                        if match is BotMatch.REGEX:
                            regex_only_logins[login] += 1
                        repo = e.get("repo") or {}
                        if repo.get("id") and repo.get("name"):
                            repo_obs.append((
                                repo["id"], repo["name"],
                                datetime.fromisoformat(
                                    e["created_at"].replace("Z", "+00:00")
                                ).astimezone(UTC),
                            ))
            print(f"{day} done, {events} events so far")

    renames = rename_events(repo_obs)
    print("\n=== MEASURED ===")
    print(f"days sampled        : {[str(d) for d in SAMPLE_DAYS]}")
    print(f"fetch outcomes      : {dict(outcomes)}")
    print(f"total events        : {events}")
    print(f"duplicate ratio     : {duplicate_ratio(ids):.6f}")
    print(f"distinct event ids  : {len(set(ids))}")
    print(f"bot classification  : { {k.value: v for k, v in bots.items()} }")
    print(f"RENAMES DETECTED    : {len(renames)}   <-- gates SCD2")
    print(f"distinct repos      : {len({r for r, _, _ in repo_obs})}")
    print("\ntop 40 regex-only matches (inspect these for false positives):")
    for login, n in regex_only_logins.most_common(40):
        print(f"  {login:40s} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Run it**

Run: `uv run python scripts/measure_dataset.py 2>&1 | tee /tmp/measure.log`
Expected: completes; ~6 GB downloaded to `data/raw/` (gitignored).

- [ ] **Step 7: Write the findings document, and act on the rename count**

Write `docs/findings/2026-09-XX-dataset-measurements.md` with the actual
output. Then **decide, and record the decision**:

- **Renames ≥ 50** — the candidate window supports SCD2. Proceed
  unchanged.
- **Renames < 50** — the window is too thin. Do **not** widen the volume.
  Options, in preference order: (a) sample days further apart across a
  longer span; (b) move Tier 4's window earlier so more history is
  observable; (c) scope `dim_repo` to a longer, thinner slice than the
  rest of Tier 4. Record which, and why, in the design doc §4.5.

Also record the **regex-clause false-positive rate**: manually inspect the
top 40 regex-only logins, count how many are genuinely not bots, and state
the rate. That number becomes ADR-005's evidence.

- [ ] **Step 8: Update STATUS.md and commit**

```bash
git add src/almanac/explore/measure.py scripts/measure_dataset.py tests/unit/test_measure.py docs/findings docs/design docs/STATUS.md
git commit -m "feat: dataset measurements, including the rename count that gates SCD2

Three non-adjacent days sampled across the candidate quarter -- a rename
is only visible as one repo id carrying different names at different
times, so a single day cannot show one at all.

Bot classification reports which clause matched, so the regex clause's
false-positive rate is a measured number rather than an assumption."
```

---

## Task 8: Embedding cost sizing

**Files:**
- Create: `scripts/size_embeddings.py`
- Create: `docs/findings/2026-09-XX-embedding-cost.md`

**Interfaces:**
- Consumes: `modern_events_path`
- Produces: a findings document only. No library code — this is a sizing
  spike, and its output is a number that decides Phase 5's scope.

**Why now:** Phase 5 embeds PR and issue text at Tier 4 scale. If local
CPU throughput makes that a multi-day job, Phase 5 needs redesigning —
and finding that out in week nine is the expensive version.

- [ ] **Step 1: Write the sizing script**

```python
# scripts/size_embeddings.py
"""Measure real embedding throughput and project it to Tier 4 scale."""

import gzip
import json
import sys
import time

from almanac.config import Settings

MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def main() -> int:
    from sentence_transformers import SentenceTransformer

    path = sorted(Settings().fixture_dir.glob("modern-*.jsonl.gz"))[0]
    texts: list[str] = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            e = json.loads(line)
            pr = (e.get("payload") or {}).get("pull_request") or {}
            text = " ".join(filter(None, [pr.get("title"), pr.get("body")]))
            if text.strip():
                texts.append(text[:2000])

    if not texts:
        print("no PR text in fixture; widen the sample before sizing")
        return 1

    model = SentenceTransformer(MODEL)
    model.encode(texts[:16])  # warm up; excluded from timing

    start = time.perf_counter()
    vectors = model.encode(texts, batch_size=32, show_progress_bar=False)
    elapsed = time.perf_counter() - start

    per_sec = len(texts) / elapsed
    print(f"model            : {MODEL}")
    print(f"texts embedded   : {len(texts)}")
    print(f"dimensions       : {vectors.shape[1]}")
    print(f"elapsed          : {elapsed:.2f}s")
    print(f"THROUGHPUT       : {per_sec:.1f} texts/sec")
    print()
    # Projection uses the measured 6,352 PRs/hour from design doc 5.1.
    for pct in (5, 25, 100):
        prs_month = 6352 * 24 * 30 * pct / 100
        hours = prs_month / per_sec / 3600
        print(f"  {pct:3d}% repo sample: {prs_month:,.0f} PRs/month -> {hours:.1f} h CPU")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run it**

```bash
uv run --with sentence-transformers python scripts/size_embeddings.py
```

Note `--with` rather than adding the dependency: this is a spike, and
`sentence-transformers` is not a Phase 0 dependency.

- [ ] **Step 3: Write the findings document and set Phase 5's scope**

Write `docs/findings/2026-09-XX-embedding-cost.md` with the measured
throughput and the projections. Then record a decision in design doc §9:

- **Under ~4 h CPU at the chosen sample rate** — Phase 5 proceeds as
  designed, embedding locally.
- **Over that** — Phase 5 narrows: embed only PRs in the modeling window
  rather than all of Tier 4, or reduce the sample rate for the embedding
  path specifically. **Record which, with the measured number as the
  reason.**

- [ ] **Step 4: Update STATUS.md and commit**

```bash
git add scripts/size_embeddings.py docs/findings docs/design docs/STATUS.md
git commit -m "chore: size embedding throughput before committing Phase 5's scope

Measured local CPU throughput and projected it to Tier 4 volumes at three
sample rates, so Phase 5's scope is set by a number rather than
discovered in week nine."
```

---

## Task 9: Azure pricing verification and Terraform skeleton

**Files:**
- Create: `infra/terraform/{main,variables,outputs,providers}.tf`
- Create: `infra/terraform/README.md`
- Create: `docs/findings/2026-09-XX-azure-pricing.md`

**Interfaces:**
- Consumes: nothing
- Produces: an applied Azure footprint with **no compute running**, and a
  verified pricing document that settles the Unity Catalog decision.

- [ ] **Step 1: Verify pricing against Microsoft's own calculator**

Record, from the Azure Pricing Calculator for the chosen region — **not
from any blog or aggregator**:

| Item | Standard | Premium |
|---|---|---|
| Jobs Compute $/DBU | | |
| All-Purpose $/DBU | | |
| Model Serving $/DBU | | |
| Underlying VM $/hr for the chosen worker SKU | | |

Then compute and record: the **Premium-over-Standard delta applied to
Tier 3's estimated backfill hours**. That number decides the Unity Catalog
question left open in design doc §11.

Also confirm the two figures the design doc currently marks unverified:
the Model Serving per-launch charge and its DBU rate (§8.1).

- [ ] **Step 2: Write the Terraform**

```hcl
# infra/terraform/providers.tf
terraform {
  required_version = ">= 1.9"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}
```

```hcl
# infra/terraform/variables.tf
variable "location" {
  type        = string
  description = "Azure region. Must support Azure Databricks and the chosen worker SKU."
}

variable "prefix" {
  type    = string
  default = "almanac"
}

variable "databricks_sku" {
  type        = string
  description = "standard | premium. Unity Catalog requires premium."
  default     = "premium"
}

variable "tags" {
  type = map(string)
  default = {
    project = "almanac"
    env     = "dev"
    owner   = "sree"
  }
}
```

```hcl
# infra/terraform/main.tf
resource "azurerm_resource_group" "this" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = var.tags
}

# ADLS Gen2: is_hns_enabled is what makes it Gen2 rather than plain blob.
resource "azurerm_storage_account" "lake" {
  name                     = "${var.prefix}lake"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  tags                     = var.tags
}

resource "azurerm_storage_container" "medallion" {
  for_each           = toset(["bronze", "silver", "gold", "features"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lake.id
}

# The workspace itself carries no cost while no cluster runs. Clusters are
# created by later phases, never here.
resource "azurerm_databricks_workspace" "this" {
  name                = "${var.prefix}-dbx"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.databricks_sku
  tags                = var.tags
}
```

```hcl
# infra/terraform/outputs.tf
output "workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}
```

- [ ] **Step 3: Plan, apply, and confirm what exists**

```bash
cd infra/terraform
terraform init
terraform plan -var="location=eastus2"
terraform apply -var="location=eastus2"
az databricks workspace show -n almanac-dbx -g almanac-rg --query "sku.name"
```

**If the region rejects the SKU or has no capacity, pick another region
and record which — do not retry blindly.**

- [ ] **Step 4: Confirm nothing is billing compute**

```bash
az resource list -g almanac-rg -o table
```

Expected: resource group, storage account, Databricks workspace. **No
cluster, no serving endpoint.** The workspace itself does not accrue DBUs
while idle; storage is billed by GB and is negligible while empty.

- [ ] **Step 5: Prove destroy is clean, then re-apply**

```bash
terraform destroy -var="location=eastus2"
az group exists -n almanac-rg     # expect: false
terraform apply -var="location=eastus2"
```

This is the design doc's own gate — `terraform destroy` must leave
nothing — and proving it now is far cheaper than proving it in week four
under credit pressure.

- [ ] **Step 6: Write the pricing findings and settle Unity Catalog**

Write `docs/findings/2026-09-XX-azure-pricing.md` with the verified table
and the Premium delta. Then record the Unity Catalog decision in design
doc §11, replacing "decide in Phase 0" with the actual choice and the
measured number behind it.

- [ ] **Step 7: Update STATUS.md and commit**

```bash
git add infra/terraform docs/findings docs/design docs/STATUS.md
git commit -m "feat: Azure footprint provisioned early, with pricing verified

Resource group, ADLS Gen2 and a Databricks workspace applied now, with no
compute, so week three is a backfill rather than a setup scramble.
Destroy-then-reapply proven clean here rather than under credit pressure.

Pricing taken from Microsoft's calculator, not aggregators, and the
Premium-over-Standard delta settles the Unity Catalog question."
```

---

## Phase 0 exit gate

Phase 0 is done when **all** hold:

- [ ] `make check` green; CI green on `main`
- [ ] The Spark suite runs inside the container
- [ ] Tier 0 fixtures committed, each under 5 MB, and no raw archive data tracked
- [ ] `docs/findings/` contains four documents, every number in them measured
- [ ] The 2014↔2025 diff is confirmed **against real data**, and design doc §12 trap 7 is corrected if it was wrong
- [ ] **The rename count is known and the SCD2 window decision is recorded**
- [ ] The bot regex clause's false-positive rate is a measured number
- [ ] Embedding throughput measured and Phase 5's scope set from it
- [ ] Azure pricing verified from Microsoft's calculator; the Unity Catalog decision recorded with its number
- [ ] `terraform destroy` proven to leave nothing, and the footprint re-applied
- [ ] `docs/STATUS.md` carries a verification-log row per task, stating actual results including any failures

**Do not begin Phase 1 with any box unticked.** Every one of them is an
assumption Phase 1 code would otherwise be written against.
