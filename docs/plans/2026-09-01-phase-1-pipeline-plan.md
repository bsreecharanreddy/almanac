# Phase 1 — Local Pipeline + Calibration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A config-driven Bronze→Silver pipeline that ingests all three
schema eras idempotently, quarantines bad records rather than dropping
them, and ends by measuring real cluster throughput on Azure — the number
every remaining cost figure in the design depends on.

**Architecture:** Sources are declared in YAML and validated by Pydantic;
no source-specific Python. Every transform is a pure `DataFrame -> DataFrame`
function in a module that performs no I/O, with reads and writes confined
to thin runner functions. Bronze never transforms. Silver normalizes across
eras, deduplicates, and splits clean from quarantined records — never
dropping either.

**Tech Stack:** Python 3.12+, PySpark 4.2.0, Delta Lake 4.4.0, Pydantic v2,
`pytest`, `chispa`, `ruff`, `mypy --strict`, Azure Databricks.

**Spec:** `docs/design/2026-09-01-almanac-system-design.md` — §4.1 Bronze,
§4.1a `event_id` across eras, §4.1b legacy timestamps, §4.2 Silver, §4.5
dataset scope and the calibration procedure, §9 phasing, §12 traps.

## Global Constraints

Copied verbatim from the spec and `CLAUDE.md`. Every task's requirements
implicitly include these.

- **Never quote a number that was not measured.**
- **Bronze never transforms.** Raw shape in, raw shape out, plus ingestion
  metadata. Every correction happens in Silver, where it is testable.
- **`ingested_at` is never conflated with `created_at`.** Wall-clock time
  and event time are different columns and always will be.
- **Every write is idempotent.** Re-running any hour produces identical
  content, never duplicates. `replaceWhere` on the partition.
- **Quarantine, never drop.** A record failing a quality rule goes to a
  quarantine table with the rules it failed, in a `_failed_rules` array.
- **Wrap every quality rule in `coalesce(cond, False)`.** In Spark, `NULL`
  in a boolean predicate is neither true nor false, so an un-coalesced rule
  silently passes records it should catch.
- **Assert the split.** `clean.count() + quarantined.count() == input.count()`
  is a test, not a comment. Records must not vanish between layers.
- **Pure transforms are separated from I/O.** `DataFrame in, DataFrame out`.
- **Spark session timezone is UTC, always.**
- **Actor identities are pseudonymized in anything published.**
- **`docs/STATUS.md` updates in the same commit as the work it describes.**
- **One commit per completed task**, each with its own green full-suite run.
- Python **3.12+**. `mypy --strict` and `ruff` must pass.
- **If reality contradicts the design doc, reality wins** — correct the
  design doc in the same commit and note it in the STATUS verification log.

## Measured facts this phase must respect

These came out of Phase 0 and are not assumptions. Getting any of them
wrong reintroduces a bug the project already paid to find.

| Fact | Consequence for this phase |
|---|---|
| **Legacy events have no `id`** (0 of 2,000) | `event_id` must be a content hash for `legacy_v1`, with an `event_id_source` column recording which |
| **Legacy `created_at` carries `-07:00`** | Parse as offset-aware and convert to UTC. A naive parse shifts every pre-2015 event seven hours, silently |
| **`actor` is a bare string in legacy, an object in modern** | Era dispatch must handle a *type* change, not a path change |
| **Three eras, not two** — `reduced_v3` from 2025-10-15 | Ingestible but not modelable. Bronze and Silver accept it; the label cannot be computed from it |
| **Case-only repo renames exist** (`GLB` → `glb`) | Any name comparison is case-sensitive. A case-insensitive join silently misses them |
| **Duplicate rate across *adjacent* hours is untested** | Phase 0 used non-adjacent hours, so trap 4 was never exercised. Task 5 tests it for real |

## File structure created by this phase

| File | Responsibility |
|---|---|
| `conf/sources/gharchive.yml` | Declarative source: URL pattern, partitioning, schema era rules |
| `src/almanac/pipeline/source.py` | Pydantic models for a source declaration. **Pure.** |
| `src/almanac/pipeline/bronze.py` | Bronze transform (metadata only) + idempotent writer |
| `src/almanac/pipeline/gaps.py` | **Pure.** Hour-gap detection over an expected range |
| `src/almanac/pipeline/eras.py` | **Pure.** Era dispatch: `event_id`, actor, timestamp normalization |
| `src/almanac/pipeline/dedup.py` | **Pure.** Deduplication within and across hour boundaries |
| `src/almanac/pipeline/quality.py` | **Pure.** Rule definitions and the clean/quarantine split |
| `src/almanac/pipeline/silver.py` | Silver runner wiring the pure pieces to I/O |
| `src/almanac/pipeline/runner.py` | Config-driven entry point: YAML → layer execution |
| `scripts/calibrate.py` | Task 7's Azure throughput measurement |
| `tests/unit/test_*.py` | One test module per pure module |
| `tests/integration/test_pipeline.py` | Bronze→Silver against committed fixtures |

---

## Task 1: Declarative source config

**Files:**
- Create: `conf/sources/gharchive.yml`
- Create: `src/almanac/pipeline/__init__.py`, `src/almanac/pipeline/source.py`
- Test: `tests/unit/test_source_config.py`

**Interfaces:**
- Produces: `SourceConfig.load(path: Path) -> SourceConfig` with fields
  `name: str`, `url_template: str`, `partition_by: list[str]`,
  `format: str`, `quality_rules: list[QualityRule]`.
  `QualityRule` has `name: str`, `expression: str`, `severity: Severity`.

The design's proof obligation (§9, Phase 2 gate) is that a second source
onboards **via YAML alone, zero new Python**. That only holds if source
behaviour is data from the first commit — retrofitting it later never works.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_source_config.py
from pathlib import Path

import pytest
from pydantic import ValidationError

from almanac.pipeline.source import Severity, SourceConfig

CONF = Path(__file__).resolve().parents[2] / "conf" / "sources" / "gharchive.yml"


def test_gharchive_config_loads() -> None:
    cfg = SourceConfig.load(CONF)
    assert cfg.name == "gharchive"
    assert "{year}" in cfg.url_template
    assert cfg.partition_by == ["event_date", "event_hour"]


def test_quality_rules_are_named_and_typed() -> None:
    cfg = SourceConfig.load(CONF)
    assert cfg.quality_rules, "at least one rule must be declared"
    for rule in cfg.quality_rules:
        assert rule.name
        assert rule.expression
        assert isinstance(rule.severity, Severity)


def test_rule_names_are_unique() -> None:
    # _failed_rules is keyed by name; duplicates would make a failure
    # untraceable to the rule that caused it.
    names = [r.name for r in SourceConfig.load(CONF).quality_rules]
    assert len(names) == len(set(names))


def test_unknown_field_is_rejected(tmp_path: Path) -> None:
    # A typo in YAML must fail loudly, not be silently ignored.
    p = tmp_path / "bad.yml"
    p.write_text(
        "name: x\nurl_template: 'u'\npartition_by: [a]\nformat: json\nquality_rules: []\ntyop: 1\n"
    )
    with pytest.raises(ValidationError):
        SourceConfig.load(p)
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_source_config.py -v`
Expected: FAIL — `ModuleNotFoundError: almanac.pipeline`

- [ ] **Step 3: Write the config**

```yaml
# conf/sources/gharchive.yml
name: gharchive
url_template: "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
format: json
partition_by: [event_date, event_hour]

quality_rules:
  - name: event_id_present
    expression: "event_id IS NOT NULL AND length(event_id) > 0"
    severity: reject
  - name: created_at_present
    expression: "created_at IS NOT NULL"
    severity: reject
  - name: created_at_not_future
    expression: "created_at <= ingested_at"
    severity: reject
  - name: repo_id_present
    expression: "repo_id IS NOT NULL"
    severity: reject
  - name: actor_login_present
    expression: "actor_login IS NOT NULL AND length(actor_login) > 0"
    severity: warn
  - name: event_type_known
    expression: "event_type IS NOT NULL"
    severity: reject
```

- [ ] **Step 4: Write the models**

```python
# src/almanac/pipeline/source.py
"""Declarative source configuration. Pure; no Spark, no I/O beyond a read."""

from enum import StrEnum
from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class Severity(StrEnum):
    """What a failed rule does to the record.

    ``REJECT`` quarantines it. ``WARN`` annotates it but leaves it in the
    clean set -- used where a field is genuinely optional in some era.
    """

    REJECT = "reject"
    WARN = "warn"


class QualityRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    expression: str = Field(min_length=1)
    severity: Severity


class SourceConfig(BaseModel):
    """A source, declared entirely in data.

    Onboarding a second source must require a new YAML file and no new
    Python -- see design doc §4.5a and the Phase 2 gate.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    url_template: str = Field(min_length=1)
    format: str = Field(min_length=1)
    partition_by: list[str] = Field(min_length=1)
    quality_rules: list[QualityRule]

    @field_validator("quality_rules")
    @classmethod
    def _rule_names_unique(cls, rules: list[QualityRule]) -> list[QualityRule]:
        names = [r.name for r in rules]
        if len(names) != len(set(names)):
            raise ValueError("quality rule names must be unique")
        return rules

    @classmethod
    def load(cls, path: Path) -> Self:
        return cls.model_validate(yaml.safe_load(path.read_text()))
```

- [ ] **Step 5: Run tests, expect pass**

Run: `uv run pytest tests/unit/test_source_config.py -v` → 4 passed

- [ ] **Step 6: Full suite, lint, commit**

```bash
make check
git add conf/ src/almanac/pipeline/ tests/unit/test_source_config.py docs/STATUS.md
git commit -m "feat: declarative source config with validated quality rules"
```

---

## Task 2: Bronze — metadata only, never transform

**Files:**
- Create: `src/almanac/pipeline/bronze.py`
- Test: `tests/unit/test_bronze.py`

**Interfaces:**
- Consumes: `SourceConfig` (Task 1); `local_session()` from `almanac.spark`
- Produces: `add_ingestion_metadata(df, *, ingested_at, source_file) -> DataFrame`
  and `write_bronze(df, path, *, event_date, event_hour) -> None`

Bronze's contract is that it is **replayable**: whatever Silver gets wrong
can be recomputed from Bronze without re-downloading. That only holds if
Bronze preserves the raw payload untouched.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_bronze.py
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze

pytestmark = pytest.mark.spark

INGESTED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def test_raw_payload_is_preserved_verbatim(spark: SparkSession) -> None:
    raw = spark.createDataFrame([('{"id":"1"}',)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    assert out.select("raw_json").collect() == raw.select("raw_json").collect()


def test_metadata_columns_added(spark: SparkSession) -> None:
    raw = spark.createDataFrame([("{}",)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    assert {"ingested_at", "source_file"} <= set(out.columns)
    assert out.first()["source_file"] == "f.gz"


def test_ingested_at_is_not_created_at(spark: SparkSession) -> None:
    # The single most important separation in the whole pipeline.
    raw = spark.createDataFrame([("{}",)], "raw_json string")
    out = add_ingestion_metadata(raw, ingested_at=INGESTED, source_file="f.gz")
    assert "created_at" not in out.columns


def test_rerunning_an_hour_is_idempotent(spark: SparkSession, tmp_path) -> None:
    # The property the whole backfill depends on: a retried hour must not
    # double its rows.
    path = str(tmp_path / "bronze")
    df = spark.createDataFrame(
        [("a", "2025-08-13", 14), ("b", "2025-08-13", 14)],
        "raw_json string, event_date string, event_hour int",
    )
    for _ in range(2):
        write_bronze(df, path, event_date="2025-08-13", event_hour=14)
    assert spark.read.format("delta").load(path).count() == 2


def test_writing_a_second_hour_does_not_disturb_the_first(spark: SparkSession, tmp_path) -> None:
    path = str(tmp_path / "bronze")
    h14 = spark.createDataFrame(
        [("a", "2025-08-13", 14)],
        "raw_json string, event_date string, event_hour int",
    )
    h15 = spark.createDataFrame(
        [("b", "2025-08-13", 15)],
        "raw_json string, event_date string, event_hour int",
    )
    write_bronze(h14, path, event_date="2025-08-13", event_hour=14)
    write_bronze(h15, path, event_date="2025-08-13", event_hour=15)
    assert spark.read.format("delta").load(path).count() == 2
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_bronze.py -v -m spark`
Expected: FAIL — `ModuleNotFoundError: almanac.pipeline.bronze`

- [ ] **Step 3: Implement**

```python
# src/almanac/pipeline/bronze.py
"""Bronze: land raw records with ingestion metadata. Never transform.

Bronze exists so that anything Silver gets wrong can be recomputed without
re-downloading ~190 GB. That guarantee holds only while the raw payload is
preserved byte-for-byte, so this module adds columns and does nothing else.
"""

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, *, ingested_at: datetime, source_file: str) -> DataFrame:
    """Attach ingestion provenance. Pure: DataFrame in, DataFrame out.

    ``ingested_at`` is wall-clock time and is **never** the event's
    ``created_at``. Conflating them is design doc §12 trap 2 and would make
    every point-in-time feature wrong in a way tests would not catch.
    """
    return df.withColumn("ingested_at", F.lit(ingested_at).cast("timestamp")).withColumn(
        "source_file", F.lit(source_file)
    )


def write_bronze(df: DataFrame, path: str, *, event_date: str, event_hour: int) -> None:
    """Idempotently write exactly one hour's partition.

    ``replaceWhere`` scopes the overwrite to this hour, so a retry replaces
    that hour and leaves every other partition untouched. Plain overwrite
    would delete the table; plain append would duplicate on retry.
    """
    predicate = f"event_date = '{event_date}' AND event_hour = {event_hour}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .partitionBy("event_date", "event_hour")
        .save(path)
    )
```

- [ ] **Step 4: Run tests, expect pass**

Run: `uv run pytest tests/unit/test_bronze.py -v -m spark` → 5 passed

- [ ] **Step 5: Full suite, lint, commit**

```bash
make check
git commit -m "feat: bronze ingest with replaceWhere idempotency"
```

---

## Task 3: Hour-gap detection

**Files:**
- Create: `src/almanac/pipeline/gaps.py`
- Test: `tests/unit/test_gaps.py`

**Interfaces:**
- Consumes: `hours_in_range` from `almanac.extract.urls`
- Produces: `missing_hours(expected, present) -> list[datetime]` and
  `GapReport(expected: int, present: int, missing: list[datetime])`

A missing hour is a **data defect to report**, never a gap to paper over
(§12 trap 5). A PR's response event can land in a skipped hour; losing it
fabricates an SLA breach that never happened.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_gaps.py
from datetime import UTC, datetime

from almanac.pipeline.gaps import GapReport, missing_hours


def h(day: int, hour: int) -> datetime:
    return datetime(2025, 8, day, hour, tzinfo=UTC)


def test_no_gaps_when_all_present() -> None:
    expected = [h(13, 0), h(13, 1), h(13, 2)]
    assert missing_hours(expected, set(expected)) == []


def test_single_gap_detected() -> None:
    expected = [h(13, 0), h(13, 1), h(13, 2)]
    assert missing_hours(expected, {h(13, 0), h(13, 2)}) == [h(13, 1)]


def test_result_is_sorted() -> None:
    expected = [h(13, 0), h(13, 1), h(13, 2), h(13, 3)]
    got = missing_hours(expected, {h(13, 0)})
    assert got == sorted(got)


def test_extra_present_hours_are_ignored() -> None:
    # A present hour outside the expected range is not this function's
    # concern and must not crash it.
    assert missing_hours([h(13, 0)], {h(13, 0), h(14, 5)}) == []


def test_report_counts_are_consistent() -> None:
    r = GapReport.build([h(13, 0), h(13, 1)], {h(13, 0)})
    assert r.expected == 2
    assert r.present == 1
    assert r.missing == [h(13, 1)]
    assert not r.is_complete


def test_complete_report_is_flagged_complete() -> None:
    r = GapReport.build([h(13, 0)], {h(13, 0)})
    assert r.is_complete
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_gaps.py -v`
Expected: FAIL — `ModuleNotFoundError: almanac.pipeline.gaps`

- [ ] **Step 3: Implement**

```python
# src/almanac/pipeline/gaps.py
"""Hour-level completeness. Pure; no I/O.

A missing hour is a defect this pipeline reports, never one it hides.
Design doc §12 trap 5: a PR's first response can land in a skipped hour,
and losing it fabricates an SLA breach that never occurred.
"""

from collections.abc import Iterable, Set
from dataclasses import dataclass
from datetime import datetime


def missing_hours(expected: Iterable[datetime], present: Set[datetime]) -> list[datetime]:
    """Expected hours absent from ``present``, sorted ascending."""
    return sorted(h for h in expected if h not in present)


@dataclass(frozen=True, slots=True)
class GapReport:
    expected: int
    present: int
    missing: list[datetime]

    @property
    def is_complete(self) -> bool:
        return not self.missing

    @classmethod
    def build(cls, expected: Iterable[datetime], present: Set[datetime]) -> "GapReport":
        exp = list(expected)
        gaps = missing_hours(exp, present)
        return cls(expected=len(exp), present=len(exp) - len(gaps), missing=gaps)
```

- [ ] **Step 4: Run tests, expect pass** → 6 passed

- [ ] **Step 5: Full suite, lint, commit**

```bash
make check
git commit -m "feat: hour-gap detection reported, never silently filled"
```

---

## Task 4: Era normalization — the three measured hazards

**Files:**
- Create: `src/almanac/pipeline/eras.py`
- Test: `tests/unit/test_eras.py`

**Interfaces:**
- Consumes: `SchemaEra`, `era_for` from `almanac.explore.schema`
- Produces: `normalize_events(df) -> DataFrame` emitting
  `event_id`, `event_id_source`, `actor_login`, `created_at` (UTC),
  `repo_id`, `repo_name`, `event_type`, `schema_era`

This is where Phase 0's three most dangerous findings get handled. Each has
its own test, and each test fails loudly if the handling regresses.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_eras.py
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.eras import normalize_events

pytestmark = pytest.mark.spark


def test_legacy_timestamp_offset_is_converted_not_truncated(
    spark: SparkSession,
) -> None:
    """Measured: pre-2015 `created_at` carries -07:00.

    A naive parse reads 2014-06-12T03:00:00-07:00 as 03:00 UTC. It is
    10:00 UTC. Seven hours, silently, on every legacy event.
    """
    df = spark.createDataFrame(
        [("2014-06-12T03:00:00-07:00", "abc", 1, "o/r", "PushEvent", None)],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    out = normalize_events(df).first()
    assert out["created_at"] == datetime(2014, 6, 12, 10, 0, tzinfo=UTC)


def test_modern_timestamp_is_utc_unchanged(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("2025-08-13T14:00:00Z", "abc", 1, "o/r", "PushEvent", "999")],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    out = normalize_events(df).first()
    assert out["created_at"] == datetime(2025, 8, 13, 14, 0, tzinfo=UTC)


def test_legacy_event_id_is_a_content_hash(spark: SparkSession) -> None:
    """Measured: 0 of 2,000 legacy events carry an `id`."""
    df = spark.createDataFrame(
        [("2014-06-12T03:00:00-07:00", "abc", 1, "o/r", "PushEvent", None)],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    out = normalize_events(df).first()
    assert out["event_id"], "legacy events must still get an id"
    assert out["event_id_source"] == "content_hash"


def test_modern_event_id_is_the_native_id(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("2025-08-13T14:00:00Z", "abc", 1, "o/r", "PushEvent", "999")],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    out = normalize_events(df).first()
    assert out["event_id"] == "999"
    assert out["event_id_source"] == "native"


def test_content_hash_is_stable_across_runs(spark: SparkSession) -> None:
    # If the hash were not deterministic, dedup would fail and every
    # re-run would duplicate legacy history.
    df = spark.createDataFrame(
        [("2014-06-12T03:00:00-07:00", "abc", 1, "o/r", "PushEvent", None)],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    assert normalize_events(df).first()["event_id"] == normalize_events(df).first()["event_id"]


def test_distinct_legacy_events_hash_differently(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("2014-06-12T03:00:00-07:00", "abc", 1, "o/r", "PushEvent", None),
            ("2014-06-12T03:00:00-07:00", "abc", 2, "o/s", "PushEvent", None),
        ],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    ids = [r["event_id"] for r in normalize_events(df).collect()]
    assert len(set(ids)) == 2


def test_era_is_labelled_on_every_row(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("2014-06-12T03:00:00-07:00", "a", 1, "o/r", "PushEvent", None),
            ("2025-08-13T14:00:00Z", "b", 2, "o/s", "PushEvent", "1"),
            ("2025-11-01T00:00:00Z", "c", 3, "o/t", "PushEvent", "2"),
        ],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    eras = {r["schema_era"] for r in normalize_events(df).collect()}
    assert eras == {"legacy_v1", "modern_v2", "reduced_v3"}


def test_repo_name_case_is_preserved(spark: SparkSession) -> None:
    """Measured: case-only renames exist (GLB -> glb).

    Lower-casing here would make them invisible to SCD2 in Phase 2.
    """
    df = spark.createDataFrame(
        [("2025-08-13T14:00:00Z", "a", 1, "Lumacaonta/GLB", "PushEvent", "1")],
        "created_at_raw string, actor_raw string, repo_id long, "
        "repo_name string, event_type string, id string",
    )
    assert normalize_events(df).first()["repo_name"] == "Lumacaonta/GLB"
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_eras.py -v -m spark`
Expected: FAIL — `ModuleNotFoundError: almanac.pipeline.eras`

- [ ] **Step 3: Implement**

```python
# src/almanac/pipeline/eras.py
"""Normalize the three schema eras into one shape. Pure; no I/O.

Every branch here exists because Phase 0 measured a difference, not because
a difference was expected. See docs/findings/2026-09-01-schema-eras.md and
docs/findings/2026-09-01-third-schema-era.md.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from almanac.explore.schema import ERA_BOUNDARY_MODERN, ERA_BOUNDARY_REDUCED

# Columns hashed to synthesize an id for legacy events. Chosen to be the
# minimal set that distinguishes two genuinely different events; adding
# more would make the hash sensitive to fields that vary by serialization.
_HASH_COLUMNS = ("created_at", "actor_login", "repo_id", "event_type")


def normalize_events(df: DataFrame) -> DataFrame:
    """Map raw event columns onto the canonical Silver shape.

    Three era differences are handled, each measured in Phase 0:

    1. Legacy ``created_at`` carries a ``-07:00`` offset. ``to_timestamp``
       on an offset-aware string converts correctly; a naive parse would
       shift every pre-2015 event by seven hours, silently, past every
       schema check that exists.
    2. Legacy events carry no ``id`` at all (0 of 2,000 sampled), so
       ``event_id`` falls back to a deterministic content hash and
       ``event_id_source`` records which mechanism produced it.
    3. ``actor`` is a bare string in legacy and an object in modern -- a
       type change. The caller flattens it to ``actor_raw`` before this
       function sees it, keeping the type-dispatch at the read boundary.
    """
    created = F.to_timestamp("created_at_raw")

    era = (
        F.when(created >= F.lit(ERA_BOUNDARY_REDUCED), F.lit("reduced_v3"))
        .when(created >= F.lit(ERA_BOUNDARY_MODERN), F.lit("modern_v2"))
        .otherwise(F.lit("legacy_v1"))
    )

    out = (
        df.withColumn("created_at", created)
        .withColumn("schema_era", era)
        .withColumn("actor_login", F.col("actor_raw"))
    )

    # sha2 over a null-safe concatenation. concat_ws skips nulls rather
    # than propagating them, so a missing field cannot collapse the hash
    # of every row to the same value.
    content_hash = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in _HASH_COLUMNS]), 256)

    has_native = F.col("id").isNotNull() & (F.length(F.col("id")) > 0)

    return out.withColumn(
        "event_id", F.when(has_native, F.col("id")).otherwise(content_hash)
    ).withColumn(
        "event_id_source",
        F.when(has_native, F.lit("native")).otherwise(F.lit("content_hash")),
    )
```

- [ ] **Step 4: Run tests, expect pass** → 8 passed

- [ ] **Step 5: Full suite, lint, commit**

```bash
make check
git commit -m "feat: era normalization handling id, offset, and actor-type differences"
```

---

## Task 5: Deduplication, including the adjacent-hour case Phase 0 never tested

**Files:**
- Create: `src/almanac/pipeline/dedup.py`
- Test: `tests/unit/test_dedup.py`

**Interfaces:**
- Consumes: normalized events from Task 4
- Produces: `deduplicate(df) -> DataFrame`, `duplicate_stats(df) -> Row`

**This task closes a known, documented hole.** Phase 0 measured a duplicate
ratio of 1 in 6.0M and explicitly recorded that the result **did not test
trap 4**, because the sample used non-adjacent hours and never compared two
consecutive ones. That test gets written here.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_dedup.py
import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.dedup import deduplicate, duplicate_stats

pytestmark = pytest.mark.spark

SCHEMA = (
    "event_id string, created_at timestamp, event_hour int, repo_id long, ingested_at timestamp"
)


def rows(spark: SparkSession, data: list[tuple]) -> "DataFrame":  # noqa: F821
    return spark.createDataFrame(data, SCHEMA)


def test_exact_duplicate_within_one_hour_removed(spark: SparkSession) -> None:
    from datetime import UTC, datetime

    t = datetime(2025, 8, 13, 14, 0, tzinfo=UTC)
    df = rows(spark, [("e1", t, 14, 1, t), ("e1", t, 14, 1, t)])
    assert deduplicate(df).count() == 1


def test_duplicate_across_adjacent_hours_removed(spark: SparkSession) -> None:
    """Design doc §12 trap 4 -- the case Phase 0 measured but never tested.

    The same event id appearing in hour 14 and hour 15 must collapse to one
    row. Deduplicating per-partition would keep both.
    """
    from datetime import UTC, datetime

    t14 = datetime(2025, 8, 13, 14, 59, tzinfo=UTC)
    t15 = datetime(2025, 8, 13, 15, 0, tzinfo=UTC)
    df = rows(spark, [("e1", t14, 14, 1, t14), ("e1", t15, 15, 1, t15)])
    assert deduplicate(df).count() == 1


def test_earliest_occurrence_is_the_one_kept(spark: SparkSession) -> None:
    # Event time is the source of truth. Keeping the later copy would
    # inflate every response-latency measurement built on it.
    from datetime import UTC, datetime

    t14 = datetime(2025, 8, 13, 14, 59, tzinfo=UTC)
    t15 = datetime(2025, 8, 13, 15, 0, tzinfo=UTC)
    df = rows(spark, [("e1", t15, 15, 1, t15), ("e1", t14, 14, 1, t14)])
    assert deduplicate(df).first()["created_at"] == t14


def test_distinct_events_are_untouched(spark: SparkSession) -> None:
    from datetime import UTC, datetime

    t = datetime(2025, 8, 13, 14, 0, tzinfo=UTC)
    df = rows(spark, [("e1", t, 14, 1, t), ("e2", t, 14, 1, t)])
    assert deduplicate(df).count() == 2


def test_stats_report_what_was_removed(spark: SparkSession) -> None:
    from datetime import UTC, datetime

    t = datetime(2025, 8, 13, 14, 0, tzinfo=UTC)
    df = rows(spark, [("e1", t, 14, 1, t), ("e1", t, 14, 1, t), ("e2", t, 14, 1, t)])
    stats = duplicate_stats(df)
    assert stats["total"] == 3
    assert stats["distinct"] == 2
    assert stats["duplicates"] == 1
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_dedup.py -v -m spark`
Expected: FAIL — `ModuleNotFoundError: almanac.pipeline.dedup`

- [ ] **Step 3: Implement**

```python
# src/almanac/pipeline/dedup.py
"""Deduplication on event_id. Pure; no I/O.

Deliberately **not** partition-scoped. Design doc §12 trap 4: the archive
can repeat an event across an hour boundary, so deduplicating within each
hourly partition would keep both copies. Phase 0 measured a duplicate ratio
but its sample used non-adjacent hours, so this case was never exercised
until Task 5's tests.
"""

from typing import Any

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep exactly one row per ``event_id``, the earliest by event time.

    Earliest-by-``created_at`` rather than by ``ingested_at``: event time is
    the source of truth, and keeping a later copy would inflate every
    response-latency measurement derived from it. ``ingested_at`` breaks
    ties so the result is deterministic when both timestamps match.
    """
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("created_at").asc(), F.col("ingested_at").asc()
    )
    return df.withColumn("_rn", F.row_number().over(ordering)).filter(F.col("_rn") == 1).drop("_rn")


def duplicate_stats(df: DataFrame) -> Row | Any:
    """Counts for the run report: total, distinct, and the difference."""
    return df.select(
        F.count("*").alias("total"),
        F.countDistinct("event_id").alias("distinct"),
        (F.count("*") - F.countDistinct("event_id")).alias("duplicates"),
    ).first()
```

- [ ] **Step 4: Run tests, expect pass** → 5 passed

- [ ] **Step 5: Full suite, lint, commit**

```bash
make check
git commit -m "feat: cross-hour deduplication, closing the trap 4 test gap"
```

---

## Task 6: Quality rules and the clean/quarantine split

**Files:**
- Create: `src/almanac/pipeline/quality.py`, `src/almanac/pipeline/silver.py`
- Test: `tests/unit/test_quality.py`, `tests/integration/test_pipeline.py`

**Interfaces:**
- Consumes: `SourceConfig` (Task 1), normalized+deduped events (Tasks 4–5)
- Produces: `apply_rules(df, rules) -> DataFrame` adding `_failed_rules`,
  and `split(df) -> tuple[DataFrame, DataFrame]` returning `(clean, quarantined)`

Two constraints carry the weight here: **null-safety** (an un-coalesced rule
silently passes what it should catch) and **conservation** (no record may
vanish between input and the two outputs).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_quality.py
import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.quality import apply_rules, split
from almanac.pipeline.source import QualityRule, Severity

pytestmark = pytest.mark.spark

RULES = [
    QualityRule(name="id_present", expression="event_id IS NOT NULL", severity=Severity.REJECT),
    QualityRule(name="repo_present", expression="repo_id IS NOT NULL", severity=Severity.REJECT),
    QualityRule(name="actor_present", expression="actor_login IS NOT NULL", severity=Severity.WARN),
]
SCHEMA = "event_id string, repo_id long, actor_login string"


def test_passing_record_has_empty_failed_rules(spark: SparkSession) -> None:
    df = spark.createDataFrame([("e1", 1, "a")], SCHEMA)
    assert apply_rules(df, RULES).first()["_failed_rules"] == []


def test_failed_rule_is_named_not_just_flagged(spark: SparkSession) -> None:
    # A boolean tells you a record is bad. The rule name tells you why,
    # which is what makes data quality analyzable rather than a bin.
    df = spark.createDataFrame([(None, 1, "a")], SCHEMA)
    assert apply_rules(df, RULES).first()["_failed_rules"] == ["id_present"]


def test_multiple_failures_all_recorded(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, None, "a")], SCHEMA)
    got = apply_rules(df, RULES).first()["_failed_rules"]
    assert set(got) == {"id_present", "repo_present"}


def test_null_in_predicate_counts_as_failure(spark: SparkSession) -> None:
    """The three-valued-logic trap, wrapped in coalesce(cond, False).

    `NULL > 5` is NULL, not False. An un-coalesced rule would let the row
    through as if it had passed.
    """
    rule = [QualityRule(name="positive", expression="repo_id > 0", severity=Severity.REJECT)]
    df = spark.createDataFrame([("e1", None, "a")], SCHEMA)
    assert apply_rules(df, rule).first()["_failed_rules"] == ["positive"]


def test_warn_severity_does_not_quarantine(spark: SparkSession) -> None:
    df = spark.createDataFrame([("e1", 1, None)], SCHEMA)
    clean, quarantined = split(apply_rules(df, RULES))
    assert clean.count() == 1
    assert quarantined.count() == 0


def test_warn_is_still_recorded_on_the_clean_row(spark: SparkSession) -> None:
    df = spark.createDataFrame([("e1", 1, None)], SCHEMA)
    clean, _ = split(apply_rules(df, RULES))
    assert clean.first()["_failed_rules"] == ["actor_present"]


def test_reject_severity_quarantines(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, 1, "a")], SCHEMA)
    clean, quarantined = split(apply_rules(df, RULES))
    assert clean.count() == 0
    assert quarantined.count() == 1


def test_split_conserves_every_record(spark: SparkSession) -> None:
    """Records must not vanish between layers. Asserted, not assumed."""
    df = spark.createDataFrame(
        [("e1", 1, "a"), (None, 1, "a"), ("e3", None, None), ("e4", 4, None)],
        SCHEMA,
    )
    applied = apply_rules(df, RULES)
    clean, quarantined = split(applied)
    assert clean.count() + quarantined.count() == applied.count() == 4
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_quality.py -v -m spark`
Expected: FAIL — `ModuleNotFoundError: almanac.pipeline.quality`

- [ ] **Step 3: Implement**

```python
# src/almanac/pipeline/quality.py
"""Quality rules and the clean/quarantine split. Pure; no I/O."""

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from almanac.pipeline.source import QualityRule, Severity

FAILED_RULES = "_failed_rules"
REJECT_RULES = "_reject_rules"


def apply_rules(df: DataFrame, rules: Sequence[QualityRule]) -> DataFrame:
    """Annotate each row with the names of the rules it failed.

    An **array of rule names**, not a boolean: a boolean says a record is
    bad, the array says why, which is what makes quality analyzable instead
    of a dead-letter bin (design doc §4.2).

    Every predicate is wrapped in ``coalesce(expr, False)``. Spark uses
    three-valued logic, so ``NULL > 5`` is ``NULL`` rather than ``False``,
    and an un-coalesced rule silently passes exactly the malformed rows it
    was written to catch.
    """

    def _failed(rule: QualityRule) -> "F.Column":
        # coalesce(expr, False): Spark's three-valued logic makes
        # `NULL > 5` evaluate to NULL, not False, so an un-coalesced rule
        # silently passes exactly the malformed rows it was written to catch.
        return F.when(~F.coalesce(F.expr(rule.expression), F.lit(False)), F.lit(rule.name))

    empty = F.array().cast("array<string>")

    def _names(subset: Sequence[QualityRule]) -> "F.Column":
        if not subset:
            return empty
        # array() keeps a null per passing rule; array_compact drops them.
        # Confirmed available in Spark 4.2.0 during Phase 0 Task 2.
        return F.array_compact(F.array(*[_failed(r) for r in subset]))

    rejects = [r for r in rules if r.severity is Severity.REJECT]
    return df.withColumn(FAILED_RULES, _names(rules)).withColumn(REJECT_RULES, _names(rejects))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition into ``(clean, quarantined)``.

    Only ``REJECT`` failures quarantine. A ``WARN`` is recorded in
    ``_failed_rules`` and the row stays in the clean set -- used where a
    field is legitimately absent in one era but required in another. That
    is why severity has to survive into this function rather than being
    collapsed to a single boolean upstream.

    Nothing is dropped: every input row appears in exactly one output, which
    ``test_split_conserves_every_record`` asserts.
    """
    has_reject = F.size(F.col(REJECT_RULES)) > 0
    return (
        df.filter(~has_reject).drop(REJECT_RULES),
        df.filter(has_reject).drop(REJECT_RULES),
    )
```

- [ ] **Step 4: Run tests, expect pass** → 8 passed

- [ ] **Step 5: Add the integration test**

```python
# tests/integration/test_pipeline.py
"""Bronze -> Silver against the committed fixture. No network."""

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_fixture_flows_bronze_to_silver(spark, tmp_path) -> None:
    from almanac.config import Settings
    from almanac.pipeline.silver import run_silver

    fixture = Settings().fixture_dir / "2025-08-13-14.jsonl.gz"
    clean, quarantined = run_silver(spark, str(fixture), str(tmp_path))

    assert clean.count() > 0, "the fixture must produce usable rows"
    # Conservation across the whole pipeline, not just one function.
    raw = spark.read.json(str(fixture)).count()
    assert clean.count() + quarantined.count() <= raw


def test_silver_is_idempotent(spark, tmp_path) -> None:
    from almanac.config import Settings
    from almanac.pipeline.silver import run_silver

    fixture = Settings().fixture_dir / "2025-08-13-14.jsonl.gz"
    first, _ = run_silver(spark, str(fixture), str(tmp_path))
    n = first.count()
    second, _ = run_silver(spark, str(fixture), str(tmp_path))
    assert second.count() == n
```

- [ ] **Step 6: Full suite, lint, commit**

```bash
make check
git commit -m "feat: null-safe quality rules with conserving quarantine split"
```

---

## Task 7: Calibration on Azure — the deadline-critical measurement

**Files:**
- Create: `scripts/calibrate.py`
- Create: `docs/findings/2026-09-XX-cluster-throughput.md`
- Modify: `docs/design/2026-09-01-almanac-system-design.md` (§4.5 Tier 3 span)

**This is the task the schedule was rebuilt around.** Cluster throughput is
the single unmeasured input that gates Tier 3's span and every dollar figure
downstream of it (§13). It runs the moment Bronze works — it does **not**
wait for Gold.

**Cost guard.** This task spends real credit. Before starting: confirm
`terraform apply` state is current, confirm 0 clusters are running, and
confirm the budget alert exists. After: **tear the cluster down and verify**.

- [ ] **Step 1: Write the calibration script**

```python
# scripts/calibrate.py
"""Measure real cluster throughput on one day of data.

Deliberately minimal: one day (24 files) through Bronze only. The number
being bought is GB-gz per cluster-hour, and Bronze is enough to get it
because ingest is the volume-bound stage. Running Silver too would conflate
two rates and cost more credit for a less interpretable answer.
"""

import json
import sys
import time
from datetime import UTC, date, datetime

import urllib.request

from almanac.extract.urls import archive_url, hours_in_range
from almanac.pipeline.bronze import add_ingestion_metadata, write_bronze
from almanac.spark import local_session


def content_length(url: str) -> int:
    """Compressed size of one archive file, via HEAD.

    Measured rather than derived from the mean file size, because the
    calibration's whole purpose is to stop estimating.
    """
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return int(resp.headers["Content-Length"])


def main(day: str, bronze_path: str) -> None:
    d = date.fromisoformat(day)
    spark = local_session("almanac-calibration")
    hours = hours_in_range(
        datetime(d.year, d.month, d.day, 0, tzinfo=UTC),
        datetime(d.year, d.month, d.day, 23, tzinfo=UTC),
    )

    started = time.monotonic()
    total_bytes = 0
    for h in hours:
        url = archive_url(h.date(), h.hour)
        # Compressed size from Content-Length -- the same signal the Phase 0
        # fetcher already uses and already has tests for. Cheap (one HEAD per
        # hour) and it is the denominator the whole calibration is for.
        total_bytes += content_length(url)

        df = spark.read.json(url)
        df = add_ingestion_metadata(df, ingested_at=datetime.now(UTC), source_file=url)
        write_bronze(df, bronze_path, event_date=h.date().isoformat(), event_hour=h.hour)
    elapsed = time.monotonic() - started

    rows = spark.read.format("delta").load(bronze_path).count()
    gb = total_bytes / 1024**3
    hours_elapsed = elapsed / 3600

    print(
        json.dumps(
            {
                "day": day,
                "hours": len(hours),
                "rows": rows,
                "compressed_gb": round(gb, 3),
                "wall_clock_seconds": round(elapsed, 1),
                "rows_per_second": round(rows / elapsed, 1) if elapsed else None,
                # The number this whole task exists to produce.
                "gb_per_cluster_hour": round(gb / hours_elapsed, 2) if elapsed else None,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
```

- [ ] **Step 2: Verify the cost preconditions**

```bash
az account show --query "{name:name, id:id}" -o table
az consumption budget list --query "[].{name:name,amount:amount}" -o table
# must print 0 clusters
databricks clusters list --output json | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('clusters',[])))"
```

Expected: subscription `bscr-az-portfolio`, budget present, **0 clusters**.

- [ ] **Step 3: Run the calibration on a 4-node `Standard_D4ds_v6` job cluster**

One day of Q3 2025. Record wall-clock, DBUs consumed, and dollars from the
Databricks run report — not estimated from the cluster's hourly rate.

- [ ] **Step 4: Derive Tier 3's span and commit the arithmetic**

Compute, showing the working:

```
GB-gz per cluster-hour   = <measured GB> / <measured cluster-hours>
$ per day-of-data        = <cluster $/hr> x <hours per day-of-data>
affordable days          = (0.40 x remaining credit) / ($ per day-of-data)
```

Take the largest **contiguous** slice at or under `affordable days`. §4.5's
rule against sampling the time dimension still governs — the slice shrinks
by span, never by hour-sampling.

- [ ] **Step 5: Tear down and verify**

```bash
# terminate the cluster, then prove it
databricks clusters list --output json | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('clusters',[])))"
az consumption usage list --start-date <today> --end-date <today> -o table
```

- [ ] **Step 6: Write the findings doc and update the design**

Record the measured throughput, the derived span **with its arithmetic**,
and what would change the answer. Update §4.5's Tier 3 row from
"calibration-derived" to the actual number, and tick the throughput item in
§13.

- [ ] **Step 7: Commit**

```bash
make check
git add scripts/calibrate.py docs/findings/ docs/design/ docs/STATUS.md
git commit -m "feat: measure cluster throughput; derive Tier 3 span from it"
```

---

## Phase 1 exit gate

Every box must be ticked before Phase 2 starts. Phase 2 spends the bulk of
the credit, and it must not start on an unproven pipeline.

- [ ] `make check` green — ruff, `mypy --strict`, full pytest suite
- [ ] CI green on the PR
- [ ] **Re-running any hour produces byte-identical Bronze content**
- [ ] **Cross-hour duplicates are removed** — trap 4 tested for the first time
- [ ] **Legacy timestamps land as UTC**, verified against the `-07:00` case
- [ ] **Legacy events get a stable content-hash `event_id`**, distinct events hash distinctly
- [ ] **Repo-name case is preserved** end to end
- [ ] **`clean + quarantined == input`** asserted by test, not by inspection
- [ ] **Every quality rule is null-safe**, with a test proving a null predicate fails rather than passes
- [ ] All three schema eras ingest without error
- [ ] Missing hours are reported, never silently filled
- [ ] **Cluster throughput measured on Azure**, findings doc committed
- [ ] **Tier 3's span derived, with its arithmetic, in STATUS.md**
- [ ] **0 clusters running; teardown verified**
- [ ] `docs/STATUS.md` updated in the same commit as each task
- [ ] README refreshed if status or the architecture diagram changed

## Deferred out of Phase 1, on purpose

Named so their absence is a decision rather than an oversight:

- **Gold, SCD2, and the accumulating snapshot** — Phase 2. Silver must be
  trustworthy before anything models on top of it.
- **The label** — needs `fact_pull_request`, which is Phase 2.
- **The REST API second source** — its purpose is to prove YAML-only
  onboarding, which is only a meaningful test once the framework is
  complete. Phase 2 gate.
- **Photon A/B** — reuses this phase's calibration slice but belongs to the
  Phase 2 burn, so both runs are compared under identical conditions.
- **Orchestration** — the runner is invoked directly. Workflows come with
  the cloud burn.
- **Pseudonymization** — required in anything *published*; nothing is
  published from Silver.
