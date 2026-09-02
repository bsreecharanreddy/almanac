# Phase 2 — Gold + the Azure Burn Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Kimball Gold layer built in dbt on Delta — SCD2 `dim_repo`,
an accumulating-snapshot `fact_pull_request` carrying the measured label,
and `agg_repo_daily` — proven on a second source that onboards by YAML,
then run across the full Q3 2025 quarter on Azure job clusters before the
credit expires.

**Architecture:** Bronze and Silver stay PySpark (pure
`DataFrame -> DataFrame` transforms, I/O at the edges). Gold is **dbt**,
per design §8, running against Delta through dbt-spark's `session` method
locally and `dbt-databricks` on the cluster. Facts are built from the
**event stream**, never from embedded payloads (§4.3a).

**Tech Stack:** Python 3.12+, PySpark 4.2.0, Delta Lake 4.4.0,
**dbt-core 1.12.3 + dbt-spark 1.11.0 (`session`)**, dbt-databricks 1.12.5,
Pydantic v2, `pytest`, `chispa`, `ruff`, `mypy --strict`, Azure Databricks.

**Spec:** `docs/design/2026-09-01-almanac-system-design.md` — §4.2 Silver,
§4.3 Gold, §4.3a event-native facts, §4.5 dataset scope, §4.5a second
source, §5.1 the label, §8 stack, §8.2 Photon, §9 phasing, §12 traps.

**Window:** Sep 9–20. **The credit expires 2026-09-24.** This is the only
phase besides Phase 1 that is deadline-bound; nothing here may slip right.

---

## Global Constraints

Copied from the spec and `CLAUDE.md`. Every task's requirements implicitly
include these.

- **Never quote a number that was not measured.**
- **Point-in-time correctness.** Every fact and every label uses only
  events with `created_at` at or before the moment being described. A
  `MAX()` over a whole partition is the easiest way to break this.
- **Facts come from event-level fields, never embedded payload objects**
  (§4.3a). `payload.action`, `payload.number` and the event's own
  `created_at`/`actor` are in bounds; `payload.pull_request.created_at`
  and `.user` are not.
- **Every write is idempotent.** `replaceWhere` on partitions for
  Bronze/Silver, `MERGE` for dimensions and accumulating snapshots.
- **Null-safe equality (`<=>`) in every SCD2 comparison.** Plain `<>`
  misses null-to-value transitions, which is exactly what a newly
  populated column looks like.
- **Repo-name comparison is case-sensitive.** Case-only renames are
  measured to exist (§12 trap 8).
- **Quarantine, never drop.** Failures carry a `_failed_rules` array.
- **Assert the split**, never assume it.
- **Pure transforms separated from I/O.**
- **Spark session timezone is UTC, always.**
- **Job clusters only. `terraform destroy` between sessions.**
- **`docs/STATUS.md` updates in the same commit as the work it describes.**
- **One commit per completed task**, each with its own green full-suite run.
- **If reality contradicts the design doc, reality wins** — correct the
  design doc in the same commit and note it in the STATUS verification log.

---

## Two Phase 1 defects this plan must fix before any Gold work

Both were found while writing this plan, by reading the committed code
rather than the Phase 1 plan's description of it. Neither is caught by the
existing suite, because both are invisible at a sample size of one file —
which is all Phase 1 ever ran.

### Defect A — Silver overwrites the entire table on every file

`run_silver` writes:

```python
df.write.format("delta").mode("overwrite").save(f"{output_path}/{subdir}")
```

No `partitionBy`, no `replaceWhere`. **Every hour processed destroys every
hour already written.** Bronze got this right in Task 2 and Silver did not,
and no test caught it because Silver was only ever run against a single
fixture, where "overwrite the table" and "replace this partition" are the
same operation.

At Tier 3's 2,208 files, Silver would finish holding one hour of data —
and a count-based check on the final table would look plausible.
`SILVER_COLUMNS` also carries no `event_date`/`event_hour`, so there is
nothing to partition on yet. **Task 1 fixes both.**

### Defect B — Silver reads the raw archive file, not Bronze

`run_silver` takes a `source_path` and calls `spark.read.json(path)` on
the original `.json.gz`. Bronze's output is never read by anything.

That makes the medallion a claim rather than a structure, and it costs
real money at Tier 3: Bronze's whole purpose (per `bronze.py`'s own
docstring) is that a reprocess does not re-download ~190 GB. Silver
re-parsing source files means every Silver change re-fetches the quarter.

It also contradicts `CLAUDE.md` directly — *"Bronze never transforms.
Payload stays a JSON string; parsing is per-type in Silver."* Bronze
already stores `raw_json` as text (corrected in Task 7 when per-hour
schema inference broke the calibration). Silver must parse **from that
column**, which is also what makes per-type parsing possible at all.

---

## Measured facts this phase must respect

Phase 0 and Phase 1 findings still bind. These are **new**, measured while
writing this plan — against the committed fixtures and a real dbt spike,
not inferred from documentation.

### Fixture probe — both eras, 2,000 events each

| Fact | Measurement | Consequence |
|---|---|---|
| **PR identity survives all three eras** | `payload.number` on 149/149 modern `PullRequestEvent`; `number` is one of the **5 fields that survived** the Oct-2025 reduction (`base`, `head`, `id`, `number`, `url`) | `(repo_id, pr_number)` is the fact's natural key in every era. It is the one PR field the reduction did not take |
| **`PullRequestReviewEvent` does not exist in the legacy era** | **0** of 106 legacy PRs, vs 68 modern | The label's primary component is **structurally absent pre-2015**. Tier 2 (2014) can be ingested and dimensioned but its label rests on the other two components only. This is not currently stated anywhere in the design doc |
| **The broadened label is what makes legacy workable at all** | legacy has 42 `PullRequestReviewCommentEvent` + 194 `IssueCommentEvent` | §5.1 broadened the label for a coverage reason. It turns out to also be the reason the label survives the era boundary — a second, independent argument for a decision already made |
| **`draft` does not exist in the legacy era** | `None` on all 106 legacy PRs; 5 of 149 modern are drafts | §5.1's draft-exclusion rule is era-bound and must be null-safe, not `= false` |
| **`issue.pull_request` discriminates PR comments from issue comments** | present on **55 of 92** modern `IssueCommentEvent` (59.8%) | The label counts an `IssueCommentEvent` only when the issue **is** a PR. GitHub shares one number sequence between issues and PRs per repo, so a join on `(repo_id, number)` cannot collide — but the flag is the explicit test and costs nothing |
| **`payload.pull_request` has three widths, not two** | **36** keys legacy, **48** modern, **5** reduced | `merged` exists in legacy (35 true / 14 false on closed) and in modern, and is gone in reduced. Era dispatch for payload fields is three-way |

### dbt-on-Delta spike — run for real, 2026-09-01

The stack table's dbt choice was never executed. It is now, before any
plan depends on it. `dbt-core 1.12.3` + `dbt-spark[session] 1.11.0` +
`pyspark 4.2.0` + `delta-spark 4.4.0` installed with **no dependency
conflict** (`dbt-spark` declares `pyspark<5.0.0,>=3.0.0`).

**It works — incremental `merge` and SCD2 snapshots both — but only with a
persistent metastore, and the failure mode is silent.**

| Run | Metastore | `dim_repo` after a rename | Delta op |
|---|---|---|---|
| First attempt | in-memory (default) | 3 rows, **all current**, no closed row | `CREATE OR REPLACE TABLE AS SELECT` |
| With `enableHiveSupport()` + on-disk Derby | persistent | repo 2 old row **closed**, new row current, exactly one current per repo | **`MERGE`** — 1 updated, 2 inserted |

**dbt reported `success=True` in both cases.** With an ephemeral metastore
dbt cannot see the existing relation, so `is_incremental()` is false and
every model silently falls back to a full rebuild — producing a table that
is the right shape, plausibly populated, and wrong. An SCD2 dimension that
has quietly become a snapshot of "now" is exactly the defect this project
exists to demonstrate competence against.

**Therefore the local dbt target is not optional configuration.** It is:

```python
.enableHiveSupport()
.config("javax.jdo.option.ConnectionURL",
        f"jdbc:derby:;databaseName={metastore_path};create=true")
```

and **Task 2 writes a regression test that fails if a Gold model ever
executes as `CREATE OR REPLACE` on a second run** — asserted from the
Delta commit log's `operation` field, not from row counts, because row
counts looked correct in the broken run.

---

## File structure created by this phase

| File | Responsibility |
|---|---|
| `src/almanac/pipeline/silver.py` | **Modified.** Reads Bronze; partitioned `replaceWhere` writes |
| `src/almanac/pipeline/payloads.py` | **Pure.** Per-type payload parsing, three-way era dispatch |
| `src/almanac/spark.py` | **Modified.** Persistent metastore for the dbt-capable session |
| `dbt/dbt_project.yml`, `dbt/profiles.yml` | dbt project; `session` and `databricks` targets |
| `dbt/models/sources.yml` | Silver Delta tables declared as dbt sources |
| `dbt/snapshots/dim_repo.sql` | SCD2 dimension, `check` strategy, case-sensitive |
| `dbt/models/gold/fact_pull_request.sql` | Accumulating snapshot, incremental `merge` |
| `dbt/models/gold/agg_repo_daily.sql` | Aggregate mart |
| `dbt/models/gold/schema.yml` | Contracts + `not_null`/`unique`/`relationships` |
| `dbt/tests/` | Singular tests: SCD2 invariants, label point-in-time |
| `conf/sources/github_rest.yml` | Second source — **YAML only**, the Phase 2 gate |
| `src/almanac/gold/runner.py` | Invokes dbt; the I/O boundary for Gold |
| `scripts/backfill.py` | Tier 3 orchestration over the derived span |
| `tests/unit/test_payloads.py`, `tests/unit/test_gold_*.py` | Unit tests |
| `tests/integration/test_medallion.py` | Bronze→Silver→Gold on fixtures |
| `docs/findings/2026-09-…-photon-ab.md` | The Photon result, positive or null |
| `docs/findings/2026-09-…-tier3-backfill.md` | The burn: cost, throughput, defects |

---

## Task 1: Silver reads Bronze, partitions its writes, and keeps the payload

**Files:**
- Modify: `src/almanac/pipeline/silver.py`, `src/almanac/pipeline/eras.py`
- Create: `src/almanac/pipeline/payloads.py`
- Test: `tests/unit/test_payloads.py`, `tests/unit/test_silver_writes.py`

**Interfaces:**
- `read_bronze(spark, path, *, event_date, event_hour) -> DataFrame`
- `parse_payload(df, *, era) -> DataFrame` (pure)
- `run_silver(...)` gains partitioned `replaceWhere` writes

**This task closes Defects A and B and is a hard precondition for every
other task.** Gold cannot be built on a Silver that carries no payload,
and the backfill cannot run on a Silver that overwrites itself.

- [ ] **Step 1: Write the failing tests**

Three, each pinning one defect:

```python
def test_second_hour_does_not_destroy_the_first(spark, tmp_path):
    """Defect A. Two hours written in sequence; both must survive."""
    run_silver(spark, hour_a, out, ingested_at=T, config=cfg)
    run_silver(spark, hour_b, out, ingested_at=T, config=cfg)
    dates = {r["event_hour"] for r in spark.read.format("delta").load(clean).collect()}
    assert dates == {14, 15}  # fails today: {15}


def test_rerunning_one_hour_replaces_only_that_partition(spark, tmp_path):
    """Idempotency, at partition scope rather than table scope."""


def test_silver_parses_from_bronze_raw_json(spark, tmp_path):
    """Defect B. Silver's input is a Bronze table, not a .json.gz path."""
```

- [ ] **Step 2: Add `event_date`/`event_hour` to `SILVER_COLUMNS`**

Derived from `created_at`, not from the filename — a late-arriving event
belongs to the hour it happened in. Both go in `SILVER_COLUMNS`; the
docstring already explains why that tuple is the enforced contract.

- [ ] **Step 3: Parse from Bronze's `raw_json`**

`spark.read.format("delta").load(bronze).selectExpr("from_json(raw_json, schema)")`.
The schema is **explicit, not inferred** — Task 7 already measured that
per-file inference disagrees between hours of the same day, which is what
broke the calibration's first Delta write. Era dispatch reads the parsed
struct exactly as `is_legacy` does today.

- [ ] **Step 4: Per-type payload columns**

`payloads.py`, pure, three-way era dispatch. Minimum for §5.1's label:
`action`, `pr_number`, `is_pr_comment`, `merged`, `draft`. `merged` and
`draft` are null in the eras that lack them — **null, never `false`**,
because "not a draft" and "the era had no drafts" are different facts and
collapsing them makes the legacy slice look like 106 non-draft PRs.

- [ ] **Step 5: Partitioned writes**

`.partitionBy("event_date", "event_hour")` with `replaceWhere` scoped to
the hour, mirroring `bronze.py`.

- [ ] **Step 6: Verify** — `make check` green; then **mutate**: revert
  `replaceWhere` to `mode("overwrite")` and confirm exactly the Defect A
  test reddens.

---

## Task 2: dbt scaffold on Delta, with the silent-rebuild regression test

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/models/sources.yml`
- Modify: `src/almanac/spark.py`, `pyproject.toml`, `Makefile`, CI workflow
- Test: `tests/integration/test_dbt_target.py`

**Interfaces:**
- `dbt_session(warehouse, metastore) -> SparkSession` — Hive support on
- Two dbt targets: `session` (local, CI) and `databricks` (the burn)

- [ ] **Step 1: Write the failing test — the one the spike earned**

```python
def test_gold_model_merges_on_second_run_rather_than_rebuilding(dbt_project):
    """A silent CREATE OR REPLACE is the failure this test exists for.

    Asserted from the Delta commit log, not from row counts: in the broken
    run the row counts were correct and the table was still wrong.
    """
    dbt("run")
    dbt("run")
    ops = [c["operation"] for c in delta_history("gold.fact_pull_request")]
    assert ops[0] == "CREATE OR REPLACE TABLE AS SELECT"
    assert "MERGE" in ops[1:]
```

- [ ] **Step 2: Persistent metastore in `spark.py`**

`enableHiveSupport()` plus an on-disk Derby path. Document *why* in the
docstring, in the same register as `configure_spark_with_delta_pip`'s
existing note — this is a correctness requirement wearing a configuration
costume.

- [ ] **Step 3: dbt project + both targets.** `session` for local and CI;
  `databricks` reading host/token/warehouse from env for the burn.
  Nothing about the models differs between targets.

- [ ] **Step 4: Declare Silver as dbt sources** with freshness where it
  is meaningful.

- [ ] **Step 5: Wire `make dbt` and CI.** dbt runs in CI on fixtures.
  **CI is the only place the metastore config is exercised on a clean
  machine** — Phase 1's Task 2 finding was that a UTC CI runner is
  structurally blind to timezone defects; here the asymmetry runs the
  other way, and a laptop with a warm metastore is blind to this one.

- [ ] **Step 6: Verify** — `make check` + `make dbt` green.

---

## Task 3: `dim_repo` — SCD2, case-sensitive, null-safe

**Files:** `dbt/snapshots/dim_repo.sql`, `dbt/tests/assert_dim_repo_scd2.sql`

- [ ] **Step 1: Failing tests** — three invariants, from the testing policy:
  - a rename closes the old row and opens exactly one current row
  - **exactly one `dbt_valid_to IS NULL` per `repo_id`**, always
  - a **case-only** rename (`GLB` → `glb`) is detected — measured to exist
  - a repo renamed **twice** yields three versions, not two (§12 trap 8
    measured one in the window)

- [ ] **Step 2: The snapshot.** `strategy='check'`, `check_cols=['repo_name']`,
  `file_format='delta'`. Verified working in the spike.

- [ ] **Step 3: Case sensitivity.** Spark string comparison is
  case-sensitive by default; the test exists so a later "helpful"
  `lower()` cannot pass silently.

- [ ] **Step 4: Null-safety.** Any comparison added later uses `<=>`.
  A repo whose name goes null (deleted repos, §12 trap 11) is a
  transition, not a non-event.

- [ ] **Step 5: Verify**, then **mutate**: change `check_cols` comparison
  to case-insensitive and confirm the case-only test reddens.

---

## Task 4: `fact_pull_request` — accumulating snapshot, event-native

**Files:** `dbt/models/gold/fact_pull_request.sql`, `dbt/models/gold/schema.yml`

**The construction rule from §4.3a is the whole point of this task.**
Every column comes from the event stream. The payload-native version is
easier and stops working in October 2025.

| Column | Event-native source |
|---|---|
| `opened_at` | the `opened` event's own `created_at` |
| `author_login` | the `opened` event's `actor_login` |
| `closed_at` | the `closed` event's `created_at` |
| `first_review_at` | earliest `PullRequestReviewEvent` |
| `merged` / `draft` | payload, **era-bound**, null where the era lacks it |

- [ ] **Step 1: Failing tests**
  - **out-of-order arrival preserves the earliest timestamp** — the
    defining property of an accumulating snapshot, and the one a
    `MERGE ... UPDATE SET` clobbers by default
  - one row per `(repo_id, pr_number)`, asserted as `unique`
  - a PR whose events span two hourly files accumulates rather than
    duplicating
  - **no column reads `payload.pull_request.created_at`** — a grep-style
    test, because §4.3a is a rule a future edit will quietly break

- [ ] **Step 2: Incremental merge model.** `unique_key=['repo_id','pr_number']`,
  `incremental_strategy='merge'`.

- [ ] **Step 3: `least()` on every timestamp update**, not assignment.
  Out-of-order batches are the normal case at 2,208 files, not an edge.

- [ ] **Step 4: Right-censoring.** PRs open at the window edge get a
  `is_censored` flag rather than a fabricated outcome (§5.1). The
  censoring rate is **reported**, not hidden.

- [ ] **Step 5: Verify** — including that the fixture's 149 modern PR
  events produce the expected distinct PR count.

---

## Task 5: The label, and `agg_repo_daily`

**Files:** `dbt/models/gold/fact_pull_request.sql` (extended),
`dbt/models/gold/agg_repo_daily.sql`, `dbt/tests/assert_label_point_in_time.sql`

**The label is `time_to_first_human_response`** — §5.1, measured, not
assumed. The earliest of `PullRequestReviewEvent`,
`PullRequestReviewCommentEvent`, or an `IssueCommentEvent` on an issue
that **is** a PR, **whose actor is not the PR author**.

- [ ] **Step 1: Failing tests**
  - a comment by the PR author is **not** a first response
  - an `IssueCommentEvent` on a genuine issue (no `issue.pull_request`)
    never lands on a PR row
  - **the label never reads an event at or after its own `as_of`** — the
    governing principle, tested here for the first time
  - a **legacy** PR still gets a label from the two components that era
    has — the measured 0-review-events case
  - a **draft** PR accrues no SLA time, and a legacy PR (null `draft`) is
    not silently treated as a draft

- [ ] **Step 2: Author exclusion.** `author_login` comes from the `opened`
  event (Task 4), so a PR with no observed `opened` event has a null
  author — and then the exclusion cannot be evaluated. Those rows are
  **excluded with the exclusion stated**, exactly as §5.1 handles
  right-censoring. Silent inclusion would count self-comments as
  responses.

- [ ] **Step 3: `is_bot` as a first-class column** (§5.1) using the
  **corrected** anchored heuristic from
  `docs/findings/2026-09-01-bot-classification.md` — the bare `ci$` rule
  matched 869 human surnames. Bots are never dropped; metrics segment by
  the flag.

- [ ] **Step 4: `agg_repo_daily`.** Daily grain per repo. **Star counts
  are "stars gained", never "total stars"** (§12 traps 1–2: `WatchEvent`
  means star, and there are no un-star events). Push volume uses
  `payload.size`, **never `size(commits)`** — the array is capped at 20
  (trap 3).

- [ ] **Step 5: Verify.**

---

## Task 6: Contracts and dbt tests in CI

**Files:** `dbt/models/gold/schema.yml`, `dbt/tests/`, CI workflow

- [ ] **Step 1:** `not_null` / `unique` / `relationships` /
  `accepted_values` on every Gold model. `relationships` from
  `fact_pull_request.repo_id` to `dim_repo`.
- [ ] **Step 2:** dbt model contracts (enforced column types), so a
  schema change fails the build rather than the reader.
- [ ] **Step 3:** **The contract fails the build, not a document** —
  CLAUDE.md's testing table. `dbt build` runs in CI on fixtures.
- [ ] **Step 4:** Verify a deliberately broken contract actually reddens CI.

---

## Task 7: The GitHub REST API second source — a falsifiable gate

**Files:** `conf/sources/github_rest.yml`, and **whatever else proves
necessary — which is the measurement.**

**§4.5a's claim is that a new source onboards via YAML alone, zero new
Python.** This task tests that claim rather than assuming it. The REST API
is paginated, authenticated and rate-limited at 5,000 req/hr; a gzip file
fetch is none of those. **The honest prior is that it will need new
Python.**

- [ ] **Step 1: Write the YAML first, before any code.** If it is
  sufficient, the gate passes as designed.
- [ ] **Step 2: Record precisely what the YAML could not express.** Every
  line of new Python required is the finding. Pagination, auth, and
  rate-limit backoff are the three candidates.
- [ ] **Step 3: Publish the result either way**, in the register §8.2
  uses for Photon: *"the config-driven framework onboards a file source
  by YAML and needs N lines of Python for a paginated API"* is a more
  credible statement than an unfalsifiable claim of generality. A partial
  pass is the most likely and most interesting outcome.
- [ ] **Step 4: Enrichment only where the firehose is lossy** — `merged`,
  `draft`, `title`, size — for a **bounded** repo set. 5,000 req/hr is a
  hard ceiling and the repo set is chosen to fit it, measured not guessed.
- [ ] **Step 5: Tests never touch the network.** Committed fixtures, as
  everywhere else in this repo.

---

## Task 8: Orchestration and the Tier 3 backfill — the burn

**Files:** `scripts/backfill.py`, `infra/terraform/` (Workflows), findings doc

**Tier 3 is the full Q3 2025 quarter — 2025-07-01 to 2025-09-30, 92 days,
2,208 files, ~185 GB gz, $31.71 at 17.2% of the credit.** Derived in
Phase 1 with its arithmetic in STATUS.md. It is not re-litigated here.

- [ ] **Step 1: Parallelise the fetch.** Phase 1 measured **174.3 s of
  523.5 s billed — a third — as single-threaded HTTP download**. At 92×
  that is the single largest lever available, and it is already
  identified in STATUS.md's carry-forward list.
- [ ] **Step 2: Databricks Workflows**, job clusters only, defined in
  Terraform. Deferred out of Phase 1 explicitly; it lands here.
- [ ] **Step 3: Checkpoint per day.** A failure at day 60 resumes at day
  60. `replaceWhere` (Task 1) is what makes a resumed day safe.
- [ ] **Step 4: Gap report per day** — `gaps.py` already exists and
  already reported 24/24 on the calibration. Missing hours are recorded,
  **never** silently filled (§4.5: sampling the time dimension is
  forbidden, and a gap in the label window fabricates SLA breaches).
- [ ] **Step 5: Watch the two named cost risks.** The dedup window is a
  full shuffle keyed on `event_id` and is the candidate for dominant cost
  at 92 days rather than 1. **The measured 13.84 GB/cluster-hour is
  Bronze-only**; the full medallion will be slower, and the 2.3× headroom
  is exactly what absorbs it. **If the real rate makes the quarter exceed
  40% of the credit, Tier 3 shrinks. That is the rule working, not a
  failure** (§4.5).
- [ ] **Step 6: Measure and publish** — GB/cluster-hour per layer, cost
  per run, defects found. Compare against Phase 1's Bronze-only figure
  and state the difference plainly.
- [ ] **Step 7: Verify teardown.** `active clusters: 0`.

---

## Task 9: The Photon A/B, teardown, and phase wrap-up

**Files:** `docs/findings/2026-09-…-photon-ab.md`, STATUS.md, README, design doc

**The hypothesis is pre-registered in §8.2 and is not to be edited after
seeing the result:** Photon helps silver and gold materially, helps bronze
ingest little or not at all because bronze is almost entirely JSON parsing
— the case vendor docs name as only partially covered — and the blended
result may be a wash or a loss.

- [ ] **Step 1: Same slice, same cluster shape, same code, one variable.**
  The slice is Phase 1's calibration day, so both arms are comparable to
  a number that already exists.
- [ ] **Step 2: Per-layer, never blended.** §8.2's table has a row per
  layer specifically because a blended number would hide the predicted
  effect.
- [ ] **Step 3: Record wall-clock, DBUs consumed, and $/GB per layer.**
  The Photon meter bills at the same $/DBU but consumes more DBUs per
  hour — measured, §8.2 — so wall-clock alone answers nothing.
- [ ] **Step 4: Publish the result including if negative.** Reporting it
  only when favourable would make the measurement worthless. A null
  result closes an open item in §13 either way.
- [ ] **Step 5: `terraform destroy`**, verified empty.
- [ ] **Step 6: Refresh all three reader-facing artifacts** — README
  (status, diagram: Gold moves to `done`), CLAUDE.md (its status line
  still reads *"Phase 1 in progress"*), story-bank gist.
- [ ] **Step 7: Close §13's open items** that this phase measured, and
  add §5.1's era-bound label coverage, which the design doc does not
  currently state.

---

## Phase 2 exit gate

Every box ticked before Phase 3 starts.

- [ ] `make check` green — ruff, `mypy --strict`, full pytest suite
- [ ] `dbt build` green in CI on fixtures
- [ ] CI green on the PR
- [ ] **Silver no longer overwrites itself** — two hours coexist (Defect A)
- [ ] **Silver reads Bronze**, not the source archive (Defect B)
- [ ] **A Gold model's second run is a `MERGE`**, asserted from the Delta log
- [ ] **SCD2 verified**: rename closes the old row, exactly one current row per repo, case-only renames detected, double-rename yields three versions
- [ ] **Accumulating snapshot verified**: out-of-order arrival preserves the earliest timestamp
- [ ] **No fact column reads an embedded payload object** (§4.3a)
- [ ] **The label excludes the PR author**, excludes drafts null-safely, and is computed without reading its own future
- [ ] **Legacy PRs get a label** despite zero `PullRequestReviewEvent`
- [ ] **Right-censoring flagged and its rate reported**
- [ ] Second-source result published — **whether or not "zero new Python" held**
- [ ] **Tier 3 backfill complete at the derived span**, or shrunk with the arithmetic recorded
- [ ] **Photon result published, including if negative**, per layer
- [ ] Cost per run measured and reported
- [ ] **`terraform destroy` leaves nothing; 0 clusters**
- [ ] `docs/STATUS.md` updated in the same commit as each task
- [ ] README, CLAUDE.md and the story-bank gist refreshed

## Deferred out of Phase 2, on purpose

Named so their absence is a decision, not an oversight.

- **The feature platform, as-of joins, and the leakage suite** — Phase 3.
  Task 5 tests point-in-time correctness for the *label*; the general
  feature-level invariant needs the offline store.
- **Any model, baseline included** — Phase 4. A baseline before Gold is
  trustworthy would measure the pipeline, not the model.
- **`fact_event` and the per-type detail tables beyond PRs** (`pushes`,
  `stars`, `forks`) — §4.2 names them; only the PR path is needed for the
  label, and the burn is the deadline-bound work. Adding them is cheap
  later and expensive now.
- **OpenLineage and Power BI** — Phase 7, with governance and docs.
- **Streaming** — Phase 6. Every write here is batch.
- **`dim_time`** — ADR-004 settled this: millions of rows storing
  attributes the fact already carries.
- **Pseudonymization** — required in anything *published*; nothing is
  published from Gold this phase.
