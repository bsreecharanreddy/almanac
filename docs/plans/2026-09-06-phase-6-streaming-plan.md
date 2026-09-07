# Phase 6: streaming ingest + online store (implementation plan)

Written from design doc §4.6 (`docs/design/2026-09-01-almanac-system-design.md`),
which carries the two live probes and the decisions this plan wires into
code. Same register as `docs/plans/2026-09-04-phase-5-embeddings-vector-index-plan.md`:
interfaces and test intent, not full inline code, since there is no separate
review pass between writing this and implementing it. TDD discipline,
`make check` (lint → typecheck → test) green before every commit, one
STATUS.md verification-log row per task, one commit per task, unchanged.

**Ordering principle, unchanged from Phase 5:** every real-money step —
the live poll window, the online store, the streaming cluster — happens
once, last, in Task 9, after everything upstream is built and tested
against local fixtures and replayed archive hours. Streaming plus a
non-scale-to-zero online store is the most expensive shape this project
has run, so the ordering matters more here than it did in Phase 5.

**Scope decisions already taken (2026-09-06):** the cloud window is pulled
forward from §9's original Nov 2–8, and the phase covers **both** streaming
ingest and the online store, meeting §9's Phase 6 gate as written rather
than amending it.

**Amended after Task 1.** The pull-forward is *not* a race against the
2026-09-24 credit expiry. §11 now records that **credit expiry is a budget,
not a wall** — modest paid spend afterwards is acceptable provided
resources come down when idle, and **teardown ships with provisioning**
rather than as a follow-up. The window moves earlier because the work is
ready. Phase 6 is explicitly **not** scoped down to fit a credit balance;
if a capacity or window length is chosen for cost, the arithmetic is
stated and the choice is the user's, not a silent narrowing.

## Task dependency and cost

Tasks 2–8 are local-only and cost nothing; each is independently
committable and testable against fixtures. Only Tasks 1 and 9 touch the
cloud, and only Task 9 provisions anything billable.

```
1 (measure, cloud) ─┐
                    ├─→ 2 (config+poller) ─→ 3 (ingest) ─→ 4 (replay) ─→ 5 (features) ─┐
                    │                                                                   ├─→ 9 (cloud run)
                    └─────────────→ 6 (CDF/NOT NULL) ─→ 7 (online store) ─→ 8 (job TF) ─┘
```

Task 6 has no dependency on the streaming path and can be done at any
point after Task 1; it is placed where it is because Task 7 needs it.

---

## Task 1: Measure before provisioning — throughput, credit, CU rate

Not a code task. The same cloud-verification shape as Phase 5 Task 1 and
Phase 2's DBU-rate lookup, and it exists because §4.6's two probes are
`n=1` and must not become design premises unexamined.

**Prerequisite:** the Databricks PAT has expired (1-hour lifetime); re-mint
before starting. Azure CLI auth is live (`bscr-az-portfolio`).

**What gets measured and committed to
`docs/findings/2026-09-06-events-api-and-online-store-rates.md` before
Task 2 starts:**

- **Real Events API throughput**, over a proper window — poll
  authenticated at the advertised `x-poll-interval` for ≥30 consecutive
  intervals, recording per poll: events returned, distinct `event_id`s
  not seen in the prior poll, and the observed overlap. This yields the
  real capture fraction, replacing §4.6's single-poll ~11% estimate.
  State the `n` in the claim itself.
- **Whether a `304 Not Modified` on a conditional `If-None-Match` request
  is exempt from the rate limit.** GitHub documents this; it is load-
  bearing for the poll budget, so it gets verified against live response
  headers rather than cited.
- **Real remaining Azure credit**, via `system.billing.usage` joined to
  `list_prices` — the method `2026-09-03-measured-dbus.md` established.
  Phase 5 spent $65.75; the balance against $184 is currently unverified.
- **The Lakebase CU rate**, and specifically the **idle** rate. §4.6
  predicts the Vector Search trap repeats here; this is where that
  prediction gets confirmed or falsified before any spend.

**Gate:** if the measured capture fraction is far below the single-poll
estimate, that strengthens rather than weakens the replay harness's role —
it does not block the phase. If the Lakebase idle rate prices the bounded
window above ~15% of remaining credit, capacity drops to `CU_1` or the
window shortens; the arithmetic is committed either way.

**Done when:** the findings doc exists with four measured numbers, each
carrying its `n` and the exact command that produced it.
**Commit:** `docs: measure Events API throughput and online-store rates`

## Task 2: Event-stream config and the poller

**Files:**
- New: `src/almanac/stream/__init__.py`, `src/almanac/stream/poller.py`
- New: `conf/sources/github_events.yml` (the plan originally said
  `config/sources/*.yaml`; the repo's real convention, matching
  `github_rest.yml`, is `conf/sources/*.yml`)
- Test: `tests/unit/test_stream_poller.py`

**Config — composed, not inherited.** `RestSourceConfig` already carries
`auth: AuthConfig` and `rate_limit: RateLimitConfig`, but its own shape
(`url_template`, `list_url_template`, `enrichment_fields`) does not
describe a polled event stream, and it is `frozen` / `extra="forbid"`.
So a sibling model reusing the same two components:

```python
class EventStreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    kind: Literal["event_stream"]
    url: str
    auth: AuthConfig  # reused, not redefined
    rate_limit: RateLimitConfig  # reused, not redefined
    pages_per_poll: int = 3  # the Link header's real ceiling (§4.6)
    poll_interval_header: str = "X-Poll-Interval"
    max_attempts: int = 3
```

This follows `RestSourceConfig`'s own precedent — "distinct from
`SourceConfig`: the shapes do not overlap" — and the repo's
composition-over-inheritance convention. **The token comes from
`auth.token_env`**, a named environment variable, exactly as the §4.5a
client already does. No token literal enters the repo, the config, or a
log line.

**Interfaces:**

```python
def poll_once(client: RestClient, cfg: EventStreamConfig, etag: str | None) -> PollResult: ...
def run_poller(
    cfg: EventStreamConfig, dest: Path, *, max_polls: int | None = None, clock: Clock = ...
) -> PollStats: ...
```

`PollResult` carries events, the new `ETag`, the server's poll interval,
and remaining rate budget. `PollStats` aggregates the window.

**Tests — faked transport, no network (CI runs `-m "not network"`):**
- `test_respects_server_poll_interval` — sleeps on the header value, never a constant.
- `test_conditional_request_sends_if_none_match`
- `test_304_is_no_new_events_not_an_error`
- `test_backs_off_on_429_using_retry_after` — **a 429 is a signal to handle, not a limit to raise.**
- `test_partial_write_then_rename` — a crash never leaves a half-written object (existing Bronze convention).
- `test_poll_time_recorded_separately_from_created_at` — the divergence Task 3's watermark measures.
- `test_token_never_appears_in_logs_or_error_messages`

**Done when:** all seven pass; `make check` green.
**Commit:** `feat(stream): poll the GitHub Events API with ETag and backoff`

## Task 3: Streaming ingest — watermark, dedup, exactly-once

**Files:**
- New: `src/almanac/stream/ingest.py`
- Test: `tests/unit/test_stream_ingest.py`, `tests/integration/test_stream_exactly_once.py`

**Interfaces:**

```python
def stream_events(
    spark: SparkSession, landing: str, *, watermark: timedelta = timedelta(minutes=10)
) -> DataFrame: ...
def write_stream_silver(
    df: DataFrame, dest: str, checkpoint: str, *, available_now: bool = False
) -> StreamingQuery: ...
```

Era normalization goes through the **existing** `pipeline/eras.py`
handlers rather than a second implementation, reusing
`payloads.parse_events` too — so a row landed here has the exact
`SILVER_COLUMNS` shape a batch Silver run would produce. **Renamed from
this plan's own `write_bronze_stream`**: what it writes is Silver-shaped
(normalized, deduped, via the same functions as the batch Silver path),
not raw — the true streaming-Bronze equivalent is the landing zone itself
(Task 2's `.jsonl` files), matching §4.6's own words, "a poller lands raw
event JSON to cloud storage." Calling the write target "Bronze" while it
holds `SILVER_COLUMNS`-shaped rows would mislead a reader who knows the
batch pipeline's own naming.

**Tests:**
- `test_dedups_on_event_id_within_watermark` — the *same* key Silver's batch dedup uses (§4.2), so the paths agree by construction.
- `test_duplicate_outside_watermark_is_counted_not_silently_absorbed` — **found to be a stricter claim than it first looked**: verified directly that a row this old is dropped by `dropDuplicatesWithinWatermark` *before it reaches a `foreachBatch` write at all* — counting it from the written output is structurally impossible, not just harder. Fixed with Spark's `observe()`, attached *before* the watermark step, read back via `late_event_count(query)`.
- `test_late_row_increments_metric` — late arrival is reported, never discarded quietly. Unified with the row above under one mechanism (`is_late` + the same `observe()`), since both are the same underlying quantity: processing delay against the watermark.
- `test_exactly_once_across_restart` *(integration)* — restart from the same checkpoint as a **new** `StreamingQuery` object (a real Python-level "crash" is not needed: a second `.start()` against a checkpoint is the actual restart contract), assert output equal to an uninterrupted run over the same data (compared as a row set, not literal bytes — Parquet's physical layout isn't reproducible byte-for-byte even from identical input). The streaming analogue of Phase 1's "rerun any hour twice" gate.
- `test_asserts_reduced_era` — §4.1a's legacy era has no `event_id`, but the live feed is `REDUCED_V3` only, so the path asserts the era rather than handling a case it can never meet. Deliberate, not overlooked.

**Done when:** the restart test passes repeatedly (run it 3×; a flaky exactly-once test is a failing one).
**Commit:** `feat(stream): watermarked, exactly-once Bronze ingest`

## Task 4: The replay harness — where correctness is actually proved

**Files:**
- New: `src/almanac/stream/replay.py`
- Test: `tests/integration/test_stream_replay.py`

This is the task §4.6's throughput measurement promotes from nice-to-have
to load-bearing. The live feed is a ~11% tail and cannot demonstrate
completeness; replay can, on demand, deterministically.

**Interface (as built):**

```python
def replay_hours(
    spark: SparkSession,
    hours: list[str],
    dest: str,
    *,
    source: Path | None = None,  # one real archive-hour file; defaults to the reduced-era fixture
    lateness: timedelta = timedelta(0),
    duplicate_rate: float = 0.0,
    shuffle: bool = False,
    seed: int = 0,
) -> ReplayStats: ...
```

`hours` names poll cycles, not distinct files: one real hour is split
into `len(hours)` **chronologically contiguous** slices, delivered in
order so the watermark advances smoothly and nothing is spuriously
dropped by construction. `duplicate_rate` carries each cycle's *latest*
events forward into the next (a real cross-batch duplicate, close enough
to the boundary to be deduped rather than watermark-dropped). `lateness`,
when set, appends one trailing cycle that redelivers every event
`lateness` after its own `created_at`, once the watermark has already
passed the whole replay.

**`seed` is not decoration.** Phase 5's Bug 3 was a nondeterministic
`rand` sample read twice; a replay harness that injects disorder
unseeded is the same defect waiting to happen, and an unreproducible
correctness test is not a correctness test.

**`source` was added beyond the plan's sketch.** Replay must go through
`write_stream_silver`'s `_reject_non_reduced_era` guard unchanged (Task
9 runs the identical path live), so it can only replay `REDUCED_V3`-era
data — and **no committed fixture was in that era**: both existing ones
predate the 2025-10-15 payload reduction. Added a third,
`reduced-2025-11-03-14.jsonl.gz` (2,000 events, `scripts/build_fixtures.py`),
plus a `reduced_events_path` conftest fixture.

**Tests, one per forced condition:**

- `test_forces_event_after_watermark_passed` — the trailing late cycle's redeliveries are all counted by `late_event_count` and none of them duplicate what on-time delivery already wrote.
- `test_forces_duplicate_across_micro_batches`
- `test_forces_out_of_order_within_batch`
- `test_forces_gap_and_observes_recovery` — three cycles, same checkpoint, each transition a real restart; no loss, nothing double-counted.
- `test_replayed_stream_equals_batch_silver` — **the gate.** Replayed streaming output must equal batch Silver output over the same committed fixture. If they disagree, one is wrong, and the test names the differing rows. **It caught two real, silent Task 2/3 bugs** (see STATUS): the landing envelope typed `event` as a struct via `payloads.EVENT_SCHEMA`, which omits `actor`, nulling every `actor_login`; and `stream_events` wrote `event_date` as a `DateType` where batch and the rest of the codebase use a `yyyy-MM-dd` string.

Uses the **committed fixture**, not new synthetic data, so a replay
result is directly comparable to a batch result over the same input.

**Done when:** all five pass (confirmed 3× — 439s/247s/216s), and the whole-repo `make check` is green.
**Commit:** `feat(stream): replay harness forcing late, duplicate and out-of-order events`

## Task 5: Streaming feature aggregation

**Files:**
- New: `src/almanac/stream/features.py`
- Test: `tests/unit/test_stream_features.py`, extend `tests/integration/test_features_leakage.py`

**Scope is constrained by §4.6's first finding**, and the plan says so
plainly: reduced-era events cannot produce §5.1's label or any text
feature, so the online feature set is what the reduced payload supports —
rolling event counts per repo and per actor, arrival rate, inter-event
timing, time-since-last-event.

**The non-negotiable:** computed with the **same** point-in-time
discipline as the offline features. A streaming aggregate at time T uses
only events with `created_at < T`. CLAUDE.md's one governing principle
does not get an exemption because the compute model changed.

**As built.** `compute_repo_stream_features` / `compute_actor_stream_features`
(one parametrized `_stream_features`, two wrappers), each a timeseries-keyed
row per event: `events_prior_1h`, `events_prior_24h` (`rangeBetween(-N, -1)`
— the `-1` is the strict `<`), `secs_since_last_event` (`lag`),
`arrival_per_hour_24h`. **The strict `<` lives in the feature, not a
downstream join** — the offline groups are inclusive-of-self and lean on
`as_of_join` for the boundary, but the online store serves the latest row
per entity with no join in the path. An **unseen** entity yields all-null,
not zero (unknown ≠ quiet, the discipline `compute_author_activity`
applies to an unclosed prior PR); once seen, `0` in a window is a real
value. A tie-break on `event_id` makes the frame deterministic on a shared
`created_at`, and a same-second prior event is not counted.

**Tests:**

- `test_rolling_counts_exclude_events_at_or_after_the_events_own_time`, `test_a_same_second_event_is_not_counted_as_prior` — strict `<`, matching `as_of_join`.
- `test_cold_start_entity_yields_null_not_zero`, `test_zero_in_window_is_kept_once_the_entity_has_been_seen` — the unknown-vs-zero line.
- `test_actor_features_partition_on_actor_not_repo` — the actor timeline crosses repos.
- `test_streaming_features_are_leak_free_and_reproducible_only_when_version_pinned` *(integration, in `test_features_leakage.py`)* — a late arrival with `created_at < T` is leak-free but moves a live re-read, so a build pins the Silver version exactly as offline does.

**Explicitly out of scope, recorded rather than omitted:** re-serving the
Phase 4 champion on live features. Its vector needs fields the live feed
does not carry.

**Done when:** the leakage suite is green including the new streaming case; whole-repo `make check` green.
**Commit:** `feat(stream): point-in-time-correct streaming features`

## Task 6: Close the online-store prerequisite gaps

**Files:**
- Modify: `src/almanac/features/registration.py`
- Test: extend `tests/unit/test_features_registration.py`

Two gaps found during design (§4.6's table), both pure DDL:

- `ALTER TABLE … SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`
  — required by `publish_table` for `TRIGGERED` and `CONTINUOUS`. Present
  in `embed/pipeline.py`, absent for feature tables.
- `ALTER TABLE … ALTER COLUMN <pk> SET NOT NULL` — required; not enforced.

**Tests:** string-building plus a real local-metastore round trip, the
same way `primary_key_sql` already is —
`test_enables_cdf_on_feature_table`, `test_sets_pk_columns_not_null`,
`test_timeseries_pk_unchanged` (a regression guard: §4.4a's DDL is already
correct and must not drift while adding these).

**As built.** `change_data_feed_sql` (one `str`) and `not_null_key_sql`
(one `ALTER COLUMN` per key column, `list[str]`), plus a shared
`_key_columns` helper both it and `primary_key_sql` route through so the
two cannot disagree about what the key is. **Only CDF round-trips
locally**: a probe against open-source Delta 4.4.0 confirmed
`ALTER COLUMN … SET NOT NULL` is refused on a populated table ("cannot
change nullable column to non-nullable") — Databricks-managed Delta
accepts it, so `not_null_key_sql` gets the same string-plus-deferred-
execution treatment `primary_key_sql`'s TIMESERIES constraint already
has, verified live in Task 9. The exact CDF/NOT-NULL SQL was taken
verbatim from Microsoft Learn's online-feature-store page (Gate 2,
re-checked live 2026-09-06). **One scope addition beyond the Files
list**: `features/runner.py`'s `--register` block now also runs these two
statements (CDF, then NOT NULL, then the existing PK constraint that
needs them), so `--register` produces a genuinely publish-ready table
rather than a half-state a later task must remember to finish — the same
"complete the wiring where the flag is" call as Task 4's `source` param.
The `register=True` path stays unexercised by the local suite (the
Derby metastore rejects TIMESERIES), exactly as before.

**Done when:** `make check` green; the existing registration tests still pass unmodified.
**Commit:** `feat(features): CDF and NOT NULL keys for online publishing`

## Task 7: Online store wrapper, authored not applied

**Files:**
- New: `src/almanac/stream/online_store.py`
- Modify: `pyproject.toml` — `databricks-feature-engineering>=0.13.0` into
  the `ml` extra, **floor re-checked live against PyPI's JSON API at
  implementation time**, not carried from this plan (Phase 5 Task 2's
  Gate 2 catch: a live-search claim about a version was wrong, PyPI's own
  API was right)
- Test: `tests/unit/test_online_store.py`

**Interfaces** wrap the documented API rather than reimplementing it:

```python
def create_store(name: str, capacity: Capacity = "CU_1") -> OnlineStore: ...
def publish_feature_table(
    store: OnlineStore, source: str, online: str, mode: PublishMode = "TRIGGERED"
) -> PublishResult: ...
def delete_store(name: str) -> None: ...
```

**Provisioned with its teardown in the same change.** §4.6 records that
Lakebase does not scale to zero; this task therefore ships `delete_store`
and the cost note *before* Task 9 spends anything — precisely what Phase 5
did not do for Vector Search and had to correct afterwards.

**Tests** are contract tests against a faked client (no cloud):
`test_capacity_defaults_to_smallest`, `test_publish_requires_cdf_source`,
`test_delete_is_idempotent`.

**As built.** The version floor re-check the plan demanded paid off:
PyPI's JSON API gives **0.17.1** as current, not the `>=0.13.0` this plan
carried from Microsoft Learn's install snippet, and the repo's convention
is to floor at the current release. The real 0.17.1 API was then
**introspected rather than trusted** — every method is keyword-only, and
`create_online_store` requires `capacity` rather than defaulting it, so
the smallest-capacity default is genuinely this wrapper's decision to
make. Reading `delete_online_store`'s source produced the sharpest
finding: **it raises `NotFound` on a store that is already gone**, so
`delete_store`'s idempotency is fixing real behaviour, not decorating it,
and `_FakeClient` reproduces the raise so the test cannot pass against a
wrapper that does nothing.

Signatures are `create_store(client, *, name, capacity="CU_1")`,
`publish_feature_table(client, spark, *, store, source, online,
mode="TRIGGERED")`, `delete_store(client, *, name)`, plus
`has_change_data_feed(spark, table)` and `load_client()`. The client is
**injected**, not constructed inside, so a fake satisfies it — the same
`load_index()` / `similar_prs(index=…)` split `embed/query.py` already
uses, with an `OnlineStoreClient` Protocol and a `cast` at the untyped
boundary exactly as `embed/pipeline.py`'s `TextEncoder` does.

Two additions beyond the plan's three tests, both grounded rather than
speculative: **SNAPSHOT publishes without CDF** (the docs require CDF for
`TRIGGERED`/`CONTINUOUS` only, so guarding all three would refuse a
publish the service accepts), and
`test_the_real_client_still_matches_the_protocol`, which introspects the
real class — the floor is unbounded above, so a later release could move
these signatures out from under the Protocol while every fake-based test
kept passing. Verified non-vacuous before being committed.

**Done when:** `make check` green. **Cannot `apply` until Task 9** — the
source feature tables must exist with CDF first. Same "author now, apply
once the dependency is real" shape as `databricks_model_serving`'s
`entity_version` and Phase 5's index.
**Commit:** `feat(stream): Lakebase online-store wrapper with teardown`

## Task 8: Terraform — the streaming job and the online store

**Files:**
- New: `infra/terraform/streaming.tf`
- Modify: `infra/terraform/variables.tf`

Follows the established per-workload pattern (`embeddings.tf`,
`similarity.tf`), not a new one: a `databricks_job` for the poller +
streaming query, plus the online-store resource.

**Two things carried from prior incidents rather than rediscovered:**
- `variables.tf`'s existing notes record that a raw `databricks_job`
  cannot derive library dependencies the way a bundle would — the
  streaming job declares its own explicitly.
- **`prevent_destroy` stays OFF** on the online store, deliberately, and
  the comment says why: it is provisioned for a bounded window and torn
  down at the end of Task 9. Phase 5's `vector_search.tf` carries the
  mirror-image note; the reasoning is recorded at the resource, not in a
  commit message that nobody will find.

**As built, and the Files list was short by three.** The job needs an
entrypoint, and there wasn't one: no `stream` module had a CLI and there
was no `scripts/streaming.py`, while every other `databricks_job` in this
repo points at one through `spark_python_task`. Same shape as Task 4's
missing reduced-era fixture — a prerequisite the plan assumed rather than
checked. Added `src/almanac/stream/runner.py` (stages `poll` and
`ingest`, `choices=` on one positional, no subparsers), the
`scripts/streaming.py` shim matching `scripts/features.py`, and
`tests/unit/test_stream_runner_cli.py`. **Two stages, not one fused
process and not two threads**: the poller is network-bound and needs no
SparkSession, the ingest is a Spark query and needs no token, so a
failure stays attributable to one of them.

**Three findings from checking the provider registry live** rather than
assuming a Python-only path:

- **`databricks_database_instance` exists** (Public Preview) and is the
  online store. The lock file already pinned provider **1.130.0**, which
  carries it, so no constraint change.
- **`databricks_database_synced_database_table` is Private Preview and is
  deliberately not used.** Task 7's `publish_feature_table` owns the
  publish, because Databricks' own docs warn that deleting a synced table
  by any path other than the feature-engineering API leaves the
  underlying Postgres storage behind. One owner per object.
- **The instance exposes an explicit `stopped` flag**, which is *not* the
  automatic scale-to-zero §4.6 recorded as unsupported. Exposed as a
  variable and explicitly marked **unverified** — whether a stopped
  instance stops billing compute is a Task 9 measurement, not a claim to
  make now. Deletion remains the teardown known to work.

Two traps carried from prior incidents rather than rediscovered: the
landing zone is a **UC Volume, not `abfss://`** (the poller writes with
pathlib — the exact `FILE_NOT_EXIST` failure of 2026-09-02), and
`--config` is passed **explicitly** rather than trusting the script's
relative default (a job task's working directory is not the repo root —
the `FileNotFoundError` of the same day). The GitHub token reaches the
poller as `spark_env_vars` resolved from a secret scope, never a job
parameter, so it stays out of every run's visible parameter list.

**Done when:** `terraform fmt -check` and `terraform validate` clean, and
`terraform plan` shows the expected resources to add and nothing else to
change.
**Commit:** `infra: Terraform for the streaming job and online store`

## Task 8b: Close Task 9's runnable gaps (added 2026-09-06, not in the original plan)

**Found by tracing Task 9's gate end to end before spending anything**, which
is the only reason it was found before rather than after provisioning a
store that bills by the hour. The gate — *"a live event demonstrably updates
a served online feature value"* — needs a chain of six links. Tasks 2–8 built
the first two and the last one's wrapper; **three links did not exist**:

| Link | Before this task |
|---|---|
| live event → poll | ✅ Task 2, wired in Task 8 |
| poll → Silver | ✅ Task 3, wired in Task 8 |
| Silver → **stream feature table** | ❌ `compute_*_stream_features` was called by nothing but tests |
| → **PK + CDF + NOT NULL** | ❌ no streaming equivalent of `--register` |
| → **publish** | ⚠️ Task 7's wrapper existed but nothing could invoke it on a cluster |
| → served value | deferred, see below |

The plan treated Task 9 as pure operations. It is not: it needed code.

**Files:**
- Modify: `src/almanac/features/runner.py` — extract `write_and_register`
- Modify: `src/almanac/stream/runner.py` — `features` and `publish` stages
- Modify: `src/almanac/stream/online_store.py` — `get_online_store`, `require_store`
- Modify: `infra/terraform/streaming.tf`, `variables.tf`
- Test: extend `tests/unit/test_stream_runner_cli.py`, `tests/unit/test_online_store.py`

**Reuse, not a second copy.** The registration sequence (CDF → NOT NULL →
PK) is one fact about what "a publishable feature table" means, so it moved
into `write_and_register`, shared verbatim by the batch and streaming paths
rather than restated in each — the same rule that put `RAW_SCHEMA` in
`tests/helpers.py`. `STREAM_FEATURE_TABLES` reuses `FeatureTableSpec`
unchanged.

**Three corrections the work itself forced:**

- **`--schema` must be fully qualified** (`almanac_dbx.features`, not
  `features`). `register_feature_table` emits `{schema}.{table}`, which is a
  valid Unity Catalog three-part name only if the catalog is already in it —
  latent since §4.4a because `--register` had never run against real UC.
- **The online schema is a separate parameter, not derived from the source
  schema.** Databricks documents that an online table's *catalog* name must
  equal its backing Postgres database name, which the source catalog has no
  reason to satisfy.
- **`require_store` looks up and never creates.** Terraform owns the
  instance; a wrapper that quietly created one on a name typo would bill for
  it, and Lakebase does not scale to zero.

Adding `get_online_store` to the Protocol immediately failed `mypy` on the
existing `_FakeClient` — the Protocol catching a real drift the moment it
was introduced, which is the argument for having written it.

**Deliberately not done**, and recorded rather than silently skipped:
`databricks_job.build_features` still does not pass `--register`, so the
*batch* feature tables remain unregistered. Task 9's gate runs entirely
through the streaming path, and mutating an existing job would add a
`1 to change` to the plan for no gate benefit. The served-value read is also
left to a hand-run query in Task 9 rather than a helper, since whether Unity
Catalog exposes the synced table to Spark is exactly what the real run
settles.

**Done when:** `make check` green; `terraform fmt -check`/`validate` clean;
`terraform plan` unchanged at 5 to add, 0 to change, 0 to destroy.
**Commit:** `feat(stream): stream feature and publish stages, closing Task 9's gaps`

## Task 9: The real cloud run, then teardown

The only task that spends money, and the only one that can close §9's
gate: **"live events land and update online features."**

**Sequence, in order:**
1. `terraform apply` the online store at the capacity Task 1's rate analysis chose.
2. Run the poller against the live API for a bounded window, authenticated.
3. Streaming ingest live → Bronze → streaming features.
4. `publish_table` in `CONTINUOUS` mode; confirm a live event reaches the
   online store and changes a served feature value. **This is the gate**,
   demonstrated end to end, not asserted.
5. Measure: end-to-end freshness lag (event `created_at` → online store
   readable), real capture fraction over the window, late-arrival and
   duplicate counts, and the **real idle DBU/hour of the online store**.
6. **Tear down**, recording the measured idle rate the way
   `2026-09-06-vector-search-live-state-and-teardown.md` does.

**Capture before the irreversible step** — store metadata, a live query,
and every measured number get written down *before* teardown, not after.
That rule exists because Phase 5 nearly lost its evidence.

**A teardown-order trap to check first, not discover:** Phase 5's targeted
`terraform destroy` pulled in `databricks_job.pr_similarity` as a
dependent and would have destroyed its run history — the evidence for four
real runs. Run `terraform plan -destroy -target=…` and **read the resource
count** before applying it here.

**Done when:** the gate is demonstrated, the numbers are in a findings doc,
and `terraform plan` shows the store gone.
**Commit:** `feat(stream): live streaming window measured, online store torn down`

**As built, and the sequence above was wrong in four places — every one of
them a claim written from an API's shape rather than from a run.**

- **Step 1 could not run at all in the main workspace.** Lakebase is not
  offered in `westus3`, and a Lakebase project inherits its workspace's
  region irrevocably. That forced an entire second root module
  (`infra/terraform-lakebase/`, centralus), and the obvious fallback
  (westus2) fails a *second* constraint — the node SKU is
  `NotAvailableForSubscription` there.
- **Step 4 used `TRIGGERED`, not `CONTINUOUS`.** `TRIGGERED` is the API
  default and needs no always-on sync pipeline, which suits a bounded
  window that gets destroyed.
- **The publish target needed three prerequisites the plan assumed away**:
  the online catalog is not created by `publish_table`; it must be a
  **standard** catalog (a Database Catalog is refused outright); and its
  schema is not created either. Each cost a ~5-minute cluster start to
  learn, and `terraform validate` passed all three.
- **The gate needed two windows, not one.** "Changes a served feature
  value" is unprovable from a single publish — one publish shows only that
  a value *exists*. A second 5-poll window supplied the before/after: 84
  changed, 684 added, 0 lost.

**Step 5 is complete except the DBU rate**, which is not deferrable by
choice: `system.billing.usage` holds 0 rows in a newly created metastore
and lags ~a day in the established one, so it cannot be read during the
window it measures. `workspace_id` was captured before teardown to keep the
rows attributable afterwards.

**The "capture before the irreversible step" rule earned its place**, and
needs one addition: capture what is needed *after* teardown too — the
workspace id, without which the billing rows cannot be attributed to this
stack at all.

**The teardown-order trap check was worth running and found nothing** (16
resources, all `almanac-lb-*`), but teardown then hit four *different*
traps, now written into the module README: `terraform output` silently
returns empty once a referenced resource is destroyed, and presents as an
**auth** failure; the external location refuses deletion citing dependents
that no longer exist; `force_destroy` in config is inert until an `apply`
writes it into state; and a bare `apply` at that point plans to **recreate**
the Lakebase instance.

---

## Exit gate

- [x] Replayed streaming output equals batch Silver output over the same hours
- [x] Exactly-once holds across a mid-stream restart from checkpoint
- [~] Late, duplicate and out-of-order events are each forced and handled
- [x] A live event demonstrably updates a served online feature value
- [x] Streaming features pass the leakage suite's point-in-time assertions
- [~] Online store torn down; real idle rate measured and published
- [x] Every measured claim states its `n`

Two are marked `[~]`, not `[x]`, and neither should be quietly rounded up:

- **Late events are detected and counted, but "handled" turned out to mean
  "silently dropped."** A distinct, never-before-seen event whose event time
  is behind the watermark never reaches Silver — measured, not theorised: 161
  repos lost in the second live window, 0 in the first, the difference being
  whether the checkpoint had a watermark to restore. Duplicates and
  out-of-order events *are* genuinely handled. Closing this properly is a
  design decision (widen the watermark, or dedup on write with a Delta
  `MERGE` on `event_id` as the batch path already does), deliberately not
  taken under Task 9's own gate.
- **The store is torn down; the idle rate is not published.**
  `system.billing.usage` holds 0 rows in a new metastore and lags ~a day in
  the established one, so the number cannot be read during the window that
  generates it. Deferred with `workspace_id` captured, not abandoned.

## Deferred out of Phase 6, on purpose

- **Re-serving the Phase 4 champion on live features** — impossible on the
  reduced-era payload (§4.6), not merely unbuilt.
- **Backfilling the online store with full history** — the gate is
  freshness, not coverage.
- **Multi-region or HA read replicas** — the API supports up to 3, and this
  is a portfolio window, not a production SLA.
- **Enriching live events via the §4.5a REST client** to recover the dropped
  fields. Genuinely interesting, genuinely a different phase: it would
  restore label-capable live data at 5,000 req/hour against a bounded repo
  set. Named here so its absence reads as a decision.
