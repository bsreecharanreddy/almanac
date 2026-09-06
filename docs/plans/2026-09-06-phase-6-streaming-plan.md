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
    spark: SparkSession, landing: str, *, watermark: str = "10 minutes"
) -> DataFrame: ...
def write_bronze_stream(df: DataFrame, dest: str, checkpoint: str) -> StreamingQuery: ...
```

Era normalization goes through the **existing** `pipeline/eras.py`
handlers rather than a second implementation.

**Tests:**
- `test_dedups_on_event_id_within_watermark` — the *same* key Silver's batch dedup uses (§4.2), so the paths agree by construction.
- `test_duplicate_outside_watermark_is_counted_not_silently_absorbed`
- `test_late_row_increments_metric` — late arrival is reported, never discarded quietly.
- `test_exactly_once_across_restart` *(integration)* — kill the query mid-stream, restart from the same checkpoint, assert output byte-identical to an uninterrupted run. The streaming analogue of Phase 1's "rerun any hour twice" gate.
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

**Interface:**

```python
def replay_hours(
    spark: SparkSession,
    hours: list[str],
    dest: str,
    *,
    lateness: timedelta = ...,
    duplicate_rate: float = 0.0,
    shuffle: bool = False,
    seed: int = 0,
) -> ReplayStats: ...
```

**`seed` is not decoration.** Phase 5's Bug 3 was a nondeterministic
`rand` sample read twice; a replay harness that injects disorder
unseeded is the same defect waiting to happen, and an unreproducible
correctness test is not a correctness test.

**Tests, one per forced condition:**
- `test_forces_event_after_watermark_passed`
- `test_forces_duplicate_across_micro_batches`
- `test_forces_out_of_order_within_batch`
- `test_forces_gap_and_observes_recovery`
- `test_replayed_stream_equals_batch_silver` — **the gate.** Replayed streaming output must equal batch Silver output over the same committed fixtures. If they disagree, one is wrong, and the test names the differing rows.

Uses the **committed fixtures**, not new synthetic data, so a replay
result is directly comparable to a batch result over the same input.

**Done when:** all five pass, and the equality test is reproducible across seeds.
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

**Tests:**
- `test_rolling_counts_exclude_events_at_or_after_as_of` — strict `<`, matching `as_of_join`.
- `test_cold_start_entity_yields_null_not_zero` — the same discipline `compute_author_activity` already applies to an unclosed prior PR.
- `test_streaming_leakage_suite` *(integration)* — the existing suite gains a streaming case.

**Explicitly out of scope, recorded rather than omitted:** re-serving the
Phase 4 champion on live features. Its vector needs fields the live feed
does not carry.

**Done when:** the leakage suite is green including the new streaming case.
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

**Done when:** `terraform fmt -check` and `terraform validate` clean, and
`terraform plan` shows the expected resources to add and nothing else to
change.
**Commit:** `infra: Terraform for the streaming job and online store`

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

---

## Exit gate

- [ ] Replayed streaming output equals batch Silver output over the same hours
- [ ] Exactly-once holds across a mid-stream restart from checkpoint
- [ ] Late, duplicate and out-of-order events are each forced and handled
- [ ] A live event demonstrably updates a served online feature value
- [ ] Streaming features pass the leakage suite's point-in-time assertions
- [ ] Online store torn down; real idle rate measured and published
- [ ] Every measured claim states its `n`

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
