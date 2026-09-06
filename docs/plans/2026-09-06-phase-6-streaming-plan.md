# Phase 6: streaming ingest + online store (implementation plan)

Written from design doc §4.6 (`docs/design/2026-09-01-almanac-system-design.md`),
which carries the two live probes and the decisions this plan wires into
code. Same register as `docs/plans/2026-09-04-phase-5-embeddings-vector-index-plan.md`:
interfaces and test intent, not full inline code, since there is no separate
review pass between writing this and implementing it. TDD discipline,
`make check` before every commit, one STATUS.md verification-log row per
task, unchanged.

**Ordering principle, unchanged from Phase 5:** every real-money step —
the live poll window, the online store, the streaming cluster — happens
once, last, in Task 8, after everything upstream is built and tested
against local fixtures and replayed archive hours. Streaming plus a
non-scale-to-zero online store is the most expensive shape this project
has run, so the ordering matters more here than it did in Phase 5.

**Scope decision already taken (2026-09-06):** the cloud window is pulled
forward to before the 2026-09-24 credit expiry rather than §9's original
Nov 2–8, and the phase covers **both** streaming ingest and the online
store, meeting §9's Phase 6 gate as written rather than amending it.

---

## Task 1: Measure before provisioning — throughput, credit, CU rate

Not a code task. The same cloud-verification shape as Phase 5 Task 1 and
Phase 2's DBU-rate lookup, and it exists because §4.6's two probes are
`n=1` and must not become design premises unexamined.

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

## Task 2: The event poller

**Files:**
- New: `src/almanac/stream/__init__.py`, `src/almanac/stream/poller.py`
- Test: `tests/unit/test_stream_poller.py`

**Interfaces:**
- `poll_once(client, etag: str | None) -> PollResult` — one conditional
  request. Returns events, the new `ETag`, the server's `x-poll-interval`,
  and remaining rate budget.
- `run_poller(dest: str, *, max_polls: int | None, clock) -> PollStats` —
  the loop. Writes each poll's raw JSON to `dest` as one object per event,
  partitioned by ingest time.

**Behaviour that must be tested, all against a faked transport — no
network in the unit suite (`-m "not network"` is CI's contract):**
- Honours `x-poll-interval` from the response, never a hardcoded sleep.
- Sends `If-None-Match` and treats `304` as "no new events", not an error.
- Backs off on `403`/`429` using `Retry-After` when present. Per the
  `almanac-recurring-ci-failure` lesson carried from Canopica: **a 429 is
  a signal to handle, not a limit to raise.**
- Writes are `.part`-then-rename, so a crash never leaves a half-written
  object — the existing Bronze convention, reused not reinvented.
- Records the *poll* time separately from the event's own `created_at`.
  These diverge, and that divergence is exactly what Task 3's watermark
  measures.

**Reuse, not duplication:** `source.py` already has a rate-limited,
`Link`-paginating REST client from §4.5a's second source. The poller
extends that client rather than adding a second HTTP path.

## Task 3: Streaming ingest — watermark, dedup, exactly-once

**Files:**
- New: `src/almanac/stream/ingest.py`
- Test: `tests/unit/test_stream_ingest.py`, `tests/integration/test_stream_exactly_once.py`

**Interfaces:**
- `stream_events(spark, landing_path, *, watermark: str) -> DataFrame` —
  a streaming read of the landing zone, era-normalized through the
  **existing** `pipeline/eras.py` handlers, watermarked on the event's own
  `created_at`.
- `write_bronze_stream(df, dest, checkpoint) -> StreamingQuery` — the
  idempotent Delta sink.

**The correctness requirements, each with its own test:**
- **Dedup is on `event_id` within the watermark** — the same key Silver's
  batch dedup uses (§4.2), so the two paths agree by construction. A
  duplicate delivered inside the watermark is dropped; one delivered
  outside it is counted and reported, never silently absorbed.
- **Exactly-once across a restart.** The integration test kills the query
  mid-stream, restarts from the same checkpoint, and asserts the output is
  byte-identical to an uninterrupted run — the streaming analogue of
  Phase 1's "rerun any hour twice → byte-identical" gate.
- **Late arrival is counted, not discarded silently.** A row later than
  the watermark increments a metric that Task 8 publishes.
- **§4.1a is not silently skipped:** the legacy era has no `event_id`, but
  the live feed is `REDUCED_V3` only, so the streaming path asserts the
  era rather than handling a case it can never see.

## Task 4: The replay harness — where correctness is actually proved

**Files:**
- New: `src/almanac/stream/replay.py`
- Test: `tests/integration/test_stream_replay.py`

This is the task §4.6's throughput measurement promotes from nice-to-have
to load-bearing. The live feed is a ~11% tail and cannot demonstrate
completeness; replay can, on demand, deterministically.

**Interface:**
- `replay_hours(spark, hours, dest, *, lateness, duplicate_rate, shuffle) -> ReplayStats`
  — feeds real committed GH Archive fixtures through the **identical**
  Task 3 path, injecting controlled disorder.

**What it must be able to force, each asserted:**
- An event arriving after its watermark has passed.
- The same `event_id` delivered twice, in different micro-batches.
- Events delivered out of `created_at` order within a batch.
- A gap (a missing interval), so recovery is observable.

**The harness reuses the committed fixtures**, not new synthetic data —
the same files Phase 1's tests already use, so a replay result is
comparable to a batch result over the same input. That comparison is the
gate: **replayed streaming output must equal batch Silver output over the
same hours.** If the two disagree, one of them is wrong, and the test says
which rows.

## Task 5: Streaming feature aggregation

**Files:**
- New: `src/almanac/stream/features.py`
- Test: `tests/unit/test_stream_features.py`, extend `tests/integration/test_features_leakage.py`

**Scope is constrained by §4.6's first finding** and the plan says so
plainly: reduced-era events cannot produce §5.1's label or any text
feature, so the online feature set is what the reduced payload supports —
rolling event counts per repo and per actor, arrival rate, inter-event
timing, and time-since-last-event.

**The non-negotiable:** these are computed with the **same** point-in-time
discipline as the offline features. A streaming aggregate at time T uses
only events with `created_at < T`. The leakage suite gains a streaming
case; CLAUDE.md's one governing principle does not get an exemption
because the compute model changed.

**Explicitly out of scope, and recorded rather than omitted:** re-serving
the Phase 4 champion on live features. Its vector needs fields the live
feed does not carry. Phase 6 serves fresh features and proves the path.

## Task 6: Close the online-store prerequisite gaps

**Files:**
- Modify: `src/almanac/features/registration.py`
- Test: extend `tests/unit/test_features_registration.py`

Two gaps found during design (§4.6's table), both pure DDL:

- `ALTER TABLE … SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`
  — required by `publish_table` for `TRIGGERED` and `CONTINUOUS`. Present
  in `embed/pipeline.py`, absent for feature tables.
- `ALTER TABLE … ALTER COLUMN <pk> SET NOT NULL` — required; not enforced.

Tested as string-building plus a real local-metastore round trip, the same
way `primary_key_sql` already is. The `TIMESERIES` primary key needs no
change — §4.4a already emits it.

## Task 7: Online store, authored not applied

**Files:**
- New: `src/almanac/stream/online_store.py`
- Modify: `infra/terraform/` (online store + its teardown path), `pyproject.toml`
  (`databricks-feature-engineering>=0.13.0` into the `ml` extra, floor
  re-checked live at implementation time, not carried from this plan)
- Test: `tests/unit/test_online_store.py`

**Interfaces** wrap the documented API rather than reimplementing it:
`create_online_store(name, capacity)`, `publish_feature_table(...)`,
`delete_online_store(name)`.

**Provisioned with its teardown in the same change.** §4.6 records that
Lakebase does not scale to zero; this task therefore ships the delete path
and the cost note *before* Task 8 spends anything, which is precisely what
Phase 5 did not do for Vector Search and had to correct afterwards.

`publish_mode` starts `TRIGGERED`; `CONTINUOUS` is what Task 8 measures,
since it is the mode that keeps a streaming pipeline alive and therefore
the one that costs money.

**Cannot `apply` until Task 8** — the source feature tables must exist
with CDF first. Same "author now, apply once the dependency is real" shape
as `databricks_model_serving`'s `entity_version` and Phase 5's index.

## Task 8: The real cloud run, then teardown

The only task that spends money, and the only one that can close §9's
gate: **"live events land and update online features."**

**Sequence, in order:**
1. `terraform apply` the online store at the capacity Task 1's rate
   analysis chose.
2. Run the poller against the live API for a bounded window, authenticated.
3. Streaming ingest live → Bronze → streaming features.
4. `publish_table` in `CONTINUOUS` mode; confirm a live event reaches the
   online store and changes a served feature value. **This is the gate**,
   and it is demonstrated end to end, not asserted.
5. Measure: end-to-end freshness lag (event `created_at` → online store
   readable), the real capture fraction over the window, late-arrival and
   duplicate counts, and the **real idle DBU/hour of the online store**.
6. **Tear down**, and record the measured idle rate in the findings doc
   the way `2026-09-06-vector-search-live-state-and-teardown.md` does.

**Capture before the irreversible step** — endpoint metadata, a live query,
and the measured numbers get written down *before* teardown, not after.
That rule exists because Phase 5 nearly lost its evidence.

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
- **Multi-region or HA read replicas** — the API supports up to 3, and
  this is a portfolio window, not a production SLA.
- **Enriching live events via the §4.5a REST client** to recover the
  dropped fields. Genuinely interesting, genuinely a different phase: it
  would restore label-capable live data at 5,000 req/hour against a
  bounded repo set. Named here so its absence reads as a decision.
