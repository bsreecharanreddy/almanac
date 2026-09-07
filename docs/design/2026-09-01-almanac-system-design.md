# Almanac — System Design

**Date:** 2026-09-01
**Status:** draft, pending approval
**Scope:** authoritative. Architecture, phasing, and definition of done.
Where anything else disagrees with this document, this document wins.

---

## 1. What this is

Almanac is an **ML platform for work-queue risk**: work items arrive in a
queue, some breach their service expectation, and a model predicts which
ones early enough for a human to intervene.

It is built on GitHub's public event firehose (GH Archive) because that
dataset is real, large, free, genuinely messy, and carries a real schema
break — not because the project is about GitHub. **The domain is
incidental and that is the point.** The same architecture serves a
support-ticket triage queue, a claims backlog, a fraud review queue, or
an underwriting pipeline. Nothing in the platform layer knows or cares
that the items are pull requests.

### Why the name

An almanac is a book of tables indexed by date — you look up what was
true on a given day — *and* a book of forecasts: tide tables, planting
dates, weather predictions. Those are exactly the two pillars of this
system: point-in-time historical lookup, and prediction of what comes
next.

### Positioning

Targets **AI/ML platform engineer** roles. Not data engineer, not
analytics engineer. That distinction drives every scoping decision
below: where a choice would strengthen the DE story at the expense of
the ML-platform story, the ML-platform story wins.

The DE core is still built in full, because it is the credibility floor
— an ML platform with no data platform underneath it is a notebook. But
it is a means, not the deliverable.

---

## 2. The one governing claim

**Point-in-time correctness is the centerpiece.** Every feature computed
for a work item at time T uses only events with `created_at < T`.

This is where label leakage lives. It is invisible in code review,
impossible to bluff in an interview, and it is the single cleanest
separator between engineers who have shipped ML systems and engineers
who have trained models in notebooks. It gets its own test suite, its
own ADR, and its own demo endpoint.

Everything else in this design is in service of making that claim
demonstrable and true.

---

## 3. Architecture

```
GitHub Events API (live, rate-limited)  ──┐
                                          ├─→ INGESTION FRAMEWORK (config-driven, YAML)
data.gharchive.org (hourly .json.gz) ─────┘        │  missing-file / retry / run-metadata
                                                   ▼
                        ┌───────────────────────────────────────────┐
                        │ BRONZE — raw payload as string, replayable │
                        │ partitioned event_date / event_hour        │
                        │ replaceWhere → idempotent                  │
                        └──────────────┬────────────────────────────┘
                                       ▼
                        ┌──────────────────────────┐   ┌──────────────┐
                        │ SILVER — typed, deduped, │──→│ QUARANTINE   │
                        │ schema-version routed    │   │ _failed_rules│
                        │ (legacy_v1 / modern_v2)  │   └──────────────┘
                        └──────────────┬───────────┘
                          ┌────────────┴────────────┐
                          ▼                         ▼
      ┌───────────────────────────┐   ┌──────────────────────────────────┐
      │ GOLD (Kimball)            │   │ FEATURE PLATFORM                 │
      │ dim_repo (SCD2)           │   │ point-in-time-correct as-of joins│
      │ dim_actor / dim_org       │   │ offline store (Delta)            │
      │ dim_date / dim_event_type │   │ online store (low-latency)       │
      │ fact_event                │   │ vector index (ANN as a feature)  │
      │ fact_pull_request (accum) │   │ embeddings pipeline (incremental)│
      │ agg_repo_daily            │   └──────────────┬───────────────────┘
      └───────────┬───────────────┘                  ▼
                  ▼                   ┌──────────────────────────────────┐
            ┌───────────┐             │ ML LIFECYCLE                     │
            │  AI/BI    │             │ training → MLflow registry →     │
            │ (3 pages) │             │ serving endpoint → drift +       │
            └───────────┘             │ training/serving skew monitoring │
                                      └──────────────────────────────────┘

CROSS-CUTTING — Unity Catalog governance · OpenLineage column lineage ·
data contract enforced as a CI test · run metrics → Delta → alerts ·
Terraform (apply/destroy cycles) · GitHub Actions · cost tags + budget alerts
```

### Three load-bearing decisions

**3.1 The feature platform is a peer of Gold, not downstream of it.**
Both read Silver. Gold is modeled for human and BI consumption —
conformed dimensions, SCD2, pre-aggregates. The feature platform is
modeled for machine consumption — entity-keyed, point-in-time correct,
no conformed dimensions. Collapsing the two is the most common mistake
in this space, and "why are these separate?" is a question this design
can win. → ADR-002.

**3.2 The vector index lives inside the feature platform, not beside an
LLM.** Its consumers are model features (nearest-prior-PRs, semantic
issue dedup, reviewer similarity), and its success metric is *downstream
model lift* — not citation groundedness. This is retrieval as ML
infrastructure rather than retrieval as a chatbot. → ADR-006.

**3.3 Bronze never transforms.** Payload stays a JSON string; parsing is
per-event-type in Silver. A new event type can therefore never break
ingestion. → ADR-001.

---

## 4. Data model

### 4.1 Bronze

Faithful, replayable landing zone.

| Column | Notes |
|---|---|
| `event_id` | from `id` |
| `event_type`, `created_at` | typed minimally |
| `actor_raw`, `repo_raw`, `org_raw`, `payload_raw` | JSON as string |
| `schema_version` | `legacy_v1` \| `modern_v2` |
| `source_file`, `ingested_at` | provenance |
| `event_date`, `event_hour` | partition columns |

`ingested_at` is never conflated with `created_at`. Write mode is
`replaceWhere` on the partition columns, which is what makes replay
idempotent.

### 4.1a `event_id` across eras — the legacy era has none

**Measured 2026-09-01:** 0 of 2,000 legacy events carry an `id` field;
2,000 of 2,000 modern events do.

This invalidates the original design as written — §4.1's "`event_id`,
from `id`", §4.2's `event_id_not_null` rule, and §12 trap 4's dedup-on-
`event_id` requirement are all impossible before 2015.

**Resolution:** `event_id` carries the native id for modern events and a
deterministic **content hash of the canonical raw record** for legacy
ones, with a companion `event_id_source` column recording which.

This is a better fit for the requirement rather than a workaround. Trap
4's duplicates are the *same event repeated* across an hour-file
boundary, so they are byte-identical, so a content hash collides exactly
when it should and never otherwise. Gets an ADR.

### 4.1b Legacy timestamps are not UTC

**Measured 2026-09-01:** every one of 2,000 legacy events carries a
`-07:00` offset (`2014-06-12T14:05:31-07:00`); every one of 2,000 modern
events ends in `Z`.

**Parsing a legacy timestamp as naive UTC shifts it seven hours** —
silently, with no error, past every schema check. For a project whose
entire premise is point-in-time correctness, this is the most dangerous
property in the dataset.

Timestamps are parsed offset-aware and normalized to UTC at the Bronze
boundary. `spark.sql.session.timeZone=UTC` does **not** cover this: it
governs computation and display, not how a string carrying an explicit
offset is read. This gets an explicit regression test asserting a legacy
timestamp lands at the correct UTC instant.

**Measured 2026-09-01, Phase 1 Task 2 — the same trap exists on the
write side, and in the test harness.** Session timezone does not govern
PySpark's conversion of a Python `datetime` in *either* direction.
Measured across **three driver timezones**, not one, because the first
run could not distinguish "shifted by the driver offset" from "shifted by
four hours":

| Driver `user.timezone` | aware → `F.lit` | **naive** → `F.lit` | Spark → `.first()` |
|---|---|---|---|
| `America/New_York` (−4) | 0s | **+4h** | −4h |
| `UTC` (0) | 0s | **0s** | 0s |
| `Asia/Kolkata` (+5:30) | 0s | **−5h30** | +5h30 |

The rule, now supported rather than guessed: **an aware datetime is always
stored correctly; a naive one is read as driver-local wall time; a
collected timestamp is returned as driver-local wall time.** Nothing
raises in any case — the values are valid timestamps, just the wrong
instants.

The `UTC` row is the dangerous one. **Every defect here is invisible on a
UTC CI runner** and appears only on a developer machine, so CI is
structurally unable to catch this class and the guard has to be in the
code. Two consequences, both now enforced rather than documented:

1. `add_ingestion_metadata` **rejects a naive `datetime`**. `datetime.now()`
   is naive, so the wrong call is the one a caller writes by default.
2. **No test asserts on a collected Python `datetime`.** Comparisons are on
   `unix_timestamp()` — an instant, unambiguous. Confirmed by the table
   above: a naive-datetime assertion is green on a UTC runner and red on a
   laptop, the worst available failure shape — environment-dependent, and
   passing exactly where nobody is looking.

### 4.2 Silver

Typed, deduped, validated, per-type flattened. One common `silver.events`
table plus per-type detail tables (`pull_requests`, `pushes`, `issues`,
`stars`, `forks`) and `silver.events_quarantine`.

Quality rules attach a `_failed_rules` **array**, not a boolean — that is
what makes quarantine analyzable by rule rather than a dead-letter bin.
Every rule condition is wrapped in `coalesce(cond, false)`: under
three-valued logic a NULL passes neither `== True` nor `== False`, and
records vanish from both Silver *and* quarantine. This has its own
regression test.

The split is asserted, never assumed:
`valid.count() + quarantine.count() == scored.count()`.

### 4.3 Gold (Kimball)

`dim_repo` is **SCD Type 2** — repos genuinely get renamed and
transferred, so the slowly-changing dimension arises from the data
rather than being invented. `fact_pull_request` is an **accumulating
snapshot**: one row per PR, columns filling in as lifecycle events
arrive, earliest timestamps preserved across out-of-order batches.

`fact_event` and `fact_pull_request` stay separate rather than becoming
one wide fact with sixty mostly-null columns. → ADR-003.

No `dim_time` at timestamp grain — millions of rows storing attributes
the fact already carries. → ADR-004.

### 4.3a Facts are built from the event stream, not from embedded payloads

**A construction rule, and it is load-bearing.** Every fact and every
label is derived from **event-level fields** — `created_at`, `actor`,
`repo`, `type`, `payload.action` — never from the denormalized objects
nested inside a payload.

This was not the original design. `fact_pull_request` originally read
`payload.pull_request.created_at`, `.user`, and `.merged`. That is the
convenient path and it is the fragile one: the embedded object is a
convenience the upstream provider can change unilaterally, while the
event stream is the actual contract.

**It changed for a measured reason.** When `payload.pull_request` was cut
from 48 fields to 5 in October 2025 (§12 trap 12), a payload-native
pipeline stops working entirely. An event-stream-native one keeps
working, because the same facts are still derivable:

| Fact | Payload-native source (fragile) | Event-native source (durable) |
|---|---|---|
| `opened_at` | `payload.pull_request.created_at` | the `opened` event's own `created_at` |
| **PR author** | `payload.pull_request.user` | the `opened` event's `actor.login` |
| `first_review_at` | — | first `PullRequestReviewEvent` time |
| `closed_at` | — | the `closed` event's `created_at` |

**Verified against live post-change data** (2026-08-28): event-level
`created_at` present on 100% of events, and the PR author recoverable
from `actor.login` on 151 of 151 opened events. The primary label
survives the schema break.

What is *not* recoverable from events alone, and is therefore genuinely
era-bound: `merged` vs closed-unmerged, the `draft` flag, PR `title`/
`body`, and PR size. Those come from the second source (§4.5a).

### 4.4 Feature platform

The layer that makes this an ML platform rather than a data platform,
and the reason the project is worth building.

- **Feature definitions** as declarative specs, versioned in git
- **Offline store** in Delta, built by as-of joins that are correct by
  construction
- **Online store** for low-latency serving at prediction time
- **Vector index** as a feature primitive (see §3.2)
- **Leakage test suite** — the invariant is that a feature vector
  computed `as_of` T is byte-identical whether it is computed today or
  recomputed a year from now from the same Delta version

### 4.4a Phase 3 concretized — hand-rolled joins, UC registered (2026-09-03)

Written via `almanac-design-decision` and a brainstorming pass, before any
Phase 3 code.

**Scope: offline only.** §9's Phase 3 row lists the offline store,
as-of joins, feature specs, and the leakage suite — not the online store
or the vector index (the latter is explicitly Phase 5). An online store
with no serving endpoint to feed is speculative infrastructure; it is
built in Phase 4, alongside the endpoint that needs it.

**The as-of join is hand-rolled, not delegated to a managed feature
store.** Checked live (2026-09-03): Databricks has folded its feature
store into Unity Catalog as "Feature Engineering in Unity Catalog" —
`create_training_set` performs a native point-in-time join against any
UC table carrying a `TIMESERIES` primary key, with online-table sync
built in ([docs](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series)).
That is a legitimate current option, and was weighed against Feast
(which pushes the join/transform logic outside its own framework, so
adopting it would not remove the need to write this logic — only add a
second piece of infrastructure to operate). Neither survives against
this project's own history: §3.1's peer-of-Gold separation, `dim_repo`'s
SCD2, Silver's dedup, and the null-safe quality rules were all built by
hand specifically because that is where this project's differentiated
engineering story lives — and point-in-time correctness is CLAUDE.md's
one governing invariant, stated as "invisible in code review and
impossible to bluff." Handing that specific mechanism to a vendor
function is a different trade than handing it DBU pricing math.
**Decision: the join is `almanac.features`'s own code, tested by this
project's own leakage suite against that code — not against a vendor's
documented contract.**

**Middle path: UC registration for governance, without UC for
correctness.** Confirmed live (2026-09-03) against
[Databricks' UC feature-tables docs](https://docs.databricks.com/aws/en/machine-learning/feature-store/uc/feature-tables-uc):
declaring a table a UC time-series feature table is pure DDL —

```sql
ALTER TABLE <table> ADD CONSTRAINT <name>
  PRIMARY KEY (<entity_col>, <event_time_col> TIMESERIES);
```

— available from DBR 13.3 LTS (this project runs 17.3 LTS), with no
dependency on the `databricks-feature-engineering` client. Every
feature-platform table gets this constraint once built, purely for UC
lineage and Features-UI discoverability; the join computation itself
never calls a Databricks feature-engineering API.

**Components.** `almanac/features/`: one append-only Delta table per
feature group in the `features` container (provisioned since Phase 0),
each keyed on its entity column(s) plus `event_time`, computed from
Silver directly — never from Gold, per §3.1. A single as-of join utility
takes a *spine* (entity_id, as_of_timestamp pairs) and, per feature
group, filters to `event_time < as_of_timestamp` (strict; boundary-
tested) and takes the latest row per entity via a window function
(`row_number() over (partition by entity_id order by event_time desc)`),
then joins each feature group onto the spine.

**v1 feature groups — scoped to what Phase 4's SLA-risk model (§5)
needs, not a speculative general framework:**

| Feature group | Entity key | What | Source |
|---|---|---|---|
| `author_activity` | `author_login` | Prior PR count, merge rate, average response latency, as-of the author's own PR-open time | Silver events |
| `repo_activity` | `repo_id` | Rolling event volume, bot share, PR velocity | Silver events — Silver-native, not a read of `agg_repo_daily` (§3.1) |
| `pr_static` | `(repo_id, pr_number)` | `is_draft`, `is_bot_author`, day-of-week/hour of `opened_at` — already known at prediction time, carried through the same spine rather than joined temporally | Silver events |

**The spine is the PR-opened event population** —
`(repo_id, pr_number, author_login, opened_at)` — **recomputed from
Silver on the feature-platform side**, not read from `fact_pull_request`.
It is the same population §5.1's label targets, but re-deriving it here
rather than reading Gold's fact is what keeps §3.1's "peer of Gold, not
downstream of it" a structural fact rather than a claim.

**Leakage suite**, testing the CLAUDE.md invariant directly: write Delta
version 1 of a feature table with events up to T, append version 2 with
events after T, compute the as-of-T join against both versions via
`VERSION AS OF`, assert byte-identical output. Plus an explicit boundary
test that `event_time == as_of_timestamp` is excluded — the off-by-one
class of bug this project has already hit twice (SCD2, dedup) and caught
the same way, with a targeted test rather than by inspection.

---

### 4.5 Dataset scope — how much data, and why

**Measured 2026-09-01** via HTTP `Content-Length` on real files, not
estimated:

| File | Compressed |
|---|---|
| `2025-01-08-9.json.gz` | 113.7 MB |
| `2025-03-15-14.json.gz` | 83.2 MB |
| `2025-03-15-3.json.gz` | 62.5 MB |
| `2014-06-12-14.json.gz` | 6.2 MB |
| `2014-06-12-3.json.gz` | 5.0 MB |

Two consequences. The firehose grew roughly **15×** between 2014 and
2025, so the legacy slice is nearly free. And at a ~86 MB mean over three
2025 samples, a full quarter (**2,208** hourly files — Q3 is 92 days, not
90) is on the order of **~190 GB compressed**. The expansion ratio is now measured at **7.17×**
(§5.1), making that ~1.3 TB uncompressed — too much to pass over
repeatedly inside the credit budget. Tier 3's month is ~444 GB
uncompressed, which is tractable.

#### Window: Q3 2025 — the most recent quarter with full fidelity

**2025-07-01 → 2025-09-30.** Chosen, not inherited: it is the last full
quarter before the October 2025 payload reduction (§12 trap 12).
Confirmed rich at both ends, including the quarter's final hour
(`2025-09-30-23`, 48 keys). Every date sampled inside it measured 48 keys;
the first reduced date observed is 2025-10-15.

#### The governing rule: sample the entity dimension, never the time dimension

The reflex when data gets expensive is to shorten the time window. That
is the wrong lever here, because span is precisely what this project
needs:

- Point-in-time features need a **warm-up period** before the first
  training example can be computed at all
- Train/test must be split **temporally**; a random split is itself a
  leakage bug
- Drift monitoring over a short window demonstrates nothing
- SCD2 renames and complete PR lifecycles need calendar time to occur

So volume is controlled by a deterministic hash sample on `repo_id`,
which preserves *complete* event history for every repo retained.

**Sampling hours is forbidden.** A PR's review event could land in a
skipped hour, silently corrupting the accumulating snapshot and
fabricating SLA breaches that never happened. Hour-level gaps are a data
*defect* this pipeline detects and reports (§12 trap 5); deliberately
introducing them would poison the label.

#### Tiers

| Tier | Slice | Size | Purpose |
|---|---|---|---|
| **0 — Fixtures** | ~4 hours, committed to the repo | ~350 MB | TDD. Tests never touch the network. |
| **1 — Local dev** | 1 week of Q3 2025 | ~14 GB gz | All pipeline development in the local container. |
| **2 — Legacy** | 1 month of 2014 (720 files) | ~3.9 GB gz | Full schema-evolution path. Effectively free. |
| **3 — Cloud volume proof** | **Full Q3 2025 (Jul 1 – Sep 30), full firehose, unsampled** — derived from the calibration run, not chosen | **~185 GB gz** (92 x 2.012 measured) | The Azure burn. The honest "processed the real firehose on a real cluster" claim, with measured throughput and cost per run. **$31.71, 17.2% of the credit.** |
| **4 — Modeling** | **Q3 2025 (Jul 1 – Sep 30), repo-sampled** | tuned to budget | Features, temporal splits, drift. Span without the volume bill. |

Tiers 3 and 4 answer different questions deliberately. Tier 3 proves the
platform processes volume; Tier 4 gives the model temporal depth.
Conflating them is what makes a full unsampled quarter look mandatory
when it is not.

Scoping a model's entity universe is a **modeling decision, not a
platform limitation** — the same call gets made in production, for the
same reason.

**Not building:** a 14-year backfill. Cost with no marginal signal.

#### Tier 3 is sized by calibration, not chosen up front

**Amended 2026-09-01, after the cloud budget was re-scoped.** Tier 3 was
written as "1 month, ~62 GB gz" — a number picked before any throughput
had been measured on a real cluster. It is now **derived**, because the
one input that decides it is still unknown.

Everything needed is measured except one thing:

| Input | Status |
|---|---|
| Slice size (GB gz per day) | measured — ~2.1 GB/day at the 86 MB/hr mean |
| Expansion ratio | measured — 7.17× |
| Cluster cost | measured — **$2.370/hr** (4 × `D4ds_v6` **workers plus a driver — 5 VMs**, Premium). Was recorded as $1.896/hr, which counted the workers only; `num_workers: 4` provisions five machines. **Corrected 2026-09-01, +25%.** |
| Credit remaining | **77.6 cluster-hours** on $184 at the corrected rate (was stated as 97 at the four-node rate) |
| **Cluster throughput (GB gz/hr)** | **measured 2026-09-01 — 13.84 GB gz per *billed* cluster-hour** (36.72 by Spark time alone, 19.50 by wall clock). See `docs/findings/2026-09-01-cluster-throughput.md`. |

So the procedure, fixed in advance so the answer is not rationalized
afterwards:

1. **Calibration run.** Process exactly **one day** (24 files, ~2.1 GB gz)
   through bronze → silver → gold on the target cluster. Record wall-clock,
   DBUs consumed, and dollars.
2. **Derive** GB-gz per cluster-hour and dollars per day-of-data.
3. **Choose the span** as the largest contiguous slice costing **≤ 40% of
   the remaining credit**, leaving the rest for the Photon A/B (§8.2),
   serving (§8.1), and re-runs. Contiguity is non-negotiable — §4.5's rule
   against sampling the time dimension still governs.
4. **Commit the number to STATUS.md with its arithmetic** before the
   backfill starts.

**The decision rule binds in both directions.** If throughput is better
than expected, Tier 3 grows toward the full quarter; if worse, it shrinks,
and shrinking is not a failure. The claim being defended is "processed the
real firehose on a real cluster, and measured what it cost" — that claim
is true at one week and true at a quarter. What would make it false is
quoting a span never actually run.

**Why not just buy the biggest slice the credit allows?** Because an
unrepeatable run is worth less than a measured one. Reserving ~60% leaves
room to re-run after a bug, which on past evidence is likely.

#### Open, and gating Tier 3's final size

The decision needs Phase 0 to measure:

- ~~Uncompressed:compressed ratio~~ — **measured 7.17×, see §5.1**
- ~~Events per hour, and bot share~~ — **measured, see §5.1**
- **Rename frequency** — whether a 3-month window contains enough
  `repo_id` name changes to demonstrate SCD2 at all. If not, the slice
  *moves*, it does not grow
- Duplicate-`event_id` rate across hour boundaries
- Real Databricks DBU + VM pricing for the chosen region and SKU

---

### 4.5a Second source — the GitHub REST API

The config-driven framework's proof obligation was that a new source
onboards via **YAML alone, zero new Python** (§9, Phase 2 gate). That
second source is the **GitHub REST API**, and the choice is no longer
arbitrary — it repairs exactly what the firehose lost.

**Measured 2026-09-02, and the claim did not hold — cleanly.** A second
gzip-file mirror would onboard by YAML alone (`quality_rules` is the only
field the Spark path consumes and it is format-agnostic). A paginated,
authenticated, rate-limited API needed **~123 lines of new Python** — a
config-model extension, a REST client, rate-limit backoff, `Link`-header
pagination. The honest register statement and the line-by-line accounting
are in `docs/findings/2026-09-02-second-source.md`. "Zero new Python" held
for the case it was easy for and broke on the case that matters, which is
a more credible thing to be able to say than an unfalsifiable claim of
full generality.

**Verified 2026-09-01 against the live API:** `/repos/{owner}/{repo}/pulls/{n}`
returns a **48-key** PR object including `merged`, `draft`, `title`,
`body`, `additions`, `deletions`, `changed_files`. The list endpoint
returns 36 keys. **The REST API was never reduced — only the Events
firehose was.** Rate limit is 5,000 requests/hour authenticated.

**The division of labour:**

| Source | Provides | Scale |
|---|---|---|
| GH Archive firehose | Event stream, history, volume, all three schema eras | Millions/hour, free, complete |
| GitHub REST API | Full PR fidelity for current dates — merge outcome, draft, text, size | 5,000/hour, bounded repo set |

This is what makes the project **re-runnable on today's data end to
end**, embeddings included, rather than being a historical artifact. It
is also a genuinely common production shape: a high-volume lossy stream
for coverage, a lower-volume authoritative API for enrichment.

### 4.6 Phase 6 concretized — live streaming ingest and the online store (2026-09-06)

Recorded before any code, same shape as §4.4a and §8.3a. Two live probes
drove the decisions; both are labelled with their `n`, and neither is a
design premise until Task 1 widens it.

**The live Events API is in the reduced era too — measured, not assumed.**
One unauthenticated poll of `api.github.com/events` on 2026-09-06 returned
99 events, of which **29 were `PullRequestEvent`**. Every one carried a
`payload.pull_request` of exactly **5 keys**: `merged`, `draft`, `user`,
`title`, `body` and `additions` were all absent. This is the same
`REDUCED_V3` cut §12 found in the archive, and it confirms §4.5a's claim
("only the Events firehose was reduced") from the live side.

The consequence is structural and it constrains the whole phase: **live
events cannot produce §5.1's label or any text-derived feature.** The
label needs `user` (first response from a *non-author*) and merge
outcome; embeddings need `title`/`body`. So "online feature freshness" in
§9's Phase 6 gate means freshness of the features reduced-era events
*can* support — event counts, arrival rates, inter-event timing, actor
activity — not the champion model's full vector. Phase 6 serves fresh
features and demonstrates the path; it does not re-serve the Phase 4
champion on live data, and claiming otherwise would be the leakage-adjacent
overclaim this project exists to avoid.

**It is a lossy tail, not a firehose.** Measured from the same response's
headers: `x-poll-interval: 60`, `x-ratelimit-limit: 60`/hour
unauthenticated, and a `Link` header terminating at `page=3` — so roughly
**300 events are retrievable at any moment**. Against the archive's own
measured 2026 volume of **~155–162K events/hour** (§12's corrected figure,
itself a Gate 1 correction of an earlier `n=1` claim), a 60-second poll
surfaces on the order of **11%** of the stream.

> **Superseded 2026-09-06 by Task 1's `n=30` measurement: the real figure
> is ~7.1–7.4%, not ~11%.** Authenticated polling for 30 consecutive
> intervals returned **zero id overlap between consecutive polls, on all
> 30** — the window turns over completely inside 60 seconds — at a mean
> 192.2 events/poll, i.e. ~11,500/hour. Two further results changed the
> plan rather than confirming it: the **rate limit is not the binding
> constraint** (~180 requests/hour used of 5,000, so the 7% ceiling comes
> from honouring `x-poll-interval`, a courtesy, not a technical limit),
> and the feed carries a stable **305-second lag**, which is a floor on
> end-to-end freshness that no pipeline work can beat.
> `docs/findings/2026-09-06-events-api-and-online-store-rates.md`.

Two things follow. First, **authentication is mandatory, not an
optimization**: three pages per poll at 60 polls/hour is 180 requests/hour
against an unauthenticated ceiling of 60. Second, and more important, the
**replay harness stops being a nicety.** §11 already required "Live Events
API *and* a replay harness"; this measurement is why. The live feed cannot
demonstrate completeness, late arrival, duplication or out-of-order
delivery, because it is a sampled tail — replay of real GH Archive hours
through the identical streaming path is the only way to force those cases
on demand. The live feed proves the ingest is real; replay proves it is
correct.

**Ingest architecture.** A poller lands raw event JSON to cloud storage;
Structured Streaming reads that landing zone. The streaming layer dedups
on `event_id` inside a watermark — the *same* key Silver's batch dedup
already uses (§4.2), so the streaming and batch paths agree by
construction rather than by coincidence. Exactly-once is Delta's, via
checkpointing plus an idempotent merge, not a hand-rolled ledger. Note
§4.1a's trap applies here as it does everywhere: the legacy era has no
`event_id`, but the live feed is `REDUCED_V3` only, so streaming never
meets that case — recorded so the omission is deliberate rather than
overlooked.

**Online store technology — decided by Gate 2, and the answer changed.**
Databricks **legacy online tables are deprecated and cannot be created or
accessed after 2026-01-15**, which is already past. New online stores are
**Lakebase Autoscaling** projects created via
`fe.create_online_store(name, capacity=…)` and populated with
`fe.publish_table(...)`, from `databricks-feature-engineering>=0.13.0` on
DBR 16.4 LTS ML or serverless. Publish modes are `TRIGGERED` (default),
`CONTINUOUS` (streaming) and `SNAPSHOT`. Checked live 2026-09-06 against
Microsoft Learn's own page (last updated 2026-08-28), not from training
data — an online-tables design would have been dead on arrival.

**The cost shape is the Vector Search trap again, and it is documented
this time.** Databricks states plainly that **"Lakebase scale-to-zero is
not supported"**, alongside "online stores continuously incur costs;
delete online stores that are no longer needed." That is the same failure
mode `2026-09-06-vector-search-live-state-and-teardown.md` measured the
hard way at a flat 4 DBU/hour idle. So the online store is provisioned as
a **bounded window with teardown in the same plan that creates it**,
sized at the smallest capacity unit that works (`CU_1`/`CU_2` of
`CU_1|CU_2|CU_4|CU_8`), and its real idle rate is measured and published
the way the Vector Search rate was. Phase 5's lesson is applied here
*before* the spend, which is the only useful time to apply it.

**Two prerequisite gaps, found by checking rather than assuming.**
`publish_table` requires a primary-key constraint, non-nullable key
columns, and Change Data Feed on the source table:

| Prerequisite | State today | Action |
|---|---|---|
| PK with `TIMESERIES` designation | **Already present** — `features/registration.py` emits it | none |
| `delta.enableChangeDataFeed` | **Missing on feature tables** — set only in `embed/pipeline.py` | Phase 6 task |
| PK columns `NOT NULL` | **Not enforced** | Phase 6 task |

The first row is an unearned win worth naming: §4.4a added the
`TIMESERIES` primary key "for governance/lineage only, not correctness",
explicitly *not* for an online store. It turns out to be the exact
prerequisite `publish_table` demands.

**Scheduling.** §9 placed Phase 6 at Nov 2–8, after the credit expiry. The
project is running ~8 weeks ahead of that schedule, so the cloud window is
**pulled forward**, on the same reasoning §8.3a used for Phase 5 and §5.2
before it. Streaming plus a non-scale-to-zero online store is the most
expensive shape this project has run, which makes the bounded-window
discipline above load-bearing rather than procedural.

**Amended 2026-09-06, after Task 1 measured what was actually left.** The
original wording made the pull-forward a race against the 2026-09-24
expiry. It is not one: §11 now records that **credit expiry is a budget,
not a wall** — modest paid spend afterwards is acceptable provided
resources come down when idle. The window is pulled forward because the
work is ready, not to beat a deadline, and Phase 6 is **not** scoped down
to fit a balance.

What Task 1 did change is the arithmetic everything was being planned
against, and it was wrong in a way worth recording. The project's cost
figures track Databricks DBUs; measured against Azure's own Cost
Management API, **DBUs were only 53% of real spend** — $65.19 of $122.40
between 2026-09-01 and 09-06. Virtual Machines ($30.65), NAT Gateway
($14.97) and Storage ($11.05) made up the rest and appear in no findings
doc. So remaining credit was **~$61.60, not the ~$118** a DBU-only reading
implies. NAT and Storage also bill partly per-transaction rather than
flat, so they rise with activity — an early "standing cost" estimate of
$4.43/day was activity-inflated and the real idle rate is ~$2.30/day. Full
method and numbers in
`docs/findings/2026-09-06-events-api-and-online-store-rates.md`.

**Measured by Task 1, 2026-09-06** (this paragraph previously listed all
three as unmeasured):
`docs/findings/2026-09-06-events-api-and-online-store-rates.md`.

- **Events API throughput** — resolved at `n=30`, see the correction above.
- **Remaining credit** — resolved, and it moved the number the whole phase
  was being planned against. Databricks DBUs are **only 53% of real Azure
  spend** ($65.19 of $122.40 across 09-01→09-06; VMs, NAT Gateway and
  Storage are the rest and appear in no prior findings doc), so remaining
  credit was **~$61.60, not ~$118**. Every earlier cost figure in this
  repo is DBU-only and understates real spend by ~47%.
- **The Lakebase CU rate** — **still unresolved, and named as such.**
  `system.billing.list_prices` returns zero rows for `LAKEBASE` /
  `POSTGRES` / `ONLINE` / `OLTP`, the *second* occurrence of the gap Phase
  5 hit for `VECTOR`/`SEARCH`. Azure's Retail Prices API lists "Premium
  Database Serverless Compute" at $0.26/DBU-hour in `westus3`, which is
  the **probable** meter by naming but an inference, not a confirmed
  mapping — Vector Search's real SKU was only confirmed by provisioning
  it. Task 9 confirms this one the same way. Bounded estimate: **~$6/day
  at 1 DBU/hour, ~$25/day if it behaves like Vector Search at 4.**

  > **Settled 2026-09-07, and the premise above was wrong.** Measured:
  > **0.852 DBU/hour → $12.06/day** on
  > `PREMIUM_DATABASE_SERVERLESS_COMPUTE_US_CENTRAL` at $0.59/DBU-hour,
  > flat across `n = 14` consecutive 10-minute billing buckets. The rate
  > was in `list_prices` **all along** — priced since 2025-06-11 — and so
  > was Phase 5's; both "gaps" were the same search mistake, looking for
  > product names in a namespace that holds meter names. The `n=2`
  > pattern claimed above was therefore one error counted twice. Full
  > correction: `docs/findings/2026-09-07-live-feed-era-and-watermark.md`.

### 4.7 Phase 7 concretized — governance, reporting, and reproducibility (2026-09-07)

Recorded before any code, same shape as §4.4a, §4.6 and §8.3a. Four of
the six decisions below **supersede text written earlier in this doc**;
each says which, because a phase that silently drifts from its own design
doc is the drift §9's own correction note exists to prevent.

**Reporting splits across two tools, because Power BI Desktop cannot run
on the machine this is built on.** Verified 2026-09-07: Power BI Desktop
is Windows-only, with no Mac version and none planned — Microsoft
restated this as recently as September 2025. **This supersedes §7's
"three Power BI pages" and §9's Phase 7 row.** Pages 1–2 (Review SLA
Risk; Model & Platform Health) become Databricks AI/BI dashboards;
page 3 (Developer Engagement) stays Power BI, authored in the browser
Service.

The split is not a workaround, it is the better arrangement on its
merits. `databricks_dashboard` accepts a `file_path` to dashboard JSON,
so pages 1–2 are **version-controlled in the repo and provisioned and
destroyed by Terraform like every other resource here** — where a `.pbix`
is a binary blob no CI can diff or check. Page 3 is import-mode over
`agg_repo_daily`, which is the shape Power BI is genuinely for, and it
keeps §7's non-negotiable limitations panel. The cost of the decision,
stated plainly: two BI surfaces to maintain for one phase, in exchange
for keeping the Power BI signal without letting it dictate the
architecture.

**Corrected 2026-09-07, before Task 9: page 3 becomes a third AI/BI
dashboard, and Power BI leaves the build entirely.** The paragraph above
is kept because its Desktop finding still holds and still forces pages
1–2; what it got wrong is the sentence "page 3 stays Power BI, authored
in the browser Service", which was never checked past Desktop's platform
support. Two things were verified when the gap was found — page 3 carried
an exit-gate row with no owning task in the plan:

- **Publishing from the Databricks UI to Power BI requires a Power BI
  Premium license** (Premium capacity, PPU, or Fabric capacity) plus XMLA
  Read Write on the capacity. Microsoft Learn, *Publish to the Power BI
  service from Azure Databricks*, updated 2026-08-20. That is a paid
  product this project does not hold, on a credit expiring 2026-09-24.
- The one free path — connecting manually from the Power BI service —
  runs on a **free license restricted to My workspace**, which cannot
  share and cannot publish anywhere else (Microsoft Learn, *Power BI free
  user feature availability*). A portfolio report nobody can open is not
  a portfolio report; a screenshot would have been its only artifact.
  Sign-up is also unverifiable in advance here, since Power BI rejects
  personal Microsoft accounts and this tenant's only Global Administrator
  was one.

So the "two BI surfaces" trade above was priced without its real cost.
Page 3 ships as a third `databricks_dashboard`: version-controlled,
Terraform-managed, destroyable with the rest, and carrying §7's
non-negotiable limitations panel unchanged. **The Power BI signal is
carried by the ADR that records this evaluation** (§4.7's Task 11
candidate, *AI/BI over Power BI*) rather than by an unshareable report —
and the project's own ordering rule points the same way: page 3 is
explicitly §7's *analyst* page, and §1 puts the ML-platform story ahead
of the analyst one whenever they compete.

**Lineage is Unity Catalog's own, not OpenLineage. This supersedes §9's
Phase 7 row**, and it is a decision made against measured state rather
than a preference:

- `system.access.column_lineage` is **already enabled and already
  populated** — 18,102 rows spanning 2026-09-02 → 09-07, which is Phases
  2 through 6 captured with **zero instrumentation work ever done**.
- OpenLineage's value is a common language across heterogeneous
  execution environments. Almanac has one. Instrumenting a Spark listener
  to re-emit what UC already recorded would be ceremony bought at the
  price of real machinery.
- **The trap that shapes the task, measured before writing it:** of
  Almanac's **4,778** column-lineage rows across **29** distinct sources,
  **4,133 — 86.5% — carry only `source_path`, never
  `source_table_full_name`**, because Bronze, Silver and the feature tier
  are external Delta paths rather than registered tables. This is
  documented behavior, not a defect. A lineage query written the obvious
  way, filtering on table name, would return **13.5% of the graph and
  report no error.**

What UC cannot see is stated in the artifact rather than hidden by it:
**local Spark runs are invisible** (which is the entire test suite), and
the system tables keep a **rolling 1-year window** — Catalog Explorer and
the lineage API retain indefinitely for lineage captured after
2024-09-01. Accepting UC means accepting no vendor-neutral lineage
export. That is the trade, and it is worth it here.

**Contracts are extended, not rebuilt — §10's item is already largely
met.** `dbt/models/gold/schema.yml` carries `contract: enforced: true` on
both consumer models, and `tests/integration/test_gold_contracts.py`
proves the build fails on a breach. Recorded here specifically so Phase 7
does not re-derive work Phase 2 already shipped: the remaining gap is the
surfaces carrying **no** contract (the feature tier, streaming Silver)
plus §10's *Documentation* item, a published contract + SLA, which does
not exist in any form.

**Those surfaces are PySpark writing Delta by path, so the mechanism is
Delta's own CHECK constraints plus a shape check before the write — not a
second dbt-shaped thing.** Settled 2026-09-07 by measurement
(`docs/findings/2026-09-07-delta-contract-enforcement.md`), and the split
is not arbitrary. Delta already rejects a *widened* type on overwrite, so
that half needs nothing; it **accepts an overwrite missing a column**, keeps
the column in the schema and nulls every row, which is the one failure a
downstream reader cannot distinguish from real absent data — so the shape is
checked in `contracts.enforce` before anything lands. Row-local invariants
go on the table itself as CHECK constraints applied *by path*, which needs
no metastore and therefore runs identically local and on Databricks, unlike
the UC primary key and CDF statements gated behind `--register`. That also
closes a gap §4.6 recorded and left open: open-source Delta refuses
`ALTER COLUMN ... SET NOT NULL` on a populated table, so nothing outside
Databricks enforced non-null keys — but it accepts `CHECK (key IS NOT NULL)`,
which does. Uniqueness stays a test assertion, since it is not row-local and
no constraint can express it.

**The first thing the contract did was find a defect, which is the
argument for it.** `repo_activity` had two rows under one
`(repo_id, event_time)` — its declared Unity Catalog primary key — on 2 of
3,997 fixture rows, because two events for one repo at the same instant get
different running totals from a ROWS frame. `as_of_join` reads that key and
the online store serves the latest row per key, so both were choosing
between two rows arbitrarily. Fixed by keeping the row whose totals include
every event at that instant, which is what "to date" means.

**The cloud window is narrow, and re-provisioning is itself the
deliverable.** Only the SQL warehouse comes up, only long enough to prove
the dashboards against real Gold, then down. **This supersedes §9's
"second bounded paid window for the final live demo"** for this phase:
the full-stack demo moves to Phase 8. What Phase 7 ships instead is a
documented one-command up/down path, proven by actually being used for
this window rather than asserted — which is what makes Phase 8's demo
cheap enough to run more than once.

**Inference capture must be enabled now. Deferring it does not delay a
panel; it destroys the data.** Measured 2026-09-07:
`almanac-pr-review-sla-risk` is **`READY`**, `scale_to_zero = true`,
serving `almanac_dbx.models.pr_review_sla_risk` v1 — **it was never torn
down**, and it drew **zero inference DBUs on 09-07**, confirming Phase
4's scale-to-zero finding a second time. But `auto_capture_config` is
`null`, so not one request has ever been logged, and the only traffic
that can ever be captured is traffic occurring **after** capture is
switched on.

**The mechanism is `ai_gateway.inference_table_config`, not
`auto_capture_config`** — corrected during Task 1, before anything was
applied. The Databricks Terraform provider still documents
`auto_capture_config` **with no deprecation marker**, but the product
documentation for that mechanism is formally retired ("no longer
supported") and directs to AI Gateway. The provider trails the product,
so the **provider's silence is not evidence** — the same shape as Phase
5's finding that the PyPI rename ran ahead of the CLI surface, in the
opposite direction. This is the second time a gate-2 check has caught a
load-bearing API as superseded before design hardened around it.

That correction also **replaced the one-way constraint recorded here.**
The rules first written down — payload logging cannot be re-enabled once
disabled, and catalog/schema/prefix cannot change after setup — belong to
the **legacy** mechanism. The real constraint runs the other way: once AI
Gateway inference tables are enabled, **the endpoint cannot switch back
to legacy tables**. Enabling on an existing endpoint that has no
inference table configured is explicitly supported, which is exactly this
endpoint's case. Less irreversible than first stated, and the corrected
version is the one that governs.

What is genuinely irreversible is the data: **the log begins at the
moment capture is switched on and no earlier**, which is the whole reason
this is Task 1.

The table lands in its **own schema**, not alongside the registered
model. Databricks also creates an internal
`<payload table ID>_checkpoints` volume beside it, and deleting that
volume corrupts the table; mixing that machinery into the schema holding
the champion model makes both harder to grant on and to reason about.
The schema carries `force_destroy = false` for the same reason the model
registry schema does, and it binds harder here: a prediction log is the
one artifact in this project that **cannot be re-derived at any price**,
because re-provisioning replays no history.

**Phase 6 has no console evidence and cannot acquire any in this phase.**
Its stack was destroyed at Task 9's close. `2026-09-06-console-evidence.md`
covers Phases 2, 4 and 5 only. Named here so the gap reads as a
consequence of a recorded teardown decision rather than an oversight, and
so Phase 8's full-stack window is understood as the only remaining
opportunity — against a *fresh* instance, not the one that produced the
measurements.

## 5. The model

**Primary: PR review-SLA risk.** Given an open PR, predict whether it
will breach a review-latency expectation. A real decision with a real
intervention (reassign, escalate, split), and it forces the
point-in-time work.

**Secondary, and a better story than its size suggests: learned bot
detection.** Ship the regex heuristic first
(`login ENDSWITH '[bot]'` etc.), *measure* its false-positive rate on
names like `robotframework` and `Abbott`, then replace it with a trained
classifier and measure the lift. Shipping a heuristic, measuring your own
error rate, and then beating it is an honest, self-critical arc that
interviewers remember. → ADR-005.

### 5.1 Label definition — measured, not assumed

**Re-probed 2026-09-01 against the chosen Q3 window** (`2025-08-13-14`,
a Wednesday, 167,303 events). Original probe was `2025-03-15-14`, a
Saturday, shown alongside because the differences are themselves
informative.

| Signal | Q3 (2025-08-13, Wed) | original (2025-03-15, Sat) |
|---|---|---|
| PRs opened | **6,618** | 6,352 |
| PRs closed | 6,503 (**77.4%** merged) | 6,015 (77.8% merged) |
| Distinct PRs with a **review** event | **3,550** | 1,574 |
| **Distinct PRs with any human response** | **6,533** | — |
| **Broadening gain from the wider label** | **1.84×** | — |
| Bot events (`[bot]` suffix) | 20.3% | 18.2% |
| Draft PRs opened | **451 (6.8%)** | 127 (2.0%) |

Expansion ratio 7.17× measured separately on the March hour.

**The broadened label is now validated numerically, not just argued.**
Review-only coverage is ~54% of the PR open rate; broadening to *any
human response* reaches ~99%, a **1.84× gain**. Merge rate is stable
across both samples (77.4% / 77.8%), which is reassuring — the two hours
differ in many ways but not in that.

**Draft PRs tripled** (2.0% → 6.8%) between March and August 2025, which
makes the draft-exclusion rule below materially more important than it
looked when it was written.

**The finding that changes the design: formal review events reach only
about one PR in four.** "Time to first review" is undefined for most PRs,
so a model trained on it learns from a biased quarter of the population.

**Therefore the label is *time to first human response*** — the earliest
of `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`, or an
`IssueCommentEvent` whose actor is not the PR author. Time-to-resolution
(closes run ~1:1 with opens) is the secondary target, with near-total
coverage.

Three consequences that are design constraints, not filters to add later:

- **Bot PRs are ~22% of PR events and behave nothing like human ones** —
  Dependabot and Renovate PRs are auto-merged without review. Left in
  they teach the model that PRs resolve themselves. `is_bot` is a
  first-class feature and metrics are segmented by it; the bot
  population is never silently dropped.
- **Draft PRs do not accrue review-SLA time.** They are not ready for
  review by definition.
- **Temporal splits must fall on whole-week boundaries.** The probed hour
  was a Saturday; weekday/weekend review latency differs sharply, so an
  arbitrary split point encodes day-of-week as leakage.

**The label is era-bound, and this doc now says so.** Measured while
writing the Phase 2 plan, against the committed fixtures (2,000 events per
era; see STATUS.md's verification log for the probe):

| Label input | legacy (2014) | modern (2025) | reduced (post-Oct-2025) |
|---|---|---|---|
| `PullRequestReviewEvent` | **absent** — 0 of 106 legacy PRs | 68 | present |
| `PullRequestReviewCommentEvent` | 42 | present | present |
| `IssueCommentEvent` on a PR | 194 | present | present |
| `payload.pull_request.draft` | **absent** — null on all 106 | 5 of 149 | absent |
| `payload.pull_request.merged` | present (35/49 closed) | present | **absent** |

`PullRequestReviewEvent` — the component the "one PR in four" finding is
about — **did not exist before 2015**. A Tier 2 (2014) PR can be ingested
and dimensioned, but its label rests on the *other two* components only.
This is a second, independent argument for the broadened label: §5.1
widened it for coverage, and the width turns out to be what lets the label
survive the era boundary at all. Two rules follow, both already
load-bearing in the Phase 2 Gold code:

- **`draft` exclusion is null-safe, never `draft = false`.** "Not a draft"
  and "the era had no drafts" are different facts; collapsing them makes
  the 2014 slice look like 106 deliberate non-draft PRs.
- **`merged` is read three-way by era.** It is in the legacy and modern
  payloads and gone from the reduced era, so a fact needing it falls back
  to the event stream (a `closed` `PullRequestEvent` plus the merge
  signal) rather than trusting the field to be there.

**Every unlabelled PR states why, and the reasons are a closed set.**
`fact_pull_request.label_exclusion` carries one of five values, and is
null **iff** `time_to_first_response_seconds` is non-null — an invariant
enforced on every build by `assert_label_exclusion_accounts_for_every_row`,
so no row can be silently dropped from the trainable population:

| `label_exclusion` | Meaning | Why it is an exclusion, not a zero |
|---|---|---|
| `author_unobserved` | `author_login` is null — no observed `opened` event carried it | The "first response by someone **other than** the author" rule cannot be evaluated without the author; guessing would fabricate the label's defining condition |
| `draft` | `coalesce(draft, false)` is true | Drafts do not accrue review-SLA time. Null-safe: a legacy null is *unknown*, not "not a draft" |
| `open_unobserved` | `opened_at` is null — the opening was never observed | The duration has no start point. Distinct from `author_unobserved` only because the author is checked first |
| `right_censored` | Open, no response **yet** | The response may still arrive; recording 0 or the window length would both be wrong |
| `closed_no_response` | Closed having never received a non-author response | A real outcome, but not a *latency* — there is no duration to measure |

The distinction that matters for training is between `right_censored` and
`closed_no_response`: the first is a measurement still in progress, the
second is a completed PR whose latency is undefined. Collapsing them would
put "we do not know yet" and "there is no answer" in the same bucket.
**Censoring rate is reported, never hidden** (§13); it was 63.3% on the
committed mid-stream fixture slice, which is a property of a one-hour
window, not of the dataset.

**Volume is abundant, which reinforces §4.5.** At 6,352 PRs/hour, a 5%
repo sample still yields on the order of 230K labelled PRs per month. The
model never needs the full firehose — only the platform does, and only
once.

**Right-censoring is real:** PRs still open at the window edge have no
outcome. They are excluded from training with the exclusion stated, and
the censoring rate is reported rather than hidden.

**Baseline first, always.** No model ships without a measured comparison
against a naive baseline. A model that fails to beat its baseline is a
documented finding, not a failure to hide.

### 5.2 Phase 4 concretized — regression target, single-node training, live now (2026-09-03)

Written via brainstorming, before any Phase 4 code. Phase 3 finished
today, ~18 days ahead of §9's original Sep 21–Oct 4 window — the schedule
assumptions below are revised accordingly, not carried over unexamined.

**The label was never assembled past Gold, and that gap is closed
deliberately, not silently.** §4.4a's feature platform never reads Gold
(§3.1) — but `time_to_first_response_seconds` / `label_exclusion` /
`is_censored` exist only in `fact_pull_request.sql`, and a training set
needs both sides. **Decision: a new training-set-assembly step, outside
`almanac/features/`, joins the label columns onto the feature rows by
`(repo_id, pr_number)`, both pinned to matching Delta versions.** §3.1's
rule was about avoiding leakage inside *point-in-time feature
computation*; a label is definitionally about the future outcome, so the
same reproducibility discipline (pin the version, per Task 7's leakage
suite) applies without extending the same "never touch Gold" boundary to
a different kind of data. The alternative — reimplementing
`int_pr_events.sql`'s response classification and the censoring logic a
second time in Python — was rejected as duplicating already-tested logic
for no correctness gain.

**Regression on seconds, not classification against a threshold.**
§5.1's five-category `label_exclusion` taxonomy was built for a
continuous duration and is already enforced by Gold's own tests;
reframing as binary breach/no-breach would force an undocumented decision
about where `closed_no_response` falls that the taxonomy doesn't answer.
Training targets `time_to_first_response_seconds` directly, over rows
where `label_exclusion IS NULL`. The `/score/pr-review-risk` "risk score"
is the predicted duration compared against an SLA threshold applied at
serving time, not baked into the model — so the threshold can be retuned
without retraining. **The threshold's actual value is measured from the
real distribution during implementation, not assumed here** — the same
discipline §13 already applies to every other number in this document.

**Single-node training, not Spark MLlib.** §5.1 itself argues the volume
is modest — "the model never needs the full firehose... only the
platform does, and only once" — so Spark's job stops at building the
training set; the labelled frame is collected to pandas once (the
governing invariant already proves reproducibility, and pandas is not
back in that concern once the frame is materialized) and trained with
**LightGBM** (4.7.0, current on PyPI as of today) plus a naive baseline
via scikit-learn (1.9.0, current). No model registers unless it beats
the baseline by a measured margin (§5.1's "baseline first, always",
enforced in code rather than left to a human checklist step).

**MLflow tracking + UC registry, validated live today.**
[Databricks' model-lifecycle docs](https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/)
confirm aliases (`MlflowClient().set_registered_model_alias(name, alias,
version)`) are the current, non-deprecated mechanism against
`mlflow.set_registry_uri("databricks-uc")` — stages are legacy. **v1
registers one model version and aliases it `@champion` directly; no
`@challenger` machinery yet** — a real champion/challenger comparison
needs a second trained candidate, which belongs to a future retraining
story, not a first model with nothing to challenge against.

**Serving is Databricks Model Serving's own REST endpoint, invoked
directly — no custom API service.** Confirmed live today against the
[`databricks_model_serving` Terraform resource docs](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/model_serving):
`scale_to_zero_enabled` on the served-model block is current and matches
§8.1's already-stated intent exactly, so no design change was forced
here — only the confirmation that it still holds. The endpoint is stood
up via Terraform (same pattern as Gold's infrastructure), and "serving"
is demoed by invoking that URL directly from a script/test, measuring
real cold start and p50/p95 — matching §8.1's explicit rejection of a
custom web frontend. `GET /model/metadata` is answered by querying UC's
registry APIs directly, same reasoning. **§6's `GET /features/{id}?as_of=`
and `POST /similar-prs` are out of this plan's scope entirely** — the
first is feature-retrieval, not model-serving, and the second needs
Phase 5's vector index, which does not exist yet; both get a real home
when there is a concrete reason to build a custom API service, not
speculatively alongside this phase.

**The cloud step runs now, not in a post-Sep-24 paid window.** §9's
"bounded paid window after Sep 24" framing for live serving was reasoned
from the *original* schedule — Phase 4 landing Oct 5–18, weeks after the
$184 credit expires. Phase 3 finished today instead, and the credit does
not expire for 21 more days. Running Phase 4's real cloud verification
(training run, MLflow experiment, UC registration, and the serving
endpoint itself) against the still-live workspace now spends down
credit that would otherwise be wasted, and gets the cold-start/p50/p95
measurements recorded well ahead of the deadline rather than racing it.
This is a scope-timing decision, not a reversal of §9's underlying
allocation logic (expensive compute funded by free credit still holds —
training and serving remain cheap regardless of when they run).

**Deferred out of this plan, on purpose:** the secondary bot-classifier
upgrade (§5's "measure the heuristic, train, measure lift" arc — real
and cheap, but its own label/feature set, kept independent so this plan
stays about one pipeline end to end); the `@challenger` retraining
workflow; `GET /features/{id}?as_of=` and `POST /similar-prs` as custom
API endpoints; drift/training-serving-skew monitoring beyond what's
measurable from the model's own logged predictions against a later
feature re-pull.

### 5.3 Closing §5.2's deferred question: classification, measured (2026-09-04)

§5.2 chose regression specifically to avoid "an undocumented decision
about where `closed_no_response` falls that the taxonomy doesn't
answer." Task 9's real-cloud run then measured a genuine null result —
LightGBM regression scored `model_mae` 108,890 s against `baseline_mae`
71,917 s, worse — diagnosed as MAE regression losing to a severely
right-skewed target (p50 72 s, mean 20 h, max 91.7 days;
`docs/findings/2026-09-04-model-serving-measured.md`). §5's own opening
line already named the real question — "predict whether it will breach a
review-latency expectation" is a classification framing — so this closes
that loop with the population §5.2 deferred, using data that did not
exist when §5.2 was written.

**Evidence, measured before any decision, per this repo's own gate-1
discipline (n stated, not "measured" left bare):**

Every `label_exclusion` category, queried directly against
`almanac_dbx.gold.fact_pull_request` (the real quarter, one query,
2026-09-04): `author_unobserved` 7,055,396, `right_censored` 5,255,463,
`closed_no_response` 4,363,603, trainable (`label_exclusion IS NULL`)
2,956,518, `draft` 604,003.

The naive move — label every `closed_no_response` row a breach, since it
never got a response — was checked before it became a decision, not
after: of the 4,363,603 `closed_no_response` rows, only **1,130,833**
were open at least 1,487 s (the measured SLA threshold, below) before
closing without response; **3,232,770** (74%) closed *faster* than the
threshold with no response at all — the SLA window never had the chance
to be exceeded, so labelling those a breach would have been wrong for
three-quarters of the category. Caught by running the check, not by
reasoning about it.

**The decision:**

- **Trainable population**: `label_exclusion IS NULL OR label_exclusion
  = 'closed_no_response'`. `author_unobserved` stays excluded (no
  `opened_at` anchor — nothing to derive from). `right_censored` stays
  excluded — the PR is still open, its true outcome is not yet known,
  and labelling "no response yet" as "no breach" would be exactly the
  point-in-time-correctness violation this project's one governing
  invariant exists to prevent. (Treating a still-open PR that has
  *already* sat past the threshold as an immediate breach is a real,
  separate idea — survival-analysis-shaped, and deliberately deferred,
  not folded in here.) `draft` stays excluded, unchanged from §5.2.
- **`breach` derivation** — computed at training time from columns
  `fact_pull_request` already exposes, not a new Gold column:
  `label_exclusion IS NULL → time_to_first_response_seconds > threshold`;
  `label_exclusion = 'closed_no_response' → (closed_at - opened_at) >=
  threshold`. Same §3.1 boundary reasoning §5.2 already used for the
  continuous label: a label is supervision about the outcome, not a
  point-in-time feature, and Gold's contract (the continuous truth,
  useful to other consumers such as the reporting layer) stays
  unchanged — no dbt model touched, no re-run of the Gold job.
- **The threshold is reused, not recomputed**: **1,487 s (p75, ≈25
  min)**, the value Task 9 already measured over the *narrower*
  regression population. Recomputing it over the wider population would
  make "the SLA" move depending on which rows happen to be includable
  this time, which is the wrong kind of number to build a product
  threshold from; 1,487 s is kept as the stated, external constant both
  populations are measured against.
- **Net population: 7,320,121 rows — 2.48× the regression's 2,956,518.**
  This is a structural property of the reframe, not a side effect: a
  duration regression cannot use a row with no duration, and a yes/no
  classifier can, once "did it breach" is answerable even where "by how
  much" is not.
- **Measured breach rate: 25.55% overall (1,869,921 / 7,320,121)** — a
  real, moderate class imbalance, not assumed. Per `is_bot_author`,
  measured rather than guessed at: **bot 32.16%** (826,401/2,569,367),
  **human 21.97%** (1,043,520/4,750,754) — bots breach *more* often
  under this definition, the opposite of the pre-measurement intuition,
  and stated because it corrected an assumption rather than confirmed
  one.
- **Model**: `LGBMClassifier` (`lightgbm` 4.7.0, already pinned;
  confirmed current and sklearn-API-compatible, live,
  2026-09-04 — [LightGBM docs](https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.LGBMClassifier.html)).
  Same ten `FEATURE_COLUMNS` §5.2 already trained on.
- **Baseline**: the per-segment breach *rate* (mean of `breach` per
  `is_bot_author`), used as a predicted probability — the same
  "segment → statistic" shape as §5.2's per-segment median, extended to
  the classification statistic.
- **Metrics and the beats-baseline gate, checked live** ([MLflow's 2026
  evaluation guidance](https://mlflow.org/docs/latest/ml/evaluation/)):
  "for imbalanced datasets, PR-AUC tells you more than ROC-AUC" — at
  25.55% positive this is exactly that shape, so **average precision
  (PR-AUC) is the primary, higher-is-better gate**, with ROC-AUC and
  log-loss logged alongside for the full picture. Registration still
  requires `register AND beats_baseline`, unchanged from §5.2's
  code-enforced gate — only the metric's direction changed.
- **A comparison sweep, not one shot**: at least two `LGBMClassifier`
  configurations — plain defaults, and `is_unbalance=True` (LightGBM's
  own built-in answer to the measured 25.55%/74.45% split) — each
  logged as its own MLflow run in the same experiment alongside the
  baseline, so the result is a real comparison table rather than a
  single number standing in for "the model." Whichever candidate has
  the best PR-AUC registers, if and only if it beats the baseline.
- **Same registered model name, `pr_review_sla_risk`**: §5.2's
  regression run never registered anything (`beats_baseline = False`),
  so there is no existing v1 to collide with — the classifier, if it
  wins, becomes the model's actual first version, and
  `databricks_model_serving.pr_review_sla_risk`'s existing
  `entity_version = "1"` needs no Terraform change either way.
- **Phase 4 continues; this is not a new phase.** New tasks are added
  to the existing Phase 4 plan rather than opening a Phase 4.1 — this is
  the same product question (§5's opening line), the same model name,
  the same feature set, closing a question §5.2 explicitly deferred
  rather than starting a new capability area. §5.2's regression result
  is **not deleted**: it stands as a real, measured null result and the
  reason this section exists, cross-referenced from both directions.

**Still explicitly deferred, not silently dropped**: `right_censored`
inclusion via a survival-analysis-shaped "already past the threshold and
still open" label; per-segment or per-repo thresholds instead of one
global constant; probability calibration; the `@challenger` retraining
workflow §5.2 already deferred.

---

## 6. Endpoints

| Endpoint | Returns | Why it earns its place |
|---|---|---|
| `POST /score/pr-review-risk` | Risk score, top feature contributions, model version, feature-set version, latency | The decision. Proves a model serves, not just trains. |
| `GET /features/{entity_id}?as_of=<ts>` | Point-in-time feature vector | **The platform endpoint.** Proves a feature store exists as infrastructure. |
| `POST /similar-prs` | ANN neighbours + similarity scores | Vector index as a feature primitive. |
| `GET /model/metadata` | Registered version, training run, training-data Delta version, metrics, lineage | Reproducibility — ties a live prediction to the data that produced it. |
| `python -m almanac.runner --source X --backfill A:B` | Run-metadata row | The config-driven framework; adding a source is YAML-only. |

**The demo that carries the interview** is `/features/{id}?as_of=`.
Called at two different timestamps for the same entity, it returns
different vectors, and the earlier one provably contains no information
from after its timestamp. Point-in-time correctness is invisible in code
review; this makes it visible in two commands.

*Optional, clearly labeled as a nice-to-have:* one LLM call producing a
natural-language explanation of a risk score, generated strictly from the
model's actual feature contributions so the number never originates in
the LLM.

---

## 7. Reporting — three Power BI pages

> **Superseded on the tool, not the content (§4.7, 2026-09-07).** All three
> pages ship as Databricks AI/BI dashboards; Power BI is out. The page
> definitions below — including the non-negotiable limitations panel — stand
> unchanged. §4.7 carries the two licensing findings that forced it.

Cut from the guide's four. "Repository Deep Dive" is dropped as the least
differentiated page per hour spent.

**1. Review SLA Risk — the decision page.** Open items ranked by
predicted breach risk with the intervention list; predicted vs. actual as
items resolve; calibration curve; risk by repo. Exists to prove the model
serves a decision someone would act on.

**2. Model & Platform Health — the rare one.** Precision/recall/
calibration over time, feature drift, **training/serving skew**, feature
freshness lag, prediction volume and p50/p99 latency, Bronze→Silver→Gold
row funnel, quarantine rate **by which rule fired**, schema-version
distribution visibly showing the 2015 break, run duration and cost per
run. Almost no portfolio has an MLOps observability page; this one is
built most carefully.

**3. Developer Engagement — the analyst page.** Activation funnel,
retention cohorts, contributor concentration and bus-factor ranking,
time-to-merge as **percentiles, never a mean**, bot/human toggle
filtering the whole page. Carries the metric-definition and
interpretation signal.

**Non-negotiable:** a limitations panel on page 3 — stars are gross not
net, bots excluded by default, commits capped at 20 per push. The
cheapest high-signal element in the project.

Import mode against `agg_repo_daily`, not DirectQuery against
`fact_event`. Measures, not calculated columns. Before/after load time
measured and reported.

---

## 8. Tech stack

| Layer | Choice | Note |
|---|---|---|
| Compute | PySpark on Databricks (Azure) | The genuine capability gap this project closes |
| Storage / format | ADLS Gen2 + Delta Lake | ACID, time travel, `MERGE`, schema evolution |
| Feature store | Built, not bought | The build *is* the demonstration; a managed store hides the skill |
| ML lifecycle | MLflow (tracking, registry, serving) | Native to Databricks |
| Streaming | Spark Structured Streaming over the live Events API | Watermarks, late arrival, exactly-once |
| Transform (Gold) | dbt | Market-demanded; scoped to Gold, not Silver |
| Orchestration | Databricks Workflows | Native, no extra service to run |
| Governance | Unity Catalog + OpenLineage | UC requires the **Premium** workspace tier |
| IaC | Terraform | Apply/destroy cycles are a cost control, not a demo |
| CI/CD | GitHub Actions | Contracts and DQ enforced as build failures |
| Language | Python 3.12+, `uv`, `ruff`, `mypy --strict`, `pytest` | Current-generation tooling only |
| BI | ~~Power BI~~ → **Databricks AI/BI (§4.7, 2026-09-07)** | 3 pages, defined as committed JSON. Power BI Desktop is Windows-only, and both cloud paths are Premium-gated or unshareable |
| Local dev | Single Docker container, `pyspark` + `delta-spark`, `local[*]` | **Not** a Spark master/worker Compose cluster — slower at this volume and teaches nothing |

### 8.1 Serving topology

**Databricks Model Serving with `scale_to_zero_enabled: true`,
Terraform-managed.** Verified against the Azure docs: scale-to-zero is a
first-class field on custom-model endpoints, so "on-demand" and "managed
Model Serving" are not competing options — scale-to-zero *is* the
on-demand mode. Idle cost is near zero; the authentic managed-endpoint
story is kept intact.

Cold start is reported at roughly 10–20 seconds, occasionally minutes,
with no SLA. Irrelevant for this use case and documented rather than
hidden.

Model Serving accepts a model registered in **Unity Catalog or the
Workspace Model Registry**, so it does not force the UC/Premium decision.
Those remain independent.

**~~Unverified and on the Phase 0 pricing list~~ — now measured
(2026-09-01, Azure Retail Prices API, `westus3`).** The per-launch charge
is **$0.07** exactly and the Model Serving DBU rate is **$0.07/DBU**, not
the ~$0.08 community figure. Both community numbers were close but the
DBU rate was wrong by ~14%.

**One figure remains unverified: cold start.** "Roughly 10–20 seconds,
occasionally minutes" is still community-sourced. It is cheap to measure
directly and must be, before it is quoted anywhere — §13's rule applies to
it like anything else.

#### A live endpoint, not a described one

**Added 2026-09-01.** The endpoint is kept **up through a real demo
window** rather than created, screenshotted, and destroyed. At $0.07/DBU
with `scale_to_zero_enabled`, idle cost is near zero and the marginal
spend is bounded by actual invocations, so this is affordable in a way it
would not be on an always-warm endpoint.

What it buys is a claim the artifact cannot otherwise make: a URL that
answers. The measurable deliverables are a **measured cold-start
distribution** (replacing the community figure above), a **measured
p50/p95 warm latency**, and the **dollar cost of the whole demo window** —
each a number, none of them quotable in advance.

**Explicitly rejected:** Azure Data Factory (orchestration duplicated by
Workflows), Kubernetes (cargo-culting at this scale), multi-cloud,
a custom web frontend.

---

### 8.2 Photon — an A/B designed to permit a null result

**Added 2026-09-01.** Photon is enabled or not enabled on a cluster; the
interesting thing is not the toggle but that **its benefit on this
workload is genuinely uncertain**, and the uncertainty is specific rather
than hand-waved.

**Measured:** the Photon Jobs Compute meter bills at **the same $/DBU as
non-Photon** ($0.30 Premium, Retail Prices API, `westus3`). That is a
*rate*, not a total — a Photon-enabled cluster consumes more DBUs per
hour — so the cost question is entirely "DBUs consumed vs. wall-clock
saved", which is exactly what an A/B measures.

**Validated against current vendor guidance (2026-09-01):** Photon does
**not** support UDFs or the RDD API, and **JSON parsing of unstructured
documents has only partial coverage**. When Photon meets an unsupported
operation it falls back to Spark — the job still runs, but the higher DBU
consumption is paid on the Photon portion regardless.

That matters here more than it would on a typical benchmark, because
**bronze ingest is almost entirely JSON parsing** — the case vendor docs
name as partially covered. So:

**Hypothesis, stated before the run:** Photon helps **silver and gold**
(joins, aggregations, window functions over columnar Delta) materially,
and helps **bronze ingest** little or not at all. The blended result may
be a wash or a loss.

**Method.** Same slice, same cluster shape, same code, one variable:

| Measure | Bronze | Silver | Gold |
|---|---|---|---|
| Wall-clock, Photon off | | | |
| Wall-clock, Photon on | | | |
| DBUs consumed, off / on | | | |
| **$ per GB processed** | | | |

Per-layer, not blended — a blended number would hide exactly the effect
the hypothesis predicts.

**A null or negative result is a finding and gets published as one.** "We
measured Photon on a JSON-heavy ingest path and it did not pay for itself"
is a more useful and more credible statement than an unmeasured assumption
in either direction. Reporting it only if favourable would make the
measurement worthless.

### 8.3 Retrieval index — sized before it is chosen

**Added 2026-09-01.** §4.4 already fixes the *role*: retrieval is **feature
infrastructure**, not a chatbot — nearest-neighbour lookup over PR/issue
text producing features a deterministic model consumes. What was never
settled is the *index*.

**Measured inputs** (Phase 0 Task 8, real texts through
`all-MiniLM-L6-v2` on CPU):

| | Measured |
|---|---|
| Embedding throughput | **151.1 texts/sec** |
| Vectors, 30-day slice at 5% repo sample | **~218,000** |
| Vectors, 30-day slice at 100% | **~4.6 million** |

**Validated against current practice (2026-09-01):** the 2026 consensus is
that below roughly 10M vectors, `pgvector` on Postgres you already run, or
an in-process FAISS index, is the default — and a managed vector service
earns its cost through persistence, metadata filtering, hybrid search, or
multi-tenancy, not through vector count alone. **Both of our figures sit
under that threshold**, including the 100% case.

So the choice is *not* settled by scale, and pretending otherwise would be
dishonest. It is settled by which properties the feature path actually
needs:

- **Point-in-time filtering is mandatory.** A neighbour lookup at time *T*
  must not return a document created after *T*. This is the governing
  claim (§2) applied to retrieval, and it is a metadata-filter requirement,
  not a similarity requirement — it is the single hardest constraint on
  the choice.
- **Rebuild cost matters more than query latency.** This is batch feature
  computation, not an interactive endpoint. p95 query latency is close to
  irrelevant; index rebuild time is not.
- **Governance.** An index registered in Unity Catalog inherits the lineage
  story the rest of the platform already tells; a FAISS file on disk does
  not.

**Decision rule, fixed in advance:** default to **Databricks Vector
Search** for the UC lineage and the native point-in-time filtering, and
**fall back to FAISS rebuilt per batch** if its measured cost exceeds ~10%
of remaining credit or if point-in-time filtering cannot be expressed
cleanly. Either outcome is written up with its numbers — including, if it
happens, "the managed service was not worth it at this scale", which the
validation above suggests is a live possibility.

**What is explicitly not being built:** a RAG chatbot over GitHub data.
It would be the third-most-common portfolio project in existence and it
would violate §2 — an LLM must never produce a number a decision depends
on.

### 8.3a Phase 5 concretized — real scale, point-in-time retrieval, measured lift (2026-09-04)

Written via `almanac-design-decision` and a brainstorming pass, before any
Phase 5 code. Every prior phase has finished weeks ahead of §9's original
window (Phase 4 landed Sep 4 against an Oct 5–18 slot), and the $184
credit does not expire until Sep 24 — 20 days out. **The cloud step runs
now, against the still-live credit, for the same reason §5.2 gave Phase
4's live serving the same treatment**: §9's "funded by a bounded paid
window after Sep 24" framing for retrieval was reasoned from a schedule
that no longer holds, and running now spends credit that would otherwise
be wasted rather than reversing the underlying allocation logic (expensive
compute still funded by free credit). ≈$145 of the $184 credit remains as
of this writing (STATUS.md, Task 9's cost line) — the budget this section
sizes against.

**Gate 1 — the 5% toy-sample projection §8.3 was written against is now
stale, and must not be reused.** §8.3's "~218k at 5%, ~4.6M at 100%"
numbers came from Phase 0 Task 8: a 30-day slice, projected from one
hour's PR rate, before Tier 3 ever ran. The real Q3 2025 backfill now
exists at 92 days, unsampled — and §5.3's own real query against
`fact_pull_request` already counted **20,234,983** PR rows in that
population (the sum of every `label_exclusion` category), before a single
issue is added. That is 4–5× the old 100%-sample estimate, not because
the old measurement was wrong, but because it measured a different
thing (a 30-day repo-sampled projection) than what now actually exists
(a 92-day unsampled backfill). **Task 1 below re-measures the real corpus
— PR and issue title+body counts, against the real Gold/Silver quarter —
before any sample-rate or index-technology decision is made from it.**
Restating the old figures as current would repeat the exact mistake this
project's own design-decision skill is written to prevent (Gate 1's own
two worked examples).

**The as-of join utility (§4.4a, `as_of_join`) does not generalize to
retrieval, and reusing it would be wrong, not just inconvenient.**
`as_of_join` answers "the latest feature row before T" — a carry-forward
window function, one ordered timeline per key. A neighbor lookup answers
a different question: "the closest vectors among all candidates valid
before T" — there is no single ordering that makes that a carry-forward.
Phase 5 needs a genuinely new retrieval primitive, not an extension of
Task 8's.

**Brute-force nearest-neighbor at this scale would repeat a mistake this
project has already made twice.** `as_of_join`'s original shape and
`compute_author_activity`'s self-join both blew up into a real O(n²) cost
on the same real quarter (11.7 PiB of intermediate,
`docs/findings/2026-09-04-author-activity-self-join.md`) before being
fixed. A point-in-time-filtered candidate set grows with the corpus (every
PR opened before the query PR), so a naive self-join over 20M+ rows to
find nearest neighbors is the same trap a third time. This is the concrete
argument for real ANN infrastructure here, not just the governance
preference §8.3 already stated — at this row count, exact brute force is
not a simpler fallback, it is the thing that breaks.

**Gate 2, live (2026-09-04): point-in-time filtering expresses cleanly on
Databricks Vector Search, closing half of §13's open item.** Confirmed
against the [Vector Search filtering guide](https://docs.databricks.com/aws/en/vector-search/vector-search-filtering-guide):
metadata filters accept an operator encoded in the key (`{"opened_at <":
value}`), combined with the similarity query itself, over `TIMESTAMP`
columns — a native range predicate, not a workaround. The other half —
cost — is **not** disclosed on either the [cost-management guide](https://docs.databricks.com/aws/en/ai-search/cost-management)
or the public pricing page (both fetched live today; neither exposes a
dollar figure, the same shape of gap Phase 2 hit for DBU rates and
resolved by querying `system.billing.list_prices` directly rather than
guessing). **Task 1 pulls the real Vector Search SKU rate the same way**,
before committing to it over the FAISS fallback. What is confirmed:
billing splits into DBU (serving/ingestion) and DSU (storage); a Standard
endpoint's one vector-search unit covers ~2M vectors at dimension 768 —
**~4M at our dimension 384** (`all-MiniLM-L6-v2`), so the re-measured
corpus (Task 1) plausibly fits in one unit, but that is now a number to
check, not assume. Terraform support is current:
[`databricks_vector_search_endpoint`](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/vector_search_endpoint)
(`endpoint_type = "STANDARD"` is the value confirmed available; whether a
storage-optimized type is exposed through this resource is unverified and
gets checked against the live provider schema, not assumed) and
[`databricks_vector_search_index`](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/vector_search_index)
(`primary_key`, `index_type`, syncs from a UC Delta table — the same
"UC registration for governance" shape §4.4a already used for feature
tables).

**Decision rule stays exactly §8.3's, now with both halves checkable
against real numbers**: Vector Search unless Task 1's real SKU rate
exceeds ~10% of the ~$145 remaining (~$14.50), in which case FAISS
rebuilt per batch — cheap at this vector count for the *rebuild*, but the
point-in-time query path would still need the metadata-filter-then-search
logic hand-rolled, since FAISS has no native filtered ANN. That cost is
real and belongs in the same comparison, not left out because FAISS's
sticker price looks like zero.

**Similarity features must respect point-in-time correctness on two axes,
not one.** The query side is the metadata filter above (`opened_at <
as_of`). The **label** side is a second, easy-to-miss leak: a neighbor's
*breach outcome* is only knowable once that neighbor's own PR has closed
— an unresolved neighbor's outcome is unknown, not "not yet breached",
the exact distinction `author_activity`'s "unclosed prior PR counted as
unknown rather than not-merged" already established for a different
feature (§4.4a). `pr_similarity` (new feature group, entity key
`(repo_id, pr_number)`) computes similarity stats against the neighbor
set unconditionally, but any neighbor-outcome-derived feature (e.g. "share
of similar prior PRs that breached") is null for neighbors unresolved as
of the query time, not defaulted to non-breach.

**Success is downstream lift on the existing champion, or an honest
null — matching the phase table's own gate exactly.** `pr_similarity`
features get added to the classification frame Task 12 already built
(Phase 4), retrained through the same comparison-sweep/PR-AUC-gate
machinery, and compared against the registered champion's measured
**0.612 PR-AUC** — promoted only if it wins, left alone and written up
if it does not. This is the same "no ship without beating a measured
baseline" discipline §5.1 already enforces in code, one level up: the
baseline being beaten is now the current champion, not the naive
segment-rate model.

**`POST /similar-prs`: a demoed query path, not a persistent custom API
service.** Same reasoning §5.2 already gave for deferring `/similar-prs`
and `/features/{id}?as_of=` out of Phase 4 — no custom API service exists
yet, and building one speculatively is the mistake, not the missing
endpoint. Phase 5 is the first phase with a concrete reason for the
`/similar-prs` half (the index now exists), so it gets a thin script/CLI
demoed the same way the serving endpoint was demoed — direct queries
against the live index, measured, not wrapped in a new always-on service.
`/features/{id}?as_of=` stays deferred; nothing in Phase 5 gives it a
concrete reason yet.

**Deferred out of this section, on purpose:** hybrid/lexical search and
reranking (§3.2's own "not a chatbot" framing gives this no product
reason to exist here); embedding models other than `all-MiniLM-L6-v2`
(already measured feasible on CPU, §8.3); a standalone issue-dedup product
surface (issue embeddings feed the same index and the same point-in-time
discipline, but get their own feature/product framing only if a concrete
use surfaces); GPU inference; the `@challenger` retraining workflow.

---

## 9. Phasing — built around the credit deadline

**The binding constraint: $184 of Azure credit expires 2026-09-24, 23
days from today.** The conventional sequencing — cloud work as a late
phase, once everything is built and polished — would land in early
October, after the credits are gone. So the sequencing inverts.

**Credit allocation strategy: spend the free credits on the
compute-hungry work, and pay for the cheap work later.** Processing a
quarter of GH Archive on Spark job clusters is the expensive operation;
a small training run and a serving endpoint are not. Therefore the free
credits fund the *data platform* proof, and a bounded pay-as-you-go spend
after Sep 24 funds the *ML platform* proof.

#### Revised allocation, 2026-09-01

The original plan spent an estimated $25–40 of $184 — leaving most of the
credit to expire unused, which is a waste, not thrift. Four additions
absorb the surplus, each chosen because it converts an *assertion* in this
document into a *measurement*:

**Corrected 2026-09-01, same day.** An earlier draft of this table listed
all four additions as funded by the expiring credit. **Two of them cannot
be.** Live serving lands in Phase 4 and retrieval in Phase 5 — mid-October
and later, weeks after the credit expires on **Sep 24**. The original
strategy above was right and the correction restores it: *the credit funds
the data platform proof; a bounded paid window funds the ML platform
proof.*

**Funded by the expiring credit** (must complete by Sep 24):

| | Adds | Converts |
|---|---|---|
| **Calibration run** (§4.5) | ~1 day of data, one cluster-hour or two | throughput from unknown to measured — **gates every other cost figure** |
| **Tier 3, sized by that calibration** (§4.5) | the bulk of the spend | "processed the real firehose" from a claimed span to a measured one |
| **Photon A/B** (§8.2) | ~1 extra run of the calibration slice | an assumption about Photon into a per-layer number |
| **Re-run headroom** | ~60% of credit held back | one bug does not cost the whole claim |

**Funded by a bounded paid window after Sep 24** (cheap, and not
schedulable before it):

| | Adds | Converts |
|---|---|---|
| **Live serving window** (§8.1) | near-zero idle at $0.07/DBU + invocations | a described endpoint into a URL that answers, with measured cold start |
| **Retrieval at real scale** (§8.3) | index build + storage | a 5% toy sample into a sized, justified index choice |

**Budget discipline, in force from 2026-09-01:** a subscription budget
(`almanac-credit-burndown`, $185/mo) alerts at 25/50/75/90/100% of spend
plus a forecast breach. This exists because upgrading to pay-as-you-go
**removes the Free Trial spending limit** — past the credit, the card is
charged. The alerts double as a credit burn-down tracker.

**The ordering is deliberate.** Calibration runs *first*, because every
other number depends on throughput; the Photon A/B reuses that same slice
rather than paying for a new one. Nothing is allowed to consume the budget
the backfill needs.

#### Resequenced 2026-09-01, after Phase 0

Three things changed the schedule, and the revision is recorded rather
than silently applied:

1. **Phase 0 finished on Sep 1, not Sep 7** — six days of slack.
2. **Infrastructure is already applied.** The old Phase 2 read "lift to
   Azure" as future work; the workspace, lake, and containers exist now.
3. **The old schedule put the Azure burn at Sep 15–24 — the last ten days
   before expiry, with zero margin.** Phase 0's own record (four defects
   in one task, three CI failures, a region that had to change) says that
   is not a schedule, it is a hope. A one-week slip would have expired the
   credit unused and left the project's most expensive claim unmade.

**The slack is spent on the deadline, not on getting ahead elsewhere.**

**The key structural change: calibration moves out of Phase 2 and into
Phase 1.** It needs only working bronze ingest and one day of data — not
the Gold layer Phase 2 assumed. Running it the moment bronze works retires
the throughput unknown while there is still time to act on the answer,
turning Sep 24 from a cliff into a planning input.

| Phase | Window | Deliverable | Gate |
|---|---|---|---|
| **0 — Exploration** | **Sep 1 · DONE** | Repo, CI, local Spark + Delta container, ingestion edge, committed fixtures. Schema eras diffed against real files; dataset, label, bot-rule and rename measurements taken. Azure infrastructure applied in `westus3`. | ✅ Measured numbers committed; 69 tests green; infra live, no compute running |
| **1 — Local pipeline + calibration** | Sep 2–8 | Config-driven runner. Bronze ingest with missing-file handling and `replaceWhere`. Silver with dedup, quality rules, quarantine, three schema-era handlers. **Ends with a calibration run on Azure: one day of data through bronze, measuring GB-gz per cluster-hour and dollars per day-of-data.** | Rerun any hour twice → byte-identical content. Null-handling regression green. **Throughput measured and Tier 3's span computed and committed to STATUS.md with its arithmetic** |
| **2 — Gold + the Azure burn** ⚠️ | **Sep 9–20** | SCD2 `dim_repo`, `fact_pull_request` accumulating snapshot, `agg_repo_daily`. **Tier 3 at the span Phase 1 derived** (not a fixed month) + Tier 2 (2014) on job clusters. **Photon A/B on the calibration slice, hypothesis pre-registered (§8.2).** Capture run metrics, UC lineage, cost per run. Tear down. | Backfill completed at the derived span; Photon result published **including if negative**; `terraform destroy` leaves nothing |
| **— credit expires —** | **Sep 24** | Everything above must be done. **Four days of deliberate slack** between Phase 2's end and expiry. | Credit spent on measured work, or explicitly and knowingly not spent |
| **3 — Feature platform** | Sep 21 – Oct 4 | Point-in-time-correct offline store, as-of joins, feature specs, leakage test suite. | The `as_of` demo works; leakage suite green |
| **4 — Model + MLflow** | Oct 5–18 | Measured baseline first, then the SLA-risk model. MLflow tracking + registry. Batch scoring, then a serving endpoint. Drift and training/serving skew monitoring. **Live serving window on bounded paid spend (§8.1)**, measuring cold start. | Model beats baseline by a measured margin, or the null result is documented |
| **5 — Embeddings + vector index** | Oct 19 – Nov 1 | Incremental embedding pipeline, ANN index, similarity features, measured downstream lift. **Index choice decided by §8.3's rule, on measured vector count.** | Measured lift, or an honest documented null result |
| **6 — Streaming** | ~~Nov 2–8~~ → **pulled forward to before Sep 24 (§4.6, 2026-09-06)** | Live Events API ingest, watermarks, late-arrival and exactly-once handling, online feature freshness. **Scope confirmed to include the online store**, deferred since §4.4a. | Live events land and update online features |
| **7 — Governance, BI, docs** | Nov 9–22 | OpenLineage, contracts enforced in CI, ~~3 Power BI pages~~ → **3 AI/BI dashboards (§4.7)**, ADRs, limitations, decision memo, postmortem. Second bounded paid window for the final live demo. | A stranger clones and runs locally in <15 min |
| **8 — Ship** | Nov 23 | Tag `v1.0`. Stop. | — |

**Only Phases 1 and 2 are deadline-bound.** Everything from Phase 3 on is
schedule-flexible, because it runs on local compute or on cheap bounded
paid windows. If anything slips, it must slip *there* — never into the
credit window.

**Start applying at Phase 4.** The repo is presentable once a model
serves; the remaining phases improve it while interviews are already in
flight.

---

## 10. Goals — what "done" means

### Technical
- [ ] Tier 3 (unsampled month of 2025) and Tier 2 (2014 month) ingested, both schema eras through the same framework
- [ ] Legacy events carry a deterministic surrogate `event_id`, and duplicate legacy records dedup correctly (§4.1a)
- [ ] A legacy `-07:00` timestamp is proven by test to land at the correct UTC instant (§4.1b)
- [ ] Tier 4's 3-month repo-sampled span built, with a temporal train/test split
- [ ] Rerunning any single hour produces identical results — idempotency proven, not claimed
- [ ] Adding a source requires only a YAML file, zero new Python — proven by onboarding the GitHub REST API
- [ ] The full pipeline runs end to end on **current** data, not only on the historical window
- [ ] Facts and labels are built event-natively; no fact reads a nested payload object
- [ ] `dim_repo` is SCD2 with at least one real demonstrated rename
- [ ] `fact_pull_request` is a working accumulating snapshot
- [ ] **A feature vector computed `as_of` T is reproducible byte-for-byte a year later**
- [ ] **Leakage test suite proves no feature sees post-T data**
- [ ] Label coverage and right-censoring rate measured and reported, not hidden
- [ ] Bot and human populations segmented in every model metric
- [ ] Temporal train/test split falls on whole-week boundaries
- [ ] Model beats a measured baseline, or the null result is documented
- [ ] Model serves from a real endpoint with measured p50/p99
- [ ] Drift and training/serving skew monitored and visible
- [ ] Streaming path handles late arrival and duplicates correctly
- [ ] Quarantine rate reported per rule, per day
- [ ] Data contract enforced as a CI failure, not a markdown file
- [ ] Column-level lineage available end to end
- [ ] Actor identities pseudonymized in every published artifact (dashboards, memo, screenshots, README)
- [ ] Terraform provisions from zero; `destroy` leaves nothing
- [ ] ≥70% coverage on transformation and feature logic

### Documentation
- [ ] README a stranger can follow to a working local run in <15 minutes
- [ ] Architecture diagram drawn by hand, matching what actually exists
- [ ] 6–8 ADRs
- [ ] Data contract + SLA
- [ ] `docs/limitations.md` — every trap in §12, stated plainly
- [ ] One decision memo with a stated recommendation and a stated confidence level
- [ ] One incident postmortem from something that genuinely broke

### Career
- [ ] 100+ commits across ≥8 weeks
- [ ] Resume bullets with **measured** numbers, never estimated ones
- [ ] Architecture whiteboardable from memory in 5 minutes

---

## 11. Deliberate scope decisions

Recorded so they are not silently re-litigated later. Each of these was
an open choice with a defensible alternative.

| Decision | Rejected alternative | Why |
|---|---|---|
| Target AI/ML platform engineering | Data engineering / analytics engineering | Matches the role actually being pursued; a DE-shaped project would re-prove capability already demonstrated elsewhere |
| ML platform is the deliverable | Pure data platform, no models | Without a model there is no feature store, and without a feature store there is no point-in-time story — which is the whole reason to build this |
| Azure spend front-loaded to weeks 3–4 | Cloud work late, as a final phase | Free credits expire 2026-09-24; a late cloud phase wastes them entirely |
| Free credits fund the data-platform proof; ML serving paid for later | Split evenly, or save credits for serving | Spark backfill at volume is the expensive operation; training and serving are cheap |
| **Credit expiry is a budget, not a wall** (2026-09-06) | Scope phases down to fit the remaining balance | Broadens the row above. Modest real spend after 2026-09-24 is acceptable *provided* it is spent judiciously and **resources are torn down when not in use** — the point of provisioning is to learn, build and document, not to keep anything online. Future demos bring infrastructure up on demand. Scoping a phase down to fit a balance would have traded architecture quality for a constraint that was never real |
| **Teardown ships with provisioning** (2026-09-06) | Tear down as a follow-up task | Two incidents: Vector Search billed a flat 4 DBU/hour idle because nothing scaled to zero, and Lakebase documents the same property up front. A delete path written after the fact is written under time pressure, or not at all |
| 3 report pages (~~Power BI~~ → AI/BI, §4.7 2026-09-07) | 4+ pages | Beyond three, page count stops carrying signal and starts costing hours. The *count* was the decision and it held; only the tool changed |
| dbt included, scoped to Gold only | No dbt, or dbt through Silver | Market-demanded and cheap at Gold; rewriting Silver in dbt would discard the Spark work that is the point |
| Streaming built in Phase 6 | Deferred to a future project | Most-probed interview topic, and a deferred project may never happen |
| Data contract enforced as a CI test | Contract as a markdown document | A contract nothing enforces is a wish |
| Feature store built, not bought | Managed feature store | The build is the demonstration; a managed store hides the exact skill being shown |
| ~13 weeks | Hard 6-week ship | Quality and structure chosen over speed, deliberately and with eyes open |

| Serving | Model Serving, scale-to-zero, Terraform-managed | Always-on endpoint; hand-rolled container | Scale-to-zero *is* on-demand; keeps the managed-serving story at near-zero idle cost (§8.1) |
| Streaming | Live Events API **and** a replay harness | Either alone | Live feed is the authentic claim; replay is the only way to force late/duplicate/out-of-order cases on demand |
| Unity Catalog | Decide in Phase 0 on measured pricing | Commit either way now | Premium raises the DBU rate on *all* compute incl. the backfill; guessing this is expensive either direction |
| Real-person data | Pseudonymize identities in anything published | Publish real logins; or drop individual analysis | Repos are projects and stay named; people are pseudonymized. Keeps the bus-factor analysis without naming individuals as risks in a public portfolio |

| Modeling window | Q3 2025 (Jul–Sep) | Q1 2025, as inherited; or recent 2026 data | Last full quarter before the Oct 2025 payload reduction — the most recent data with full fidelity |
| Fact construction | Event-stream-native | Read the embedded `payload.pull_request` object | The embedded object is a convenience upstream can change unilaterally; the event stream is the contract. Proven when it changed |
| Second source | GitHub REST API | Hugging Face mirror; another archive | Restores exactly what the firehose lost (48-key PR objects, verified live), making the project re-runnable on today's data |

**Not used:** stock cloud-architecture diagrams. The architecture diagram
is drawn by hand and matches the repo one-to-one — a diagram containing
boxes that were never built is a liability, because interviewers ask
about exactly the box you skipped.

## 12. Known traps in the data

Each of these is a real property of GH Archive, each goes in
`docs/limitations.md`, and each is an interview story.

1. **`WatchEvent` means *star*, not watch.** GitHub renamed the feature
   in 2012 and never renamed the event. Actual watching is not in this
   data at all.
2. **There are no un-star or un-fork events.** The metric is "stars
   gained", never "total stars". Labeling it wrong is a factual error.
3. **`PushEvent.payload.commits` is capped at 20.** Use `payload.size`
   and `distinct_size`, never `size(commits)`. Force-pushes inflate
   `size`.
4. **Duplicate event IDs occur across hour-file boundaries.** Dedup is
   functionally necessary, not decorative. **Still unmeasured as of
   2026-09-01** — Phase 0's sample deliberately used non-adjacent hours
   (0,3,6,…) to observe renames over time, so no two consecutive hours
   were ever compared and the boundary condition was never exercised.
   Within-sample duplication was ~0 (1 in 6,002,410), which says nothing
   about the boundary. Measured properly in Phase 1, where the dedup is
   built.
5. **Missing and truncated hours.** Ingestion must distinguish *file
   absent* / *file empty* / *job failed*.
6. **Bots dominate volume — MEASURED 2026-09-01.** 29.0% of events by the
   `[bot]` suffix alone, and **day-of-week dependent** (18.2% on a
   Saturday hour vs 29.0% across three Wednesdays), because scheduled
   automation runs on weekday cadences and humans do not. Any
   unclassified metric is misleading.
   **The heuristic itself needed changing**, see
   `docs/findings/2026-09-01-bot-classification.md`: the documented false
   positives (`robotframework`, `Abbott`) do not match the anchored rule
   at all, while the real ones were far worse — a bare `ci$` clause
   matched 1,002 distinct logins of which **869 (86.7%) were human
   surnames** (Turkish `Yazici`/`Akinci`/`Avci`, Italian
   `Federici`/`Falcucci`). Fixed by requiring a separator.
   **The methodological lesson generalizes:** inspecting top-N matches by
   volume is structurally blind to this, because bots are high-volume by
   definition. Error rates for a rule over a power-law population must be
   computed per distinct entity, not per event.
7. **The 2015 schema break — MEASURED 2026-09-01**, see
   `docs/findings/2026-09-01-schema-eras.md`. Confirmed real, and worse
   than this doc originally described:
   - `repository` (legacy) vs `repo` (modern) — as expected
   - **`actor` is a bare string in legacy, an object in modern** — a type
     change, not a rename. Legacy carries detail in `actor_attributes`,
     and has **no numeric actor id at all**, so cross-era actor identity
     rests on a mutable login
   - **Legacy events have no `id` field — 0 of 2,000.** See §4.1a
   - **Legacy `created_at` carries a `-07:00` offset, not `Z`** — 2,000 of
     2,000. See §4.1b
   - The legacy-only event type observed is **`TeamAddEvent`**, not
     `DownloadEvent`/`FollowEvent`/`GistEvent` as previously assumed —
     those were retired before mid-2014
   - `repo_id` **is** stable across both eras (1,997/2,000 legacy,
     2,000/2,000 modern), so the SCD2 natural key survives
8. **Repos get renamed and transferred — MEASURED 2026-09-01.** **5,757
   renames** across 1,234,736 distinct repos in a 24-hour-file sample
   spanning the candidate quarter, against a gate of 50. `repo.id` is
   stable, `repo.name` is not, and the SCD2 arises naturally. Varied and
   real: ownership transfers, user renames, project renames, typo fixes —
   and at least one repo renamed **twice** inside the window, exercising
   the multi-version path rather than a single transition. **The window
   does not need to move.**
9. **gzip is not splittable.** Parallelism is bounded by file count, not
   file size.
10. **Language is absent from most modern events** — nested in PR
    payloads only. **The reverse holds for legacy:** `repository.language`
    is populated on 1,712 / 2,000 (85.6%) legacy events directly, so
    pre-2015 language coverage is *better*, not worse. Measured
    2026-09-01.
11. **Deleted users and repos** appear as nulls or placeholders in later
    events.

12. **A THIRD schema era, 2025-10 — MEASURED 2026-09-01**, see
    `docs/findings/2026-09-01-third-schema-era.md`. Between **2025-10-08
    and 2025-10-15** `payload.pull_request` was cut from **48 fields to
    5**, losing `merged`, `user`, `draft`, `created_at`, `title`, `body`,
    and every size field. Volume fell alongside it — one 2026 hour holds
    54,232 events against 227,376 in 2025 (−76%), PR events −97%, review
    events −96%. Documented nowhere upstream. Consequences: `SchemaEra`
    has a third member `REDUCED_V3`; Bronze and Silver ingest all three
    eras; facts are built event-natively so the primary label survives
    (§4.3a); and the fidelity the firehose no longer carries comes from
    the REST API instead (§4.5a).

---

## 13. To verify before building — never quote an unmeasured number

**Closed by Phase 0** (each with the measurement, not an assurance):

- [x] **Pre-2015 schema field names** — diffed over 2,000 events/era, §4.1a/§4.1b. Legacy has no `id` at all, `actor` is a bare string, and `created_at` carries `-07:00`
- [x] **Current file sizes** — 2025 hours 62.5–113.7 MB gz; 2014 hours 5.0–6.2 MB gz, §4.5
- [x] **Uncompressed:compressed expansion ratio** — **7.17×**, §5.1
- [x] **Events per hour, and bot share** — 227,376 events/hr peak; bot share **25.6%** on Q3, day-of-week dependent
- [x] **`repo_id` rename frequency** — **3,792** renames on Q3 against a gate of 50. Case-only renames exist, so comparison is case-sensitive
- [x] **Azure/Databricks pricing for the chosen region and SKU** — Retail Prices API, `westus3`, §8.1/§8.2
- [x] **Docker base image for `pyspark` + `delta-spark`** — Java 21 on `python:3.12-slim`; 17 no longer has an installation candidate
- [x] **Unity Catalog tier requirements and DBU impact** — Premium required; also *forced*, since Standard was discontinued for new workspaces 2026-04-01
- [x] **Whether `array_compact` exists** — confirmed available in Spark 4.2.0

**Open — and load-bearing for the four items added 2026-09-01:**

- [x] **Cluster throughput (GB gz per cluster-hour)** — **measured 2026-09-01: 13.84 GB gz per billed cluster-hour**, one day (2025-08-13, 24/24 hours, 3,794,323 rows, 2.012 GB gz) through Bronze on 4 × `D4ds_v6` workers plus a driver, DBR 17.3 LTS, no Photon. $0.3446 per day-of-data. Tier 3 derived from it as the **full Q3 2025 quarter** at 17.2% of the credit — the rule bound upward. Two caveats carried forward: the rate is Bronze-only, and the 2.3× headroom under the 40% cap is what absorbs a slower full-medallion run. `docs/findings/2026-09-01-cluster-throughput.md`.
- [x] **DBUs consumed per node-hour** — **measured 2026-09-03: 1.101 DBU/node-hour** on the Tier 3 backfill (27.788 DBU over 25.235 node-hours), against an assumed **0.75** — the assumption was **47% low**. Rates confirmed from `system.billing.list_prices`: `PREMIUM_JOBS_COMPUTE` and `PREMIUM_JOBS_COMPUTE_(PHOTON)` both **$0.30/DBU**, so Photon's penalty is consumption, never rate. **This §13 entry's own caveat proved exactly right**: the Premium-vs-Standard decision held across the whole sensitivity range and the *absolute* cost per run did not — the backfill was **$14.62, not $11.96 (+22%)**, and Gold **$0.85, not $0.78**. Tier 3 remains 7.9% of the credit against a 40% cap, so no tiering decision moves. DBU consumption is **workload-shaped, not node-shaped** (backfill 1.101 vs Gold 0.883/node-hour), so a single figure must not be quoted across shapes — the same error as quoting Bronze-only throughput for the full pipeline. Access was the whole difficulty and cost two wrong diagnoses: `system.billing` was never disabled (every schema reports `MANAGED`; the earlier "only `ai` and `information_schema`" was a **permissions filter on a non-admin identity**), and the `#EXT#` UPN is Entra's internal representation of a guest, not a sign-in name. The real blocker was `AADSTS500200` — this tenant's only Global Administrator was a personal Microsoft account, which the Databricks account console refuses; resolved with a cloud-only Entra admin user. **Still not measurable: per-layer DBUs** — one cluster, three sequential layers, so any per-layer figure is the run total apportioned by wall-clock share. `docs/findings/2026-09-03-measured-dbus.md`

- [~] **Photon's per-layer effect** (§8.2) — **wall-clock measured 2026-09-03 across three replicate pairs; DBUs still open.** Silver **2.14x** and Gold **1.38x** (every replicate clear of the 1.10 threshold); **Bronze withheld as indeterminate** — 1.18 / 1.05 / 1.23, a range straddling the threshold, because re-running an *identical* arm varies by up to 30% here and the effect under test is ~15%. The hypothesis said bronze would be near zero; that is **neither confirmed nor refuted** — the measurement never had the power to do either, and a single run returned opposite verdicts on two occasions. The DBU half is blocked on the item above, so the cost verdict is inverted instead: break-even on Photon's DBU multiplier is **1.55 / 1.96 / 2.16** for bronze at no-help / mean / best case, straddling its ~2x multiplier. **Decision: do not enable Photon**; the lever is bronze's single-threaded gzip at 64% of execution. `docs/findings/2026-09-03-photon-ab.md`
- [ ] **Model Serving cold-start distribution** (§8.1) — "10–20 seconds" is community-sourced and must not be quoted until measured
- [ ] **Vector index cost and whether point-in-time filtering expresses cleanly** (§8.3) — decides managed Vector Search vs. FAISS
- [x] **Duplicate-`event_id` rate across *adjacent* hours** — **measured 2026-09-02 by the Tier 3 backfill: 62 duplicates in 341,060,851 rows, 1 in 5.5M**, across 2,208 genuinely consecutive hourly files. Phase 0 could not test trap 4 (its sample used non-adjacent hours) and Phase 1 covered it only by unit test; this is the first measurement at scale. Day-boundary duplicates stay out of scope by design — Silver's grain is a day — so the residual across 91 boundaries is negligible but **unmeasured rather than zero**. `docs/findings/2026-09-02-zero-quarantined.md`

---

## 14. How this reads as value

**To a hiring manager**, the mapping is direct:

| Built | Requisition language satisfied |
|---|---|
| Point-in-time feature store, as-of joins, leakage tests | "production ML pipelines", "feature engineering at scale" |
| MLflow registry, serving endpoint, drift + skew monitoring | "MLOps", "model lifecycle", "deploy and monitor models" |
| Vector index as feature infrastructure | "embeddings", "vector search", "AI infrastructure" |
| PySpark medallion, schema evolution, SCD2, accumulating snapshot | "distributed data processing", "dimensional modeling" |
| Streaming ingest, watermarks, late arrival, exactly-once | "real-time data", "streaming pipelines" |
| Unity Catalog, OpenLineage, contracts in CI | "data governance", "data quality", "lineage" |
| Terraform, CI/CD, cost controls | "infrastructure as code", "platform engineering" |

**The one-liner**, with brackets staying bracketed until measured:

> Built an ML platform over GitHub's public event firehose: **[N]M**
> events across two schema eras, a point-in-time-correct feature store
> serving a review-SLA risk model at **[X]ms** p99, with drift and
> training/serving skew monitoring, column-level lineage, and data
> contracts enforced in CI.

**The honest internal assessment:** the genuine acquisitions here are
point-in-time correctness, training/serving skew, and model serving.
Everything else is reinforcement of capability already demonstrated
elsewhere. Those three are the reason to build this.
