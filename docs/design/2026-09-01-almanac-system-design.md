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
            │ Power BI  │             │ training → MLflow registry →     │
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

---

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

**Baseline first, always.** No model ships without a measured comparison
against a naive baseline. A model that fails to beat its baseline is a
documented finding, not a failure to hide.

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
| BI | Power BI | 3 pages, import mode |
| Local dev | Single Docker container, `pyspark` + `delta-spark`, `local[*]` | **Not** a Spark master/worker Compose cluster — slower at this volume and teaches nothing |

**Explicitly rejected:** Azure Data Factory (orchestration duplicated by
Workflows), Kubernetes (cargo-culting at this scale), multi-cloud,
a custom web frontend.

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

| Phase | Window | Deliverable | Gate |
|---|---|---|---|
| **0 — Exploration** | Wk 1 (Sep 1–7) | Repo, CI skeleton, local Spark container. Download 2h from 2025 + 2h from 2014; **diff the schemas for real**; measure file size, rows/hour, event distribution, bot share, duplicate rate. Terraform-provision Azure early (clusters off) so there is no setup scramble later. | Measured numbers committed; schema diff documented from real data, not assumed |
| **1 — Local pipeline** | Wk 2 (Sep 8–14) | Config-driven runner. Bronze ingest with missing-file handling and `replaceWhere`. Silver with dedup, quality rules, quarantine, dual schema handlers. | Rerun any hour twice → identical content. Null-handling regression test green. |
| **2 — Gold + AZURE BURN** | Wk 3–4 (Sep 15–24) ⚠️ | SCD2 `dim_repo`, `fact_pull_request` accumulating snapshot, `agg_repo_daily`. **Lift to Azure Databricks: Unity Catalog, ADLS, real quarter backfill + 2014 month at volume on job clusters.** Capture evidence — run metrics, UC lineage, cost per run, screenshots. Tear down. | Real backfill completed on Azure; evidence captured; `terraform destroy` leaves nothing |
| **3 — Feature platform** | Wk 5–6 | Point-in-time-correct offline store, as-of joins, feature specs, leakage test suite. | The `as_of` demo works; leakage suite green |
| **4 — Model + MLflow** | Wk 7–8 | Measured baseline, then the SLA-risk model. MLflow tracking + registry. Batch scoring, then a serving endpoint. Drift and training/serving skew monitoring. | Model beats baseline by a measured margin, or the null result is documented |
| **5 — Embeddings + vector index** | Wk 9–10 | Incremental embedding pipeline, ANN index, similarity features, measured downstream lift. | Measured lift, or an honest documented null result |
| **6 — Streaming** | Wk 10–11 | Live Events API ingest, watermarks, late-arrival and exactly-once handling, online feature freshness. | Live events land and update online features |
| **7 — Governance, BI, docs** | Wk 12–13 | OpenLineage, contracts enforced in CI, 3 Power BI pages, ADRs, limitations, decision memo, postmortem. Second bounded (paid) Azure window for the final live demo. | A stranger clones and runs locally in <15 min |
| **8 — Ship** | Wk 13 | Tag `v1.0`. Stop. | — |

**Start applying at Phase 4.** The repo is presentable once a model
serves; the remaining phases improve it while interviews are already in
flight.

---

## 10. Goals — what "done" means

### Technical
- [ ] One quarter + one 2014 month ingested, both schema eras through the same framework
- [ ] Rerunning any single hour produces identical results — idempotency proven, not claimed
- [ ] Adding a source requires only a YAML file, zero new Python
- [ ] `dim_repo` is SCD2 with at least one real demonstrated rename
- [ ] `fact_pull_request` is a working accumulating snapshot
- [ ] **A feature vector computed `as_of` T is reproducible byte-for-byte a year later**
- [ ] **Leakage test suite proves no feature sees post-T data**
- [ ] Model beats a measured baseline, or the null result is documented
- [ ] Model serves from a real endpoint with measured p50/p99
- [ ] Drift and training/serving skew monitored and visible
- [ ] Streaming path handles late arrival and duplicates correctly
- [ ] Quarantine rate reported per rule, per day
- [ ] Data contract enforced as a CI failure, not a markdown file
- [ ] Column-level lineage available end to end
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
| 3 Power BI pages | 4+ pages | Beyond three, page count stops carrying signal and starts costing hours |
| dbt included, scoped to Gold only | No dbt, or dbt through Silver | Market-demanded and cheap at Gold; rewriting Silver in dbt would discard the Spark work that is the point |
| Streaming built in Phase 6 | Deferred to a future project | Most-probed interview topic, and a deferred project may never happen |
| Data contract enforced as a CI test | Contract as a markdown document | A contract nothing enforces is a wish |
| Feature store built, not bought | Managed feature store | The build is the demonstration; a managed store hides the exact skill being shown |
| ~13 weeks | Hard 6-week ship | Quality and structure chosen over speed, deliberately and with eyes open |

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
   functionally necessary, not decorative.
5. **Missing and truncated hours.** Ingestion must distinguish *file
   absent* / *file empty* / *job failed*.
6. **Bots dominate volume.** Any unclassified metric is misleading.
7. **The 2015 schema break.** Structurally different pre-2015 format —
   `repository` instead of `repo`, different actor representation, event
   types that no longer exist. This is the schema-evolution story, and it
   is real. **Field names to be confirmed against real data in Phase 0,
   not assumed.**
8. **Repos get renamed and transferred.** `repo.id` is stable,
   `repo.name` is not. The SCD2 arises naturally.
9. **gzip is not splittable.** Parallelism is bounded by file count, not
   file size.
10. **Language is absent from most events.** Nested in PR payloads only;
    repo-language coverage is partial.
11. **Deleted users and repos** appear as nulls or placeholders in later
    events.

---

## 13. To verify before building — never quote an unmeasured number

- [ ] Exact pre-2015 schema field names, diffed against real files
- [ ] Current file sizes and events/hour, measured
- [ ] Current Azure/Databricks pricing for the chosen region and SKU
- [ ] Docker base image availability for `pyspark` + `delta-spark`
- [ ] Unity Catalog tier requirements and their DBU cost impact
- [ ] Whether `array_compact` exists in the Spark version actually used

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
