# Resume bullets

§10 asks for these and they were the last item recorded as *not done*.
Every figure traces to a findings doc; nothing here is rounded up.

**Rule applied throughout:** claim only what was measured, and prefer the
bullet that survives a follow-up question over the one that sounds
larger.

## Short form — four bullets

- Built an **ML platform for work-queue risk** end to end on 341M real
  events across a schema break: medallion pipeline, point-in-time-correct
  feature store, model, serving endpoint, streaming ingest, and governance
  — solo, on Azure Databricks, for a measured **$11.96** of compute on the
  historical backfill.
- Enforced **point-in-time correctness** as a testable invariant rather
  than a convention — a feature vector computed `as_of` T must be
  byte-identical on recompute from the same Delta version — and **found a
  leakage bug in my own registered model** through it: a random train/test
  split inflated PR-AUC by **24%** (0.612 → 0.4661 once split temporally).
- Shipped a **measured baseline before the model** and published the null
  result when LightGBM lost to a per-segment median by **51%** on the
  regression target; reframed to classification, which beat its baseline
  **1.76×** on a temporal split.
- Took the platform **live and then tore it down**, measuring what most
  projects assume: serving cold start **51.96 s**, warm **p50 263.5 ms**,
  Vector Search idle **$6.72/day** (it does not scale to zero), online
  store idle **$12.06/day**.

## Longer form — with the story behind each

**Point-in-time correctness, and finding my own leakage bug.**
The governing invariant is that no feature may read data at or after its
own timestamp, enforced by a dedicated leakage suite. Writing an honest
limitations document — *not* writing tests — surfaced that the train/test
split was random where the design requires temporal. **The suite was green
throughout and was not wrong**: it tested row time, and the bug was on
split time. Re-scored: **0.612 → 0.4661 PR-AUC**, optimistic by 24%. The
winning hyperparameter configuration changed too, so a random split would
have shipped the wrong *config*, not just an inflated number. The
retracted figure is still published, labelled.
*(`docs/findings/2026-09-08-champion-rescored-temporal-split.md`)*

**Streaming correctness under a real failure.**
A watermark-based dedup silently discarded **distinct late events**, not
just duplicates, losing **161 entities** in a live run. It looked correct
on the first run because a cold checkpoint had not advanced the window. I
wrote the prediction down *before* running the experiment that confirmed
it, then replaced the design with a stateless insert-only merge. Two
independent measurements agreed to the row.

**Cost as an engineering discipline.**
Every cloud resource is provisioned by Terraform and destroyed between
sessions, with the teardown guarded by executable checks rather than a
README. A cross-check against the cloud biller's own API found the
platform's usage table had been understating real spend by nearly half.
Idle rates were measured, not assumed: a "serverless" Vector Search
endpoint drew a flat 4 DBU/hour whether queried or idle.

**Data quality that survives contact with real data.**
341M events across three schema eras, with quality rules that quarantine
by *rule* rather than by boolean, null-safe SCD2 comparison, and
`coalesce(cond, False)` on every rule so a NULL cannot vanish from both
the valid set and the quarantine. Zero events were ever quarantined —
which I report as an explicit zero rather than an empty panel, because
"no data" and "nothing failed" look identical to a reader.

## Numbers, with their sources

| Claim | Value | Source |
|---|---|---|
| Events ingested | 341,060,851 | `2026-09-02-backfill-*` |
| Backfill cost | $11.96 | same |
| PR-AUC, temporal split | 0.4661 (baseline 0.2650) | `2026-09-08-champion-rescored-*` |
| PR-AUC, random split (retracted) | 0.612 | `2026-09-04-classification-*` |
| Serving cold start | 51.96 s | `2026-09-04-model-serving-measured` |
| Serving warm p50 / p95 | 263.5 ms / 376.8 ms | same |
| Live feed capture rate | ~7% | `2026-09-06-events-api-*` |
| Coverage, transform + features | 88%, gated 85% | Phase 7 Task 7 |
| Clone to green run | 4 m 28 s | Phase 7 Task 14 |

**Do not claim:** p99 latency (not measurable at n=20), streaming through
Gold (the live path stops at Silver and the online store), or that the
model works on current data (it reads `is_draft`, which the reduced era
does not carry).
