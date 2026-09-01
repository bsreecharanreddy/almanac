# Findings — Azure pricing, and the Unity Catalog decision

**Date:** 2026-09-01
**Region:** `eastus2`
**Source:** Microsoft's **Azure Retail Prices API**
(`https://prices.azure.com/api/retail/prices`) — the authoritative,
machine-readable source. **Deliberately not** a blog, an aggregator, or a
pricing-summary article; design doc §13 requires verifying pricing
directly and every third-party figure checked earlier turned out to be
approximate.

---

## Measured DBU rates

| Meter | Standard | Premium |
|---|---|---|
| **Jobs Compute DBU** | **$0.15 / hr** | **$0.30 / hr** |
| Jobs Compute Photon DBU | $0.15 | $0.30 |
| Jobs Light Compute DBU | $0.07 | $0.22 |
| All-purpose Compute DBU | $0.40 | $0.55 |
| SQL Analytics DBU | $0.22 | $0.22 |
| Serverless Realtime Inferencing DBU | — | **$0.07 / hr** |
| **Launch Charge, Serverless Realtime Inferencing** | — | **$0.07 per launch** |

**Premium Jobs Compute is exactly 2× Standard.**

**Two previously unverified figures are now confirmed** (design doc §8.1
flagged both as community-sourced): the Model Serving DBU rate is
**$0.07/hr** (community said ~$0.08) and the per-launch charge is
**$0.07** exactly. Model Serving with `scale_to_zero_enabled` is therefore
as cheap as claimed.

## Measured VM rates (Linux, on-demand)

| SKU | USD / hr |
|---|---|
| Standard_D4ds_v5 | **0.2260** |
| Standard_D8ds_v5 | 0.4520 |
| Standard_DS3_v2 | 0.2290 |

## Cost of the Tier 3 backfill cluster

Assumed cluster: **1 driver + 3 workers × Standard_D4ds_v5**.

| | VM | DBU | Total | Hours on $184 |
|---|---|---|---|---|
| Standard | $0.904 | $0.450 | **$1.354 / hr** | **135.9** |
| **Premium (Unity Catalog)** | $0.904 | $0.900 | **$1.804 / hr** | **102.0** |

Premium costs **+$0.45/hr — 33% more — or 34 fewer cluster-hours.**

⚠️ **One input is assumed, not measured:** DBU consumed *per node*
(0.75 DBU/hr for a D4ds_v5). The retail API publishes $/DBU but not
DBU-per-instance; that mapping lives in Databricks' own calculator.
Sensitivity:

| DBU/node | Standard hours | Premium hours |
|---|---|---|
| 0.5 | 152.8 | 122.3 |
| **0.75** | **135.9** | **102.0** |
| 1.0 | 122.3 | 87.5 |
| 1.5 | 102.0 | 68.0 |

**Confirm the real figure at the first cluster launch** and correct this
document. The decision below holds across the whole range, so it does not
block.

---

## Decision: **Unity Catalog. Premium tier.**

Design doc §11 left this open pending measured pricing. It is now closed.

**Reasoning.** Tier 3 is one unsampled month — 720 gzipped files, ~62 GB
compressed, ~444 GB uncompressed. gzip is not splittable, so parallelism
is bounded by file count rather than size: 12 cores process 12 files at a
time, ~60 waves. A full Bronze→Silver→Gold build plausibly runs 3–5
hours, and **10–15 hours covers the run plus real iteration**.

**Premium leaves 102 cluster-hours — 68 even on the most pessimistic DBU
assumption.** Against a 10–15 hour need, the budget is nowhere near
binding, so trading governance away to buy compute we would not use is a
bad trade.

Unity Catalog also carries the lineage story (§design goals) and is a
genuine resume line. It costs 34 cluster-hours we have no use for.

**What would reverse this:** if measured throughput shows the backfill
needs >60 cluster-hours, revisit — either drop to Standard, or shrink
Tier 3 to two weeks as §4.5 already permits. Measure at the first real
run before assuming.

## Cost controls in force

- **Job clusters only.** All-purpose Compute is $0.40–0.55/DBU against
  Jobs' $0.15–0.30, and an all-purpose cluster left on a schedule is the
  single most common source of a surprise portfolio bill.
- Auto-termination on anything interactive.
- `terraform destroy` between sessions.
- Budget alert configured **before** the first apply.
- All resources tagged `project` / `env` / `owner`.
