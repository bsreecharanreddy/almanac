# Findings — cluster throughput, and Tier 3's derived span

**Date:** 2026-09-01
**Run:** Databricks job run `404509887846902`, workspace `almanac-dbx`,
`westus3`. Cluster `0902-001904-h7cpqwhs`.
**Method:** `scripts/calibrate.py`, one full day of GH Archive
(**2025-08-13**, 24 hourly files) through **Bronze only**, on a job cluster
of **4 `Standard_D4ds_v6` workers plus a driver — 5 VMs**, DBR **17.3 LTS
(Spark 4.0.0)**, Premium, **no Photon** (the Photon A/B is deliberately
Phase 2, so both arms run under identical conditions).

This closes the last unmeasured input in design doc §4.5 and the open
item in §13. Every figure below is measured or derived from measured
figures; where something is assumed rather than measured, it says so.

---

## What was measured

| Quantity | Value |
|---|---|
| Hours expected / present | **24 / 24** — no gaps |
| Rows | **3,794,323** |
| Compressed volume | **2.012 GB gz** |
| Fetch time (network) | **174.3 s** |
| Spark time (compute) | **197.3 s** |
| Cluster setup | **111.0 s** |
| Billed cluster wall time | **523.5 s = 0.14542 h** |
| Rows per Spark-second | **19,230** |

Phase 0 estimated ~2.1 GB/day from an 86 MB/hour mean. The measured day is
**2.012 GB**, so that estimate was good to about 4%.

## Three throughput numbers, not one

The plan asked for a single wall-clock figure. That number cannot
distinguish a slow cluster from a slow network, and the cost model is
priced per cluster-hour, so the script times fetch and Spark separately.

| Rate | GB-gz per hour | What it measures |
|---|---|---|
| Spark compute | **36.72** | What the cluster does with the data |
| Wall clock | **19.50** | Compute plus sequential download |
| **Billed cluster time** | **13.84** | **Compute, download, and cluster setup — what you pay for** |

**The cost model uses 13.84.** A naive single-timer measurement would have
reported 19.50 and understated the bill by a third; quoting the Spark rate
alone would have understated it by 62%.

**174.3 s of the 523.5 s billed — a third of the bill — was
single-threaded HTTP download, not compute.** Parallelising the fetch is
the obvious cost lever for the Phase 2 backfill. It is noted here and
deliberately not built now: this run's purpose is to measure the pipeline
that exists, not a faster one that doesn't.

## Cost

Rates queried live from the **Azure Retail Prices API** for `westus3` on
the day of the run, the same authoritative source as
`2026-09-01-azure-pricing.md`:

| Component | Rate |
|---|---|
| `Standard_D4ds_v6`, Linux, on-demand | **$0.249 / node-hour** |
| Premium Jobs Compute DBU | **$0.30 / DBU-hour** |
| DBU per node-hour | **0.75** — *assumed, see below* |

Per node: `0.249 + 0.75 x 0.30 = $0.474/hr`.

### A 25% correction to the recorded cluster cost

Design doc §4.5 records the cluster cost as **$1.896/hr (4 x D4ds_v6,
Premium)**. The component rates above reproduce that figure *exactly* at
four nodes — which is a useful cross-check, and it is the reason the 0.75
DBU/node-hour figure is trusted here despite not being independently
queryable from the Azure API.

But a Databricks cluster with `num_workers: 4` provisions **five** VMs:
four workers and a driver. The recorded rate counted the workers only.

```
recorded : 4 x $0.474 = $1.896/hr
actual   : 5 x $0.474 = $2.370/hr   (+25%)
```

Everything downstream of the recorded rate was therefore 25% optimistic.
Corrected here, and §4.5 updated.

### Cost of the calibration itself

```
day-of-data = 0.14542 billed cluster-hours x $2.370/hr = $0.3446
```

**$0.34 per day of GH Archive, through Bronze, on this cluster.**

## Tier 3's span, derived

§4.5 fixes the rule in advance: *the largest contiguous slice costing no
more than 40% of the remaining credit.* Remaining credit is **$184**
(§9, expiring 2026-09-24).

```
budget for Tier 3   = 0.40 x $184            = $73.60
$ per day-of-data   =                          $0.3446
affordable days     = $73.60 / $0.3446       = 214 days
Q3 2025             =                          92 days
cost of full Q3     = 92 x $0.3446           = $31.71  (17.2% of credit)
```

**Tier 3 becomes the full Q3 2025 quarter — 2025-07-01 to 2025-09-30, 92
days, ~185 GB gz — not the one month originally written.** It costs 17.2%
of the credit against a 40% cap, leaving **2.3x headroom**.

Cross-checked the other way: 92 days x 0.14542 h = **13.4 cluster-hours**.
At the corrected $2.370/hr, $184 buys 77.6 cluster-hours, so 13.4 is 17.2%
of the credit — the two routes agree. (§4.5's "97 cluster-hours on $184"
was computed at the understated four-node rate; at five nodes it is 77.6.)

Volume cross-check: 92 x 2.012 GB = **185 GB gz**, against §4.5's
independent "on the order of ~190 GB compressed" for a full quarter.

§4.5 states the decision rule binds in both directions and that shrinking
is not a failure. It grew.

## What would change this answer

1. **This is Bronze only.** Task 7's plan specifies Bronze — ingest is the
   volume-bound stage — while §4.5 step 1 says bronze → silver → gold. A
   full-medallion backfill will be slower, so the derived span is
   optimistic by whatever that factor is. **The 2.3x headroom is the
   margin:** the full quarter still fits even if the complete pipeline
   costs twice what Bronze alone does. If it turns out worse than 2.3x,
   the span shrinks and that is the rule working.
2. **One day, on one date.** 2025-08-13 is a Wednesday; weekend volume
   differs. n=1 along the day-of-week dimension, stated plainly because
   this project has twice shipped a wrong conclusion from a small sample.
   The 4% agreement with Phase 0's independent mean is the reason to
   treat it as representative anyway.
3. **The dollar figures are list-price arithmetic, not a billed
   invoice.** Azure consumption data lags 24-48 hours and returned nothing
   for this subscription at the time of writing. The rates are live from
   the retail API; only DBU-per-node-hour is assumed, and it is corroborated
   by reproducing §4.5's recorded $1.896/hr exactly.
4. **Runtime drift.** Local development is Spark 4.2.0 / Delta 4.4.0; the
   cluster is DBR 17.3 LTS (Spark 4.0.0), the newest LTS. A non-LTS 18.2
   (Spark 4.1.0) exists; LTS was chosen because the number is meant to be
   production-representative.

## What the run cost in total

Five job runs, four of which failed before producing a measurement:

| Run | Cluster time | Outcome |
|---|---|---|
| `405484988959058` | 481.7 s | `RESOURCE_NOT_FOUND` — UC volume not readable by the job launcher |
| `963841318100102` | 150.4 s | `RESOURCE_NOT_FOUND` — same, after adding volume grants |
| `1001037445575583` | 180.1 s | `RUN_EXECUTION_ERROR` — public DBFS root disabled |
| `653007078353887` | 233.7 s | `RUN_EXECUTION_ERROR` — Delta schema mismatch between hours |
| `404509887846902` | **523.5 s** | **SUCCESS** |

**Total 1,569.4 s = 0.436 cluster-hours = $1.03.** Teardown verified: `0`
active clusters after the run; all five job clusters auto-terminated.

Three of those four failures were environment facts worth writing down:
the job launcher does not read `python_file` from a Unity Catalog volume
even with `READ VOLUME` granted (workspace files work), **public DBFS root
is disabled on this workspace**, and `spark.read.json` infers a different
schema for different hours of the same day — which is a data finding, not
a configuration one, and is recorded in `docs/STATUS.md`.
