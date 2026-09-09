# The final billing read, and an idle rate that is now zero

**Date:** 2026-09-09
**Method:** `system.billing.usage` joined to `system.billing.list_prices`
on `sku_name` and the price validity window, queried through the
serverless SQL warehouse, which was started for this read and stopped
immediately after.

This is the read Phase 8's exit checklist deferred: `system.billing.usage`
lags, so it cannot be queried during the window it measures. The lag has
passed.

## Total spend

**$101.79 in Databricks DBUs across 8 billed days** (2026-09-02 →
2026-09-09).

| Day | USD | What was happening |
|---|---|---|
| 2026-09-02 | 7.93 | Tier 3 backfill — the 341M-row burn |
| 2026-09-03 | 8.25 | Photon A/B, feature platform |
| 2026-09-04 | 8.11 | Model training, first serving |
| 2026-09-05 | **25.46** | Phase 5 embeddings — 1.86M vectors, the peak |
| 2026-09-06 | 20.78 | Vector index, streaming, Lakebase |
| 2026-09-07 | 22.00 | Phase 7 dashboards + the centralus stack |
| 2026-09-08 | 5.31 | Phase 8 demo window |
| 2026-09-09 | 3.96 | Pre-teardown hours only |

This is **DBUs only**. Azure-side charges (storage, networking, the VMs
underlying non-serverless job compute) bill separately and are not in this
table — the $11.96 backfill figure quoted elsewhere is a different,
Azure-side measurement and the two do not sum.

## The idle rate is now zero, which corrects an earlier number

Phase 6 measured an idle rate of **$12.06/day** and that figure has been
quoted since. It was correct *at the time* — it was measured with the
Lakebase instance up. It is **no longer the idle rate**, because the thing
generating it is gone.

Last usage recorded, per SKU, against a read taken at **15:16 UTC**:

| SKU | Last usage | Age at read |
|---|---|---|
| `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST_3` | 09-09 07:00Z | 8 h |
| `PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST_3` | 09-09 04:00Z | 11 h |
| `PREMIUM_JOBS_COMPUTE` | 09-09 03:00Z | 12 h |
| every `*_US_CENTRAL` SKU | 09-09 03:00Z | 12 h |
| `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_3` | 09-08 23:48Z | 15 h |

**Nothing has drawn a DBU in over eight hours.** The measured idle rate is
**$0.00/day**.

The serving endpoint is the one worth calling out: it is still `READY` and
deliberately left up, and it has drawn nothing since 09-08 23:48Z. That is
the difference between *believing* `scale_to_zero` works and *measuring*
it — the endpoint reports `"Scaled to zero"` and the billing table agrees.

## The centralus teardown, confirmed a third way

Teardown was previously verified through terraform state and the resource
listing. Billing is an independent third source, and it agrees: **every
`US_CENTRAL` SKU stops at exactly `2026-09-09T03:00:00Z`** and shows
nothing in the twelve hours since — including
`PREMIUM_DATABASE_SERVERLESS_COMPUTE_US_CENTRAL`, the Lakebase instance
itself.

The $0.91 + $0.20 + $0.08 of centralus charges dated 09-09 are the hours
between midnight UTC and that teardown, not a live resource. A reader
looking only at the daily total would reasonably suspect otherwise, which
is why the per-SKU `max(usage_end_time)` is the column that settles it.

## One observation about the lag itself

Records timestamped 07:00Z were readable at 15:16Z, so the lag on this
read was **at most ~8 h**, against the ~24 h this project has assumed and
written down. **n = 1** — a single read on a single day, which is not
enough to revise the standing assumption. Recorded because it is cheap to
note and would take several more reads to establish, not because it
changes the rule.

## Cost of this read

One serverless SQL warehouse start, four queries, stopped immediately
after. It will appear in a later billing day; at the observed serverless
SQL rate (~$0.70/DBU) a few minutes is cents. The reason this was deferred
rather than done automatically is that starting a warehouse to fill in a
number is spending money to tidy a document, and that is a decision for a
person rather than a default.
