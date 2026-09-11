# The Phase 9 window's cost, read the day after

**Date:** 2026-09-11
**Method:** `system.billing.usage` joined to `system.billing.list_prices`
on `sku_name`, `cloud`, and the price validity window, filtered to
workspace `7405615444091260` and `usage_date = 2026-09-10`. Queried
through the workspace's existing serverless SQL warehouse (`Serverless
Starter Warehouse`, `b5a0f06e0b9a8dde`), started for this read and
stopped immediately after — no new resource created, nothing left
running.

This is the read Phase 9's exit checklist deferred: `system.billing.usage`
lags, so it cannot be queried for the window it measures. `docs/STATUS.md`
named 2026-09-11 as the earliest date it could be read; today is that
date.

## Total spend, 2026-09-10

**$5.59 in Databricks DBUs**, across four SKUs, all inside a single
~3-hour span (12:00–15:00 UTC):

| SKU | DBUs | USD |
|---|---|---|
| `PREMIUM_SERVERLESS_SQL_COMPUTE_US_WEST_3` | 5.8886 | 4.1220 |
| `PREMIUM_JOBS_COMPUTE` | 4.4847 | 1.3454 |
| `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST_3` | 0.2524 | 0.1136 |
| `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_3` | 0.0889 | 0.0062 |
| **Total** | — | **5.5872** |

**This is DBUs only.** Azure-side charges (storage, networking, the VMs
underneath any non-serverless compute) bill separately and are not in
this table, the same qualification the 2026-09-09 read made.

## Attribution, stated honestly rather than assumed

**No other billed activity exists on 2026-09-10 in this workspace.**
Every row for the day falls inside the same 12:00–15:00 UTC span, and
`docs/STATUS.md` records nothing else running against this workspace that
day — Phase 10's nine build tasks are pure offline Python and Spark-local
work, none of it billed. That makes the whole day's $5.59 attributable to
Phase 9's Task 11 window by elimination, not by a job-id-level trace.

**The SQL-compute line is the one worth naming rather than folding in
silently.** At $4.12 it is the largest single component, and it is not
what "five one-time job-cluster runs" would predict on its own — job
clusters bill as `PREMIUM_JOBS_COMPUTE` / `_SERVERLESS_COMPUTE`, not SQL
warehouse compute. It sits inside the same time window as the job-compute
usage and nothing else ran that day, so it is almost certainly the
window's own registry/version lookups or a gate-2 pre-flight check
running through a SQL warehouse rather than a job cluster — but this read
did not trace it to a specific job or query id, and says so rather than
asserting a causal path it did not check.

**`PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_3` ($0.006) is the
Phase 4/8 model-serving endpoint**, left deliberately up and billed by
actual inference, not idle time — consistent with Phase 8's finding that
it scales to zero and only draws DBUs when called.

## What this does and does not settle

**Settled:** the window's real, measured DBU cost is $5.59, not an
unknown. That is the number `docs/STATUS.md`'s "what it cost is not known
yet" line was waiting on, and it is now known.

**Not settled by this read:** the plan's token-level reconciliation for
the foundation-model calls inside the window. That is a different
measurement — model-serving token counts, not workspace DBUs — and it
cannot run at all in this workspace, because `system.billing.usage`'s
`usage_metadata.endpoint_name`-keyed rows for model serving
(`endpoint_usage`) have never had a row here, checked in this same read.
Recorded as **uncloseable in this workspace**, not silently dropped — the
same distinction Phase 6 drew between "measured absent" and "not looked
for."

## Cost of this read

One existing serverless SQL warehouse started, three queries run, stopped
immediately after. At the observed serverless SQL rate this session's own
usage is on the order of a few cents and will appear in tomorrow's
billing day.
