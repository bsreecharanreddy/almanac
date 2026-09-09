# Training/serving skew, measured

**Date:** 2026-09-08
**Stack:** `infra/terraform-lakebase/` — centralus, workspace `7405613359901764`
**Job run:** `914815176253745` (four stages, SUCCESS, 1,098 s)
**Lakebase:** `almanac-lb-online-store`, `CU_1`, PG 16, `AVAILABLE`

## Why this needed a second region at all

Lakebase is not available in `westus3`, where the main workspace lives, and a
Lakebase project inherits its workspace's region and cannot be moved (Phase 6
Task 9, 2026-09-06). The main module still carried an unappliable
`databricks_database_instance`, and a targeted apply of it hung for **44
minutes** on 2026-09-08 before that was rediscovered — the resource is now
marked, with its symptom, in `infra/terraform/streaming.tf`.

**Proven by experiment, not by a status page**: the identical resource
reached `AVAILABLE` in centralus with the same config, subscription and
credentials. One variable changed.

## Method

The offline table is what the feature job wrote to Delta; the online table is
what Lakebase Postgres actually serves. Both were read **independently** —
the offline side over SQL, the online side over a direct `psycopg` connection
to the Postgres endpoint, the way a serving application would reach it, not
through the same client that wrote it.

Compared on all four feature columns: `events_prior_1h`, `events_prior_24h`,
`secs_since_last_event`, `arrival_per_hour_24h`.

## Result

| | `repo_stream_activity` | `actor_stream_activity` |
|---|---|---|
| offline rows | 2,228 | 2,078 |
| offline distinct keys | 1,916 | 1,543 |
| online rows | **1,916** | **1,543** |
| keys served but absent offline | 0 | 0 |
| keys offline but never served | 0 | 0 |
| keys compared | 1,916 | 1,543 |
| **rows agreeing on every feature** | **1,916 (100.00%)** | **1,543 (100.00%)** |
| per-feature mismatches | none | none |

**The row-count gap is not loss.** `online rows == offline distinct keys`
holds exactly for both tables, and both set differences are empty: the online
store keeps one row per key, the offline table is the full timeseries. Stated
because a naive row-count comparison would have reported "312 repos missing"
and been wrong.

## Two things this does not establish

- **The design makes agreement likely.** The online store is populated by
  publishing the very table the offline side reads, so this measures that the
  publish path is faithful — it does not exercise a re-implementation of the
  feature logic in a serving language, which is where skew usually comes from.
  A zero here is a real check, not a hard one.
- **`n` is one window.** 10 polls (~10 minutes), not Phase 6's 30 — the
  deviation is deliberate, since skew needs entities rather than firehose
  volume, and it is stated so no reader assumes `n=30`.

## One measurement bug, caught

The SQL API renders NULL as an empty string while Postgres returns a real
`None`. Comparing them directly counts every NULL as a mismatch and would
have manufactured skew that does not exist. Both are normalised to `None`
before comparison.
