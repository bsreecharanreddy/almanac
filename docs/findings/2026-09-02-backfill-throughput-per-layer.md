# Findings — throughput per layer, and the Bronze-only risk resolved

**Date:** 2026-09-02
**Run:** Databricks job run `227270110474807` (`databricks_job.backfill`),
workspace `almanac-dbx`, `westus3`. Q3 2025, 92 of 92 days, 2,208 hourly
files, zero missing hours.
**Method:** the run's own printed summary (`BackfillReport.summary`) for
per-layer wall time, and the Jobs API's `setup_duration` /
`execution_duration` for billed cluster time. Nothing here is re-derived
from a sample — these are the whole-quarter totals.

This closes the deliverable design doc §4.5 and `docs/STATUS.md` both name:
**publish GB per cluster-hour, per layer.**

---

## What was measured

```
days_processed        92 of 92        hours_missing      []
rows_bronze           341,060,851     rows_clean         341,060,789
rows_quarantined      0               compressed_gb      165.987
fetch_seconds         1,292.9         bronze_seconds     11,282.5
silver_seconds        4,994.5
setup_duration        471 s           execution_duration 17,698 s
```

Billed cluster time is setup + execution = **18,169 s = 5.047 h**, on
1 driver + 4 workers of `D4ds_v6` at **$2.370/cluster-hour** — so
**$11.96**, against a $31.71 estimate and a $184 credit.

## Per layer

The layers run sequentially on one cluster, so a layer's wall-clock hour
*is* a cluster-hour spent in that layer. GB is compressed input GB in
every row — the constant the layers are being compared against, not each
layer's own output size.

| Layer | Wall clock | Share of execution | GB gz / cluster-hour |
|---|---|---|---|
| Fetch | 1,292.9 s | 7.3% | **462.2** |
| Bronze | 11,282.5 s | 63.8% | **52.96** |
| Silver | 4,994.5 s | 28.2% | **119.64** |
| Unattributed | 128.1 s | 0.7% | — |
| Cluster setup | 471 s | (outside execution) | — |
| **Whole run** | **18,169 s** | — | **32.89** |

The unattributed 128 s is checkpoint writes, staging cleanup and session
construction — 0.7%, small enough not to need chasing.

**The accounting closes.** Per-layer rates compose as reciprocals, not as
an average: 1/462.2 + 1/52.96 + 1/119.64 = 0.02940 h/GB → 34.01 GB/h,
against an execution-only actual of 165.987 / 4.916 = 33.76 GB/h. The
0.25 GB/h gap is exactly the unattributed 128 s. Three independently
measured stage timers reproducing the run's total is the check that the
per-layer numbers are real and not double-counted.

---

## The Bronze-only risk, resolved — in the opposite direction

Phase 1's calibration measured **13.84 GB gz per billed cluster-hour**,
and `docs/STATUS.md` carried the caveat forward explicitly: *"The measured
throughput is Bronze-only. The 2.3x headroom is the margin, and if the
full pipeline is worse than that, Tier 3 shrinks — which is the rule
working, not a failure."*

The full pipeline is not worse. It is **2.4x better**, while doing
strictly more work (Bronze *and* Silver, where the calibration was Bronze
alone):

| Basis | Calibration (Bronze only) | Backfill (Bronze + Silver) | Ratio |
|---|---|---|---|
| Billed (setup + execution) | 13.84 GB/h | 32.89 GB/h | **2.38x** |
| Execution only | 13.84 GB/h | 33.76 GB/h | **2.44x** |

Both bases are given on purpose. The calibration's recorded 523.5 s of
"billed cluster time" is smaller than this run's 471 s of setup *alone*,
which means the two figures may not include cluster provisioning the same
way — so rather than pick one and inherit the ambiguity, the comparison is
shown on each basis. It is 2.4x either way, and the conclusion does not
depend on the choice.

**Where the gain came from.** Two causes, both present, decomposition not
measured:

1. **Fetch parallelism.** The calibration spent 174.3 s of 523.5 s — a
   third of billed time — single-threaded on HTTP download. With
   `fetch_concurrency: 8` fetch is 7.3% of execution here.
2. **Setup amortization.** One 471 s cluster provisioning is spread over
   92 days instead of 1.

Tier 3 was sized at 17.2% of the credit against a 40% cap. It landed at
**6.5%**.

---

## What this does not say

**52.96 GB/cluster-hour for Bronze is not a compute limit.** Bronze reads
one non-splittable `.json.gz` per iteration, so it runs one task on one
core while the other workers idle — measured share, diagnosed cause, in
`2026-09-02-bronze-is-single-threaded.md`. The Bronze row above is what
*this* implementation achieves on this cluster, not what the layer costs.
A splittable-input Bronze would move it, and the whole-run 32.89 with it.

**The rates are for one cluster shape**, 1 + 4 `D4ds_v6` in `westus3`.
Nothing here measures how any layer scales with worker count — and for
Bronze specifically, the single-threading finding predicts it would not
scale at all.

**One assumed input remains**, unchanged since Phase 0's pricing work:
DBU-per-node-hour. The $2.370/cluster-hour rate inherits it, so $11.96
inherits it too. The sensitivity table in
`2026-09-01-azure-pricing.md` still governs, and the decision held across
its whole range.
