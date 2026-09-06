# Findings — Events API capture, real cloud spend, and the online-store rate

**Date:** 2026-09-06
**Task:** Phase 6 Task 1 (`docs/plans/2026-09-06-phase-6-streaming-plan.md`)
**Why this exists:** design doc §4.6 recorded two probes at `n=1` and
explicitly refused to treat them as design premises. This widens them, and
one of the two moved materially. It also measures the money, which turned
out to be the more consequential half.

---

## 1. The live Events API captures ~7% of the stream

**Method.** Authenticated polling of `https://api.github.com/events`,
all pages exposed by the `Link` header (3), at the server's own
advertised `x-poll-interval` (60s), for **30 consecutive polls**. Each
poll records the set of event ids and compares it to the previous poll's
set.

```sh
GH_TOKEN=$(gh auth token) uv run python scripts/measure_events_api.py 30
```

The script is committed rather than left ad hoc, so this finding can be
re-run and checked. Re-running it after the fact reproduced the headline
numbers independently (n=2: 7.1–7.5% capture, zero overlap, 306s lag).

**Result, n = 30 polls over ~50 minutes:**

| Metric | Value |
|---|---|
| Events per window (unique across 3 pages) | 169–233, mean **192.2** |
| **Overlap with the previous poll** | **0 — on all 30 polls, without exception** |
| New ids per poll | identical to events per poll, every time |
| Observed throughput | 3.20 events/s = **~11,500/hour** |
| Feed lag (now − newest `created_at`) | **304–307s**, mean 305 |

**The overlap result is the finding.** Not one event id survived from any
poll to the next. The retrievable window turns over completely inside 60
seconds, so everything produced between two polls is unrecoverable — the
API exposes a *sample*, not a firehose.

**Capture fraction: ~7.1–7.4%.** Observed ~11,500/hour against §12's
archive-measured 2026 volume of ~155–162K/hour. This **corrects §4.6's
~11%**, which came from the single-poll probe.

### What this measurement cannot do, stated plainly

- **It cannot count what was missed.** You cannot observe what you never
  received. The capture fraction is a ratio of two *independently
  sourced* rates — polled here, archive-measured in §12 — so it inherits
  the archive figure's own uncertainty rather than being a direct count.
- **A window-span metric was tried, found unusable, and removed.** The
  first version of the script computed a `created_at` span per window,
  intending to derive capture directly as span ÷ poll interval — no
  second data source needed. It does not work: the window carries a
  minority of genuinely old events (median age 302s, but **max 194,271s ≈
  2.2 days**), so min−max spans read 56 hours, and even a p5–p95 robust
  span still blew out to a 187,444s median. That run's `CAPTURE FRACTION`
  and `implied arrival` summary lines were computed from it and are
  wrong; they are not reported here, and **the committed script omits the
  metric entirely** rather than shipping a number that cannot be trusted.
  The overlap-plus-throughput derivation above needs no span.
- **One stall.** The run contains a 1,051-second gap between polls
  against a 60s target (mean 162s). Most likely the machine suspending —
  the same class of interference that disrupted the Phase 4 cold-start
  measurement. It affects wall-clock cadence, not the per-poll
  comparison, since each poll is compared to its own predecessor.

### The rate limit is not the constraint — the politeness interval is

Authenticated budget is 5,000 requests/hour. At 3 requests per poll and
one poll per minute, the poller uses **~180/hour — under 4% of budget**;
observed `x-ratelimit-remaining` never fell below ~4,960.

So the ~7% ceiling is imposed by **GitHub's requested `x-poll-interval`
of 60s, a courtesy, not a technical limit.** Polling every ~2.5s would
stay inside the rate limit and capture substantially more.

**Decision: honour the advertised interval.** A portfolio project that
demonstrates ignoring a documented politeness header is demonstrating the
wrong thing, and §11's replay harness already covers the completeness
proof that the live feed cannot give. Recorded as a decision rather than
left implicit, because it is the direct cause of the 7% number.

### A conditional request never returned 304

The plan asked whether a `304 Not Modified` is exempt from the rate
limit. **The question could not be reached: 5 back-to-back attempts with
a fresh `ETag` all returned `200`.** The feed changes faster than two
consecutive requests, so the `ETag` is stale by the time it is sent.

This is more useful than the exemption answer would have been:
**conditional requests are not a poll-budget optimisation here.** Task 2
should still send `If-None-Match` (correct client behaviour, and it costs
nothing) but must not size its budget on 304s.

### Consequence for Phase 6's freshness gate

The **305-second feed lag is a floor on end-to-end freshness that no
pipeline work can beat.** Task 9 measures freshness as event `created_at`
→ online-store readable; ~5 minutes of that is GitHub's, before Almanac
sees the event at all. The measured number must be reported decomposed,
or it will read as pipeline latency when it is mostly upstream delay.

---

## 2. Databricks DBUs are only 53% of real spend

**This is the correction with the widest blast radius**, because every
cost figure in this repo to date is a DBU figure.

**Method.** Azure Cost Management API (`ActualCost`, daily granularity,
grouped by `ServiceName`), cross-checked against
`system.billing.usage` ⋈ `system.billing.list_prices` on the workspace.

```sh
az rest --method post \
  --url "https://management.azure.com/subscriptions/<sub>/providers/Microsoft.CostManagement/query?api-version=2023-03-01" \
  --body @cm.json
```

**Total Azure spend, 2026-09-01 → 09-06:**

| Service | Cost | Share |
|---|---|---|
| Azure Databricks (DBUs) | $65.19 | 53% |
| Virtual Machines | $30.65 | 25% |
| NAT Gateway | $14.97 | 12% |
| Storage | $11.05 | 9% |
| Virtual Network | $0.54 | <1% |
| **Total** | **$122.40** | |

The Databricks-side query agreed independently: **$66.31** of DBU spend
over 09-02 → 09-06, with only `GENIE_FREE_USAGE` (0.076 DBU) missing a
price row — immaterial. Two methods, two sources, consistent.

**So remaining credit was ~$61.60 of $184, not the ~$118 a DBU-only
reading implies.**

### Daily, and a correction to my own first estimate

| Day | Databricks | VMs | NAT | Storage | Total |
|---|---|---|---|---|---|
| 09-02 | 7.93 | 7.14 | 5.21 | 2.16 | 22.56 |
| 09-03 | 8.25 | 5.53 | 6.33 | 3.27 | 23.49 |
| 09-04 | 8.11 | 3.23 | 1.12 | 1.65 | 14.24 |
| 09-05 | 25.46 | 12.84 | 1.72 | 3.12 | 43.26 |
| 09-06\* | 15.44 | 1.84 | 0.52 | 0.86 | 18.71 |

\* partial day (~62% elapsed at time of query).

**A first pass called NAT + Storage + VNet a "standing cost" and averaged
it at $4.43/day. That was wrong.** NAT Gateway bills per-GB processed and
Storage bills per-transaction, so both track activity rather than sitting
flat — visible in the table as NAT swinging $6.33 → $0.52 across days.
Scaling the quietest (partial) day gives a true idle rate of
**~$2.30/day**, not $4.43. The overstatement would have made Phase 6 look
unaffordable when it is merely constrained.

---

## 3. The Lakebase rate is not in Databricks' price catalog

`system.billing.list_prices` returns **zero rows** for any SKU matching
`LAKEBASE`, `POSTGRES`, `ONLINE` or `OLTP`. This is the **second**
occurrence of the same gap — Phase 5 found the same for `VECTOR`/`SEARCH`
(`2026-09-04-phase-5-corpus-and-index-choice.md`) — so it is a pattern,
not a one-off: newer serverless Databricks products are not resolvable
from the SKU catalog before they are provisioned.

Azure's first-party Retail Prices API has no product named "Lakebase"
either. It does list, for `westus3`:

| Product | Rate |
|---|---|
| Premium **Database** Serverless Compute | **$0.26 / DBU-hour** |
| Premium Serverless Realtime Inferencing | $0.07 / DBU-hour |

**"Premium Database Serverless Compute" is the probable Lakebase meter** —
Lakebase is Databricks' managed Postgres — but this is an **inference from
naming, not a confirmed mapping**, and it is labelled as such. Phase 5's
precedent is exactly on point: Vector Search's real SKU
(`PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_3`) was only confirmed by
provisioning it and reading `system.billing.usage`. Task 9 confirms this
one the same way.

### The retail catalog cross-validates the Phase 5 measurement exactly

Vector Search measured **4.000 DBU/hour** idle and **$6.72/day**
(`2026-09-06-vector-search-live-state-and-teardown.md`). Against the
catalog: 4 DBU/h × 24 h × $0.07 = **$6.72/day**. Exact, to the cent.

That agreement is worth more than either number alone: it confirms the
unit is per-DBU-hour and that the Retail Prices API is the right source
for rates the Databricks catalog omits. On the same basis, a Lakebase
`CU_1` drawing ~1 DBU/hour would cost **~$6.24/day**; if it behaves like
Vector Search at 4 DBU/hour, **~$25/day**. Task 9 measures which.

---

## What this changes

1. **§4.6's ~11% capture becomes ~7.1–7.4%**, at `n=30` instead of `n=1`.
2. **Freshness has a 305-second upstream floor**, which Task 9 must report
   decomposed rather than as a single pipeline number.
3. **Conditional requests are not a budget optimisation** — Task 2 sends
   `If-None-Match` for correctness but sizes its budget without 304s.
4. **The 7% ceiling is a policy choice, now explicit** — honour
   `x-poll-interval`; the replay harness carries completeness.
5. **Every prior cost figure in this repo understates real spend by ~47%.**
   Future cost claims state whether they are DBU-only or all-in.
6. **The online store's rate is unconfirmed until provisioned**, with a
   measured range of ~$6–25/day and a method for settling it.
