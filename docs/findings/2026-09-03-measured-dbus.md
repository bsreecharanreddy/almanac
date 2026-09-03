# Findings — the one assumed input, measured, and the two costs it understated

**Date:** 2026-09-03
**Source:** `system.billing.usage` and `system.billing.list_prices`, queried
through the Serverless Starter Warehouse (started for this, stopped after).
**What it closes:** §13's **DBUs consumed per node-hour** — the single
assumed input in the cost model since Phase 0.

---

## Getting access was the whole difficulty, and the diagnosis was wrong twice

`system.billing` was never disabled. `system-schemas list` reports every
schema as `MANAGED`, meaning Databricks manages it and it is already on.
The earlier observation that the `system` catalog held only `ai` and
`information_schema` was a **permissions filter on a non-admin identity**,
not an absent schema. "It needs enabling" was wrong; it needed the
account-admin role for visibility.

The role could not be granted because the account console rejected the only
Global Administrator this tenant had:

```
AADSTS500200: User account 'bsreecharanreddy@outlook.com' is a personal
Microsoft account. Personal Microsoft accounts are not supported for this
application unless explicitly invited to an organization.
```

The tenant is the `*.onmicrosoft.com` directory Azure auto-creates at
signup, and its Global Administrator was a personal Microsoft account
present as an `#EXT#` guest. Entra Global Administrator is **not**
Databricks account admin; it is only what lets you claim the latter on
first console sign-in, which an MSA cannot do. Resolved by creating a
cloud-only Entra user with Global Administrator, signing in as that user,
and granting account admin to the original identity — which the CLI then
picked up after about 90 seconds of propagation.

**Worth recording because both wrong turns were confident**: first "the
schema needs enabling", then "sign in with the `#EXT#` UPN" — which is
Entra's internal representation of a guest, not a sign-in name, so no
password could ever have worked.

---

## The measurement

Rates first, from `list_prices`, both current:

| SKU | $/DBU |
|---|---|
| `PREMIUM_JOBS_COMPUTE` | 0.30 |
| `PREMIUM_JOBS_COMPUTE_(PHOTON)` | 0.30 |

**Identical**, confirming `2026-09-01-azure-pricing.md`'s claim that Photon
carries no DBU-*rate* premium. Its cost penalty is entirely in consumption.

Then usage, per job run:

| Run | DBUs | node-hours | DBU / node-hour |
|---|---|---|---|
| Tier 3 backfill `227270110474807` | 27.788 | 25.235 | **1.101** |
| Gold on the quarter `655249155948620` | 1.453 | 1.646 | **0.883** |

**The assumption was 0.75. The backfill measured 1.101 — 1.47x higher.**

Note the two differ (1.101 vs 0.883): DBU consumption is a property of the
workload, not only of the node type. Quoting a single figure across
workload shapes would be the same mistake as quoting Bronze-only
throughput for the full pipeline. The backfill's figure is the one to use
for backfill-shaped work.

## What that corrects

| Run | Reported | Measured | Error |
|---|---|---|---|
| Tier 3 backfill | $11.96 | **$14.62** | +22% |
| Gold on the quarter | $0.78 | **$0.85** | +9% |

Both reported figures used `$0.474/node-hour`, which is VM (`$0.249`) plus
the *assumed* DBU cost (`0.75 x $0.30 = $0.225`). The measured rate makes
it `$0.249 + 1.101 x $0.30 = $0.5793`. Cross-checked against raw DBUs
rather than only through the rate: `25.235 x $0.249 = $6.28` VM plus
`27.788 x $0.30 = $8.34` DBU = **$14.62**.

**§13's own caveat was right**: the Premium-vs-Standard decision held
across the whole sensitivity range, and the *absolute* cost per run did
not. The tiering decision is unaffected — Tier 3 at $14.62 is 7.9% of the
$184 credit against a 40% cap, still an order of magnitude inside it.

---

## The Photon verdict, which this turns from "a wash" into a loss

Measured DBU multiplier, per replicate pair:

| Replicate | standard DBU | photon DBU | multiplier |
|---|---|---|---|
| `693303490917119` | 1.0019 | 2.3851 | 2.381 |
| `325049271562727` | 0.8856 | 2.1202 | 2.394 |
| `236314776904974` | 0.9425 | 1.9959 | 2.118 |
| | | **mean** | **2.297** |

`2026-09-03-photon-ab.md` published a break-even table of 1.55 / 1.96 /
2.16 and called Photon a wash against an assumed ~2x multiplier. **Both
halves of that were wrong, in the same direction.**

The break-even arithmetic used `V = $0.474` as the VM rate, but that figure
is VM **plus assumed DBU** — double-counting DBUs into `V` while also
charging them through `d`. With VM-only (`$0.249`) and the measured DBU
cost (`$0.3304/node-hour`):

| Bronze speedup | Overall | Break-even multiplier (corrected) |
|---|---|---|
| 1.00 (no help) | 1.178x | **1.31** |
| 1.15 (mean) | 1.308x | **1.54** |
| 1.23 (best case) | 1.373x | **1.65** |

**The measured 2.297 exceeds every one of them**, and so does the lowest
individual replicate (2.118). The harness agrees on the real numbers:

```
dbu_overhead 2.297   cost_off $0.40   cost_on $0.74   paid_for_itself False
```

Photon costs **1.85x** as much for a 1.31x overall speedup. The
recommendation does not change — do not enable Photon — but its basis
does: it was "a wash on assumed inputs", it is now **a measured loss with
no plausible input that rescues it**.

`hypothesis_holds` remains `None`. The cost question became answerable and
the Bronze speedup did not; the two claims stayed independent, which is
what the harness rewrite was for.

## What is still not measurable

**Per-layer DBUs.** DBUs bill per cluster-hour and Bronze/Silver/Gold share
one cluster sequentially within a run, so any per-layer figure is the run
total apportioned by wall-clock share — arithmetic over numbers already
published, not a meter reading. Task 9 Step 3's "DBUs consumed per layer"
is satisfiable only in that sense, and §8.2 should say so rather than imply
a meter exists.
