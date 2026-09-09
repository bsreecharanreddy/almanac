# Drift between the training window and the scoring window

**Date:** 2026-09-09
**Method:** offline and local (design doc §4.8 decision 5 — no cloud, no billable window)
**Reference:** `2025-08-13-09` — 165,601 raw events, the hour §5.1's label
definition was re-probed against
**Current:** `2026-09-05-09` — 74,595 raw events, the window Task 5's
characterizer accepted

Both sides go through the **real Silver path** (`parse_events` →
`normalize_events`) and the **real module** (`almanac.model.drift`). No
fixtures, and no numbers transcribed from a SQL console.

## Null shares

| feature | reference | current |
|---|---|---|
| `event_type` | 0.0% | 0.0% |
| `event_action` | **74.6%** | **0.3%** |
| `pr_draft` | 86.7% | **100.0%** |
| `pr_merged` | 91.4% | 84.2% |
| `schema_era` | 0.0% | 0.0% |

## What the module reported

PSI thresholds are convention, not measurement: moderate ≥ 0.1, major ≥ 0.25.

| feature | kind | PSI | response |
|---|---|---|---|
| `schema_era` | covariate | **27.631** | re-score before trusting the ranking |
| `event_type` | covariate | **10.471** | re-score before trusting the ranking |
| `event_action` | covariate | **3.944** | re-score before trusting the ranking |
| `pr_merged` | covariate | **0.654** | re-score before trusting the ranking |
| `pr_draft` | **schema** | n/a | **refuse to serve** |

**`pr_draft` is the one that changes a decision.** It is null on every row
of the current window and was populated in training, so the module reports
it as *schema* drift rather than computing a PSI over a dead column — a
distribution over a field that no longer exists is noise dressed as a
measurement. Its response is concrete and not a chart: **the champion reads
`is_draft`, so it cannot score this window at all; refuse to serve rather
than re-baseline.**

`schema_era` at 27.6 is the expected sanity check rather than a discovery:
the eras genuinely are disjoint, so a PSI that did *not* explode there
would mean the comparison was not comparing what it claims to.

## The gate asked for three kinds. The module emits two.

§9's exit gate says drift should be reported "across all three kinds —
covariate, schema, semantic". `almanac.model.drift` emits **`covariate` and
`schema` only**; there is no `semantic` kind, and the run above proves it by
labelling everything one of those two.

The semantic change is real and is visible in these numbers — `merged`
moved from a **field** (`payload.pull_request.merged`) to an **action
value** (`action='merged'`), which is why `event_action`'s null share
collapses from 74.6% to 0.3% and why `pr_merged` shifts at all. But the
module labels that `covariate`, because it cannot distinguish *the
distribution moved* from *the meaning moved*. Those are the same signal to
a PSI.

**Stated rather than papered over**: detecting semantic drift automatically
would need a notion of what a field means, which this project does not
have. The honest position is that two kinds are detected mechanically, the
third was found by a human reading the payload, and §10 records the item as
partly done for exactly that reason.

## Reproduce

```
uv run python /tmp/driftreport.py <reference.json.gz> <current.json.gz>
```

The harness is scratch, not shipped: it exists to feed two archive hours
through `silver` and into `summarize`/`compare`. Two harness bugs were hit
and both were the harness, not the pipeline — keeping the source's own `id`
alongside the parsed one (`AMBIGUOUS_REFERENCE`), and omitting Bronze's
`event_date`/`event_hour` partition columns.
