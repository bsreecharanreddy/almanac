# Findings — dataset measurements

**Date:** 2026-09-01
**Sample:** 24 hourly files — 3 non-adjacent Wednesdays in the chosen Q3
window (2025-07-09, 2025-08-13, 2025-09-17) × 8 hours each
(0,3,6,9,12,15,18,21). All 24 fetched `ok`.
**Volume parsed:** 3,798,852 events.

> **Re-measured against Q3.** An earlier pass sampled Q1 2025
> (2025-01-08 / 02-12 / 03-19, 6,002,410 events, 5,757 renames, 29.0% bot
> share). The window moved to Q3 after the October 2025 payload reduction
> was discovered, so these numbers were retaken against the window
> actually being used rather than quoted from a different quarter. Both
> passes agree on every conclusion.

Three days rather than one because a rename is only visible as the same
`repo_id` carrying different names at different times — a single day
cannot show one at all.

> **On hour sampling.** Design doc §4.5 forbids sampling hours for
> *training data*, because a skipped hour can drop a PR's review event and
> fabricate an SLA breach. That ban does not apply here: this is a
> diagnostic probe of repo-name observations over time, not a label. The
> distinction matters and has one real consequence, noted below.

---

## The gate: repo renames — **PASSED, by 115×**

| | |
|---|---|
| Distinct repos observed | 1,071,901 |
| **Renames detected** | **3,792** |
| Gate to proceed | ≥ 50 |
| (Q1 pass, for comparison) | 5,757 across 1,234,736 repos |

**`dim_repo`'s SCD Type 2 is decisively supported. The candidate window
does not need to move.** This was the only remaining item in Phase 0 that
could have forced a design change, and it is closed.

The renames are *real and varied*, not an artefact — ownership transfers
(`jackjackbits/bitchat` → `permissionlesstech/bitchat`), account renames
(`tszhong0411/tszhong0411` → `nelsonlaidev/nelsonlaidev`), project pivots
(`5dlabs/agent-platform` → `5dlabs/cto`), separator changes
(`agent-plugins-platform-boilerplate` → `agent_plugins_platform`), and
case-only changes (`Lumacaonta/GLB` → `Lumacaonta/glb`).

**That last class matters for implementation.** A case-only rename is
still a rename, and any SCD2 comparison done case-insensitively — or any
join through a lower-cased name — would silently miss it. Repo `398243293`
also appears renamed in *both* the Q1 and Q3 passes
(`Vibes-INS/Vibes-INS` → `JaydenXu-BTC/Vibes-INS` → `JaydenV8/Vibes-INS`),
so multi-version SCD2 history is exercised across the sample.

## Duplicate rate — measured, but **this sample cannot test trap 4**

| | |
|---|---|
| Events | 3,798,852 |
| Distinct `event_id` | 3,798,850 |
| Duplicate ratio | 0.000001 (exactly 2 duplicates) |

**Do not read this as "duplicates are rare."** Design doc §12 trap 4
concerns duplicates *across hour-file boundaries*, and this sample took
hours 0, 3, 6… — deliberately non-adjacent. **No two consecutive hours
were compared, so the boundary condition was never exercised.** The
measurement is honest about what it covers: within-sample duplication is
essentially nil, and the cross-boundary rate remains **unmeasured**.

Testing trap 4 properly needs consecutive hours and is deferred to Phase
1, where the dedup logic is actually built. The dedup requirement stands
regardless — it is cheap, and correctness here does not depend on the
rate being high.

## Bot share

Volume-weighted, across all 6.0M events:

| Clause | Events | Share |
|---|---|---|
| `[bot]` suffix | 972,685 | **25.6%** |
| regex heuristic | 40,766 | 1.1% |
| not a bot | 2,785,401 | 73.3% |

**25.6% by the authoritative suffix rule alone** — against 29.0% on the
Q1 Wednesdays and 18.2% on a single Q1 Saturday hour. Both comparisons
hold the same direction: weekdays carry a materially higher bot share
than weekends, because scheduled automation runs on weekday cadences
while humans do not. **Bot share is strongly day-of-week dependent**,
which reinforces §5.1's requirement that temporal splits fall on
whole-week boundaries.

See `2026-09-01-bot-classification.md` for the regex clause, which turned
out to be the more interesting half.

## Design items closed by this run

- ✅ Rename frequency — 3,792 in Q3 (5,757 in Q1), SCD2 confirmed on the window actually used
- ✅ Bot share — 25.6% suffix-only, and day-of-week dependent
- ✅ Case-only renames exist — SCD2 comparison must be case-sensitive
- ✅ Regex false-positive rate — measured, and the rule changed as a result
- ⚠️ Duplicate rate across hour boundaries — **still unmeasured by design**;
  deferred to Phase 1
