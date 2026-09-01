# Findings — dataset measurements

**Date:** 2026-09-01
**Sample:** 24 hourly files — 3 non-adjacent days (2025-01-08, 2025-02-12,
2025-03-19) × 8 hours each (0,3,6,9,12,15,18,21). All 24 fetched `ok`.
**Volume parsed:** 6,002,410 events.

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
| Distinct repos observed | 1,234,736 |
| **Renames detected** | **5,757** |
| Gate to proceed | ≥ 50 |

**`dim_repo`'s SCD Type 2 is decisively supported. The candidate window
does not need to move.** This was the only remaining item in Phase 0 that
could have forced a design change, and it is closed.

The renames are also *real and varied*, not an artefact — ownership
transfers (`Yonom/assistant-ui` → `assistant-ui/assistant-ui`), user
renames (`Sam948-byte/photonvision` → `samfreund/photonvision`), project
renames (`big-mouth-cn/m3u8-checker` → `big-mouth-cn/tv`), and typo fixes
(`paulba71/DoodFoodTracker` → `paulba71/DogFoodTracker`). At least one
repo renamed **twice** inside the window (`lilshepit/skills-github-pages`
→ `lilshepit/lilshepit` → `lilshepit/Blog---lilshepit`), which exercises
the multi-version SCD2 path rather than just a single transition.

## Duplicate rate — measured, but **this sample cannot test trap 4**

| | |
|---|---|
| Events | 6,002,410 |
| Distinct `event_id` | 6,002,409 |
| Duplicate ratio | 0.000000 (exactly 1 duplicate) |

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
| `[bot]` suffix | 1,740,438 | **29.0%** |
| regex heuristic | 70,318 | 1.2% |
| not a bot | 4,191,654 | 69.8% |

**29% by the authoritative suffix rule alone**, notably higher than the
18.2% measured earlier on a single Saturday hour (design doc §5.1). These
three days are all Wednesdays, and scheduled automation (Dependabot,
Renovate, CI) runs on weekday cadences while humans do not. **Bot share is
itself strongly day-of-week dependent**, which reinforces §5.1's
requirement that temporal splits fall on whole-week boundaries.

See `2026-09-01-bot-classification.md` for the regex clause, which turned
out to be the more interesting half.

## Design items closed by this run

- ✅ Rename frequency — 5,757, SCD2 window confirmed, no change needed
- ✅ Bot share — 29% suffix-only, and day-of-week dependent
- ✅ Regex false-positive rate — measured, and the rule changed as a result
- ⚠️ Duplicate rate across hour boundaries — **still unmeasured by design**;
  deferred to Phase 1
