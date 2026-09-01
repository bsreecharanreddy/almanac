# Almanac — Status

Authoritative record of where implementation stands against
`docs/design/2026-09-01-almanac-system-design.md`. Updated **in the same
commit as the work it describes**, never as a follow-up.

## Current position

**Phase: pre-0. Design approved, implementation not started.**

No pipeline code, no tests, no CI, no cloud resources exist yet. Nothing
in this repo has been run.

**Two measurement passes have been taken** (2026-09-01), both against
real data, and both changed the design:

1. File sizes via HTTP `Content-Length` → the tiered dataset scope, design
   doc §4.5.
2. A full parse of one real hour → the label definition, design doc §5.1.
   Formal review events reach only ~1 PR in 4, so the target was
   redefined before any code was written against the old one.

Every other figure in the docs remains bracketed or absent by design.

## Next

**Phase 0 — Exploration (target: week of 2026-09-01).** Needs an
implementation plan in `docs/plans/` before any code is written.

Its purpose is to replace assumption with measurement:

1. Download 2 hours from 2025 and 2 hours from 2014.
2. **Diff the two schema eras against real files.** The design doc's §12
   describes the 2015 break directionally; the exact pre-2015 field names
   are explicitly unconfirmed and must come from real data.
3. Measure the remaining unknowns from design doc §4.5: the
   **uncompressed:compressed ratio** (compressed size is already
   measured), events per hour, event type distribution, bot share,
   duplicate-`event_id` rate within and across files, and **`repo_id`
   rename frequency** — the last of which decides whether the chosen
   3-month window can demonstrate SCD2 at all.
4. Stand up the local Spark container and a CI skeleton.
5. Provision Azure via Terraform early, clusters off, so week 3 is not a
   setup scramble.

**Gate:** measured numbers committed to the repo; the schema diff
documented from real data. If reality contradicts the design doc, reality
wins and the design doc gets corrected.

## Hard dates

| Date | What |
|---|---|
| **2026-09-24** | Azure free credits ($184) expire. Phase 2's cloud burn must complete before this. See design doc §9. |

## Verification log

Nothing verified yet — no code exists to verify. Each completed task adds
a row here stating what was run and what the actual result was, including
failures.

| Date | Task | What was run | Result |
|---|---|---|---|
| 2026-09-01 | Design | — | Design doc written and approved. No code. |
| 2026-09-01 | Dataset scope | `curl -sI` against 5 live GH Archive files | **Measured.** 2025 hours 62.5 / 83.2 / 113.7 MB gz; 2014 hours 5.0 / 6.2 MB gz. Firehose grew ~15× since 2014. A full unsampled quarter extrapolates to ~180 GB gz — an order of magnitude above the earlier working assumption, which is what drove the tiered scope in design doc §4.5 and the rule to sample repos rather than hours. Uncompressed ratio still unmeasured. |
| 2026-09-01 | Label validity probe | Downloaded and fully parsed `2025-03-15-14.json.gz` (227,376 events) | **Design-changing.** Expansion **7.17×** (83 MB gz → 597 MB). 6,352 PRs opened/hour, 6,015 closed (77.8% merged), but only **1,574 distinct PRs received a review event** — formal review reaches ~1 PR in 4, so "time to first review" was undefined for most of the population. Label redefined to *time to first human response* (§5.1). Also: bots are **18.2%** of events and 2,855 of PR events, drafts 2.0%, and the probed hour was a Saturday — forcing whole-week temporal splits. |
| 2026-09-01 | `.claude/` hooks | Ran both hooks against 3 constructed scenarios | **Defect found and fixed.** Both hooks read the git index at `PreToolUse` time, so `git add -A && git commit` as one command left the index empty and neither hook fired — silently, on all three of this repo's first commits. Patched to fall back to the working tree when the command also stages. Re-verified: clean tree → silent; STATUS.md+code → story-bank fires, STATUS check silent; code-only → STATUS check warns. |
