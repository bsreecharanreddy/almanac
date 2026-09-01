# Almanac — Status

Authoritative record of where implementation stands against
`docs/design/2026-09-01-almanac-system-design.md`. Updated **in the same
commit as the work it describes**, never as a follow-up.

## Current position

**Phase: pre-0. Design approved, implementation not started.**

No pipeline code, no tests, no CI, no cloud resources exist yet. Nothing
in this repo has been run. No number anywhere in these docs has been
measured yet — every figure in the design doc is bracketed or absent by
design.

## Next

**Phase 0 — Exploration (target: week of 2026-09-01).** Needs an
implementation plan in `docs/plans/` before any code is written.

Its purpose is to replace assumption with measurement:

1. Download 2 hours from 2025 and 2 hours from 2014.
2. **Diff the two schema eras against real files.** The design doc's §12
   describes the 2015 break directionally; the exact pre-2015 field names
   are explicitly unconfirmed and must come from real data.
3. Measure file size compressed and uncompressed, events per hour, event
   type distribution, bot share, and duplicate-`event_id` rate within and
   across files.
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
