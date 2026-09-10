---
name: almanac-paid-window
description: Use before provisioning anything billable in this project — a reporting window, the Lakebase stack, a serving endpoint, any terraform apply that creates cloud resources. Enforces reading the project's own record first, capturing perishable evidence before teardown, and verifying teardown independently, because each of those failed once on 2026-09-08 at a measurable cost.
---

# Opening a paid window in Almanac

**Incident-derived, every gate.** This was written the night of
2026-09-08, after a single window that cost 44 minutes to a diagnosis
that was wrong, nearly published a screenshot of incorrect numbers as
evidence, and reported a clean teardown that a second read contradicted.

The tooling already carries what can be mechanised: `make window-up` /
`window-down` guard the westus3 reporting window, and `make lakebase-up` /
`lakebase-down` guard the centralus stack and refuse an unsupported
region before terraform runs. **This file is the part a guard cannot
check.**

## Gate 1 — Read the record before spending

**The 44 minutes.** A targeted apply of
`databricks_database_instance.online_store` from the westus3 module hung
for 44 minutes. The API returned "temporarily unavailable" then timed
out, while every other Databricks API answered instantly, and that was
written up as a vendor outage. It was not one: **Lakebase is not offered
in westus3**, `docs/STATUS.md` had said so for two days, and the status
page took under a minute to disprove the outage claim.

Before any apply that creates a billable resource:

1. **Grep `docs/STATUS.md` for the resource and the service.** CLAUDE.md
   names it the authoritative record of where implementation stands. It
   is the cheapest check available and it was skipped.
2. **Read the module's own README** if it has one.
   `infra/terraform-lakebase/README.md` documents four teardown traps
   that exist only because Phase 6 hit them.
3. **If a claim about a vendor is forming, check the vendor.** A status
   page is 30 seconds. "It behaves like an outage" is a symptom, not a
   diagnosis — an unsupported region produces the identical symptom.

## Gate 2 — Pre-flight, before a single resource comes up

Phase 7 Task 10's pre-flight caught four dead table references that would
otherwise have been found inside a paid window. Run the offline
equivalents first:

- **The chosen window passes the characterizer** — a degraded window
  produces degenerate panels and an uncomputable label.
- **Every dashboard query resolves** against the current schema.
- **The plan reaches no further than its declared targets.**
- **The deployed wheel is current, verified by content or hash on both
  sides.** A job runs the wheel in the workspace, not your working tree:
  triggering without redeploying re-runs the old code and *reproduces the
  old number*, which looks exactly like confirmation. The re-score on
  2026-09-08 came within one step of this; the local wheel was eleven
  hours stale.
- **The cluster's library set is complete, not only the wheel.** The wheel
  carries the code and none of its runtime dependencies; the job's
  `libraries` list carries those. On 2026-09-10 the first Phase 9 run died
  at import on `lightgbm`, with the wheel sha256-verified identical on both
  sides — verifying the wheel is not verifying what it needs. Diff a new
  job's `libraries` against a job that already runs the same imports.

## Gate 3 — Capture perishable evidence before teardown, not after

Some things cannot be recovered once the stack is gone:

- **The workspace id.** Billing lands ~24h late, so the DBU reading
  happens *after* teardown, and without the id the rows are
  unattributable.
- **Run ids, durations, row counts, measured states.**
- **Anything a screenshot would show.** The online store existed for
  about 35 minutes on 2026-09-08 and the console evidence was not
  captured in it.

**And verify what you are about to capture.** Before taking dashboard
screenshots, run the queries. On 2026-09-08 a panel returned 200
perfectly healthy rows while aging every PR against a horizon **340 days**
wrong — nothing errored and nothing was empty. Capturing when asked would
have published confidently wrong numbers as proof of progress.

**Write the evidence as it is produced, not once at the end.** On
2026-09-10 the agent step failed after ten minutes of tool demonstrations
had already run. The record had been written before the agent started, so
those ten paid minutes survived as evidence instead of dying with the
process.

## Gate 4 — Verify teardown independently, then verify again

**Never the destroy's exit code.** A tool's exit code reports what it
attempted.

- **Read the count before applying the destroy.** Phase 5's targeted
  destroy pulled in a dependent job and would have thrown away the run
  history that was the evidence for four real runs.
- **Check from several angles**: terraform state, the resource listing,
  the cloud provider, and — the one that matters for a second root module
  — *that the other stack is untouched*.
- **Re-check after a pause.** The first sweep on 2026-09-08 showed two
  dashboards still `ACTIVE` after they were confirmed absent from state.
  A re-read returned zero: **propagation lag, not orphans**. A teardown
  verified once, immediately, can report stale objects in either
  direction.

## Gate 5 — If it hangs, do not kill it blindly

A killed create is how a resource ends up alive in the cloud and absent
from state, where no later `destroy` will reach it. It is recoverable via
`terraform import`, so it is not permanent — but it bills until someone
notices.

Before killing: check whether the *next* resource in the dependency order
was created. If it was not, the stall is on the first one and nothing
later exists. After killing: `force-unlock` if needed, then check state
for what actually landed, and say plainly what could not be checked.

## Red flags

| Thought | Reality |
|---|---|
| "It behaves like an outage" | An unsupported region behaves identically. Check the status page and STATUS.md. |
| "The apply succeeded, so the data is right" | An incremental model with no new rows reports SUCCESS and changes nothing. |
| "The destroy said complete" | That is what it attempted. Read state, then read it again. |
| "I'll capture the evidence after" | Billing lags a day and the workspace id dies with the stack. |
| "The panel renders, so it is fine" | It rendered 200 healthy rows against a 340-day-wrong horizon. |
| "Just retry the apply" | Twice is not a diagnosis. The second 44 minutes costs the same as the first. |
