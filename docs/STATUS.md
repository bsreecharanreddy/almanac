# Almanac — Status

Authoritative record of where implementation stands against
`docs/design/2026-09-01-almanac-system-design.md`. Updated **in the same
commit as the work it describes**, never as a follow-up.

## Current position

**Phase: 0 (Exploration), Task 2 of 9 complete.** Executing
`docs/plans/2026-09-01-phase-0-exploration-plan.md` on branch
`phase-0-exploration`.

Scaffold, tooling, CI and a containerized local Spark + Delta environment
exist and are green. No pipeline code, no cloud resources yet.

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
| 2026-09-01 | Phase 0 Task 1 — scaffold, tooling, CI | `make check` (ruff, mypy --strict, pytest) | **Green.** 3 tests pass. Installed live-checked versions: pyspark 4.2.0, delta-spark 4.4.0, chispa 0.12.0, ruff 0.16.5, mypy 2.3.1, pytest 9.1.1, pydantic 2.13.5. Three findings: (1) **delta-spark 4.4.0 declares `pyspark<=4.2.0`, and 4.2.0 is current** — the ceiling is exactly where we sit, so pyspark is capped `<4.3` or a future release breaks at SparkSession construction, not install; (2) the plan's GitHub Action versions were both wrong — `checkout` is v7.0.1 (plan said v5), `setup-uv` v10.0.1 (plan said v7), corrected from `gh api`; (3) plan ordering bug — `uv` cannot build the package without `src/almanac/__init__.py`, so the TDD "watch it fail" step could not run until the marker existed. Also found ruff 0.16.5 formats Python inside markdown, so plan docs are held to the same standard; accepted rather than excluded. |
| 2026-09-01 | Phase 0 Task 2 — local Spark + Delta | `pytest -m spark` on host; full suite in container | **Green, 6 tests, after four real defects.** (1) `delta-spark` pip ships Python bindings only — without `configure_spark_with_delta_pip` the JVM has no Delta data source and every `.format("delta")` dies with `ClassNotFoundException`; the plan configured the extensions but never arranged JAR resolution. (2) **`openjdk-17-jre-headless` no longer exists** in the Debian release backing `python:3.12-slim` (`no installation candidate`) — moved to Java 21, which Spark 4.x also supports; host stays on 17, a recorded divergence. (3) Hardcoded `JAVA_HOME` was arch-specific and wrong on one of arm64/amd64; dropped entirely since PySpark falls back to `java` on PATH. (4) No `.dockerignore` meant `COPY . .` shipped the host `.venv` into the image and host paths appeared in container tracebacks; and `uv run` at CMD re-synced without `--all-extras`, silently dropping pyspark so the container run looked like it worked but tested the wrong environment. **`array_compact` confirmed AVAILABLE** in Spark 4.2.0, closing that design doc §13 item — Phase 1's rule engine can use it. `replaceWhere` idempotency proven by test. |
| 2026-09-01 | Label validity probe | Downloaded and fully parsed `2025-03-15-14.json.gz` (227,376 events) | **Design-changing.** Expansion **7.17×** (83 MB gz → 597 MB). 6,352 PRs opened/hour, 6,015 closed (77.8% merged), but only **1,574 distinct PRs received a review event** — formal review reaches ~1 PR in 4, so "time to first review" was undefined for most of the population. Label redefined to *time to first human response* (§5.1). Also: bots are **18.2%** of events and 2,855 of PR events, drafts 2.0%, and the probed hour was a Saturday — forcing whole-week temporal splits. |
| 2026-09-01 | `.claude/` hooks | Ran both hooks against 3 constructed scenarios | **Defect found and fixed.** Both hooks read the git index at `PreToolUse` time, so `git add -A && git commit` as one command left the index empty and neither hook fired — silently, on all three of this repo's first commits. Patched to fall back to the working tree when the command also stages. Re-verified: clean tree → silent; STATUS.md+code → story-bank fires, STATUS check silent; code-only → STATUS check warns. |
