# Almanac — Status

Authoritative record of where implementation stands against
`docs/design/2026-09-01-almanac-system-design.md`. Updated **in the same
commit as the work it describes**, never as a follow-up.

## Current position

**Phase: 0 (Exploration), Task 6 of 9 complete.** Executing
`docs/plans/2026-09-01-phase-0-exploration-plan.md` on branch
`phase-0-exploration`.

Scaffold, tooling, CI, a containerized Spark + Delta environment, and the
ingestion edge (URL construction, fetching, committed fixtures) exist and
are green. No transformation code, no cloud resources yet.

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
| 2026-09-01 | Phase 0 CI — first real run | GitHub Actions on `ubuntu-24.04`/amd64, PR #1 | **Red, then fixed.** `Unable to resolve action astral-sh/setup-uv@v10`. Root cause is non-obvious and worth keeping: `gh api .../releases/latest` returns a **release tag**, which is not necessarily a resolvable **action ref**. `actions/checkout` publishes a floating major tag (`refs/tags/v7` exists); `astral-sh/setup-uv` does not — only `v10.0.1`. Confirmed with `git/ref/tags/<ref>` on both before changing anything. Pinned setup-uv exact. Vindicates pushing per task: this is a CI-only failure class that local `make test` and the container run structurally cannot catch. |
| 2026-09-01 | Phase 0 Task 3 — archive URLs | `pytest tests/unit/test_urls.py`, plus a live `curl -sI` on a generated URL | **Green, 9 tests.** Live check returned `HTTP/2 200`, so the scheme is confirmed against the real host rather than merely self-consistent. The unpadded-hour/padded-date asymmetry is pinned by test: zero-padding the hour would 404 ten of every twenty-four files. |
| 2026-09-01 | Phase 0 Task 4 — fetching | `make check` (ruff, mypy --strict, 29 tests) | **Green.** Four fetch outcomes distinguished and each pinned by test: 404 → ABSENT and never retried; 200-with-zero-bytes → EMPTY and never retried; 5xx/429 → FAILED and retried to the configured limit; transport error → FAILED leaving no file behind. Downloads land on `.part` and are renamed, so a crash mid-write cannot leave a truncated file a later run mistakes for complete. Ruff caught `class FetchStatus(str, Enum)` as superseded by `enum.StrEnum` on 3.11+; fixed rather than suppressed. |
| 2026-09-01 | Phase 0 Task 5 — Tier 0 fixtures | `make fixtures` against the live host; `make check` (32 tests) | **Green.** Both fixtures built at **0.76 MB**, far under the 5 MB cap. Corroborating measurement: the 2014 hour carried **17,138 events** against 2025's **227,376** — a **13.3× growth in event count**, consistent with the ~15× file-size growth measured earlier, from an independent signal. Confirmed via `git check-ignore` that fixtures are tracked (`.jsonl.gz` sidesteps the `*.json.gz` rule) and that `data/` holding 89 MB of raw downloads is not. |
| 2026-09-01 | Phase 0 Task 6 — schema-era diff | Field-path diff over 2,000 events per era; `make check` | **Green (4 tests), and design-changing.** Three findings, each measured across 2,000/2,000 events, now in `docs/findings/2026-09-01-schema-eras.md`: (1) **legacy events have no `id` at all** — 0/2,000 — invalidating the `event_id` dedup key the design was built on; resolved by a content hash for legacy with an `event_id_source` column (new §4.1a). (2) **`actor` is a bare string in legacy, an object in modern** — a type change, not the "different representation" the doc described, and legacy carries no numeric actor id at all. (3) **Legacy `created_at` carries a `-07:00` offset**, so naive UTC parsing shifts every pre-2015 timestamp seven hours, silently, past every schema check — the most dangerous property found so far given this project's premise (new §4.1b). Also corrected two wrong claims: the legacy-only event type is `TeamAddEvent`, not `DownloadEvent`/`FollowEvent`/`GistEvent`; and legacy language coverage is **better** than modern (1,712/2,000 direct on `repository.language`), the opposite of what trap 10 implied. `repo_id` is stable across both eras, so the SCD2 natural key survives. |
| 2026-09-01 | Label validity probe | Downloaded and fully parsed `2025-03-15-14.json.gz` (227,376 events) | **Design-changing.** Expansion **7.17×** (83 MB gz → 597 MB). 6,352 PRs opened/hour, 6,015 closed (77.8% merged), but only **1,574 distinct PRs received a review event** — formal review reaches ~1 PR in 4, so "time to first review" was undefined for most of the population. Label redefined to *time to first human response* (§5.1). Also: bots are **18.2%** of events and 2,855 of PR events, drafts 2.0%, and the probed hour was a Saturday — forcing whole-week temporal splits. |
| 2026-09-01 | `.claude/` hooks | Ran both hooks against 3 constructed scenarios | **Defect found and fixed.** Both hooks read the git index at `PreToolUse` time, so `git add -A && git commit` as one command left the index empty and neither hook fired — silently, on all three of this repo's first commits. Patched to fall back to the working tree when the command also stages. Re-verified: clean tree → silent; STATUS.md+code → story-bank fires, STATUS check silent; code-only → STATUS check warns. |
