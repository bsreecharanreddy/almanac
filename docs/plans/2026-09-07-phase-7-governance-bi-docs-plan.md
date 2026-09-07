# Phase 7: governance, reporting, and the reproducibility gate (implementation plan)

Written from design doc §4.7 (`docs/design/2026-09-01-almanac-system-design.md`),
which carries the six decisions this plan wires into code and the measured
state each was taken against. Same register as
`docs/plans/2026-09-06-phase-6-streaming-plan.md`: interfaces and test
intent, not full inline code, since there is no separate review pass
between writing this and implementing it. TDD discipline, one STATUS.md
verification-log row per task, one commit per task, unchanged.

**The gate is `make check-fast` per task and `make check` before the push** —
what the Makefile and CLAUDE.md already prescribe. This plan's first draft
said "`make check` green before every commit", stricter than either, and
expensive enough to matter: the full suite is ~20 minutes, so that would have
spent roughly four hours across 14 tasks re-running a Spark suite that a docs
or Terraform change cannot affect. Corrected 2026-09-07, once the cost became
visible. The full suite still runs before the push, unchanged — that is where
the cross-phase regressions this project keeps producing actually surface.

**This phase is different from every phase before it in one way worth
naming.** Phases 1–6 each built a new capability. Phase 7 mostly makes
what already exists *checkable by someone else* — lineage that can be
queried rather than claimed, contracts that fail a build rather than
describing themselves, a clone-to-running path with a measured number on
it. The failure mode is therefore not "the code doesn't work"; it is
**writing documentation that quietly overstates what is true**. Every
task below is shaped so its claim is produced by a command, not by a
sentence.

**A survey came first, and it removed work.** §10's "data contract
enforced as a CI failure" turned out to be **already satisfied for Gold**
(dbt `contract: enforced: true` on both consumer models, plus
`tests/integration/test_gold_contracts.py` proving the build fails). Two
panels of §7's page 2 turned out to have **no recoverable data source at
all**. One item — inference capture — turned out to be **time-critical in
a way nothing in §9 suggested**. None of that was knowable from the
design doc; all of it changed the plan.

## Ordering principle

**Task 1 first, and it is one command.** Enabling inference capture is the
only item in this phase whose cost rises the longer it waits: the endpoint
is live and free today, and the only traffic that can ever be logged is
traffic that happens after capture is on. Every day it is deferred is a
day of prediction history that Phase 8 cannot recover.

Everything else is local and free except Task 10's narrow warehouse
window. As in Phases 5 and 6, the single real-money step happens **once,
late**, after the artifacts it validates are already built and tested.

```
1 (capture ON, cloud, ~0 cost)
        │
2 (lineage extract) ─→ 3 (lineage artifact + test) ─┐
                                                     ├─→ 9 (dashboards as code)
4 (contracts: features + stream) ─→ 5 (contract+SLA doc) ─┘        │
                                                                   ▼
6 (bring-up/tear-down path) ─────────────────────────────→ 10 (narrow window, cloud)
                                                                   │
7 (coverage measured) ──┐                                          ▼
8 (PII audit) ──────────┼─→ 11 (ADRs) ─→ 12 (limitations) ─→ 13 (memo + postmortem)
                        └─→ 14 (§10 reconciliation + <15 min gate, measured)
```

Tasks 2–8 are local-only and independently committable. Task 9 authors
dashboard JSON offline; Task 10 is the only task that provisions
anything billable.

---

## Task 1: Turn on inference capture, before anything else

**Why first:** §4.7's measured state — the endpoint is `READY`,
`scale_to_zero = true`, drew zero inference DBUs on 09-07, and
`auto_capture_config` is `null`. Nothing has ever been logged.

**The change:** add an `ai_gateway { inference_table_config { … } }` block
to `databricks_model_serving.pr_review_sla_risk` in `infra/terraform/`,
plus its own `databricks_schema` — not the schema holding the registered
model, because Databricks creates an internal `_checkpoints` volume beside
the table whose deletion corrupts it. Apply, then send real invocations
against the model's actual signature so the table is created and provably
non-empty.

**As built — the mechanism in this plan's first draft was the wrong one,
caught before anything was applied.** The draft said `auto_capture_config`.
The Terraform provider still documents that block **with no deprecation
marker**, but Databricks' product docs for it are formally *retired* and
direct to AI Gateway: the provider trails the product, so **the provider's
silence is not evidence.** Second time a gate-2 check has caught a
load-bearing API as superseded before design hardened around it.

That also **corrected this task's stated constraint.** "Payload logging
cannot be re-enabled once disabled; catalog/schema/prefix cannot change
after setup" describes the *legacy* mechanism. The real one runs the other
way — once AI Gateway tables are on, the endpoint **cannot go back to
legacy** — and enabling on an endpoint with no inference table configured
is explicitly supported, which is this endpoint's exact case. What is
genuinely irreversible is only the data: the log starts when capture
starts.

**A second claim was corrected by measurement, not by reading.** The docs
describe a "fast inference table" for CPU custom-model endpoints — a
`_payload` **view** over an `_otel_logs` table, delivering in seconds. What
was actually created here is a **MANAGED table with no `_otel_logs` beside
it**, so the 1-hour best-effort path applies instead. The code comment
asserting the fast path was corrected to say what was observed.

**Done when:** the inference table exists in UC, contains rows from a real
invocation, and a **targeted** `terraform plan` is clean. **Record the row
count, the true first `request_time`, and the measured delivery lag** —
that timestamp is the honest start of the series and page 2 must not imply
history before it.

**Targeted, and this is not optional.** A bare `terraform plan` here reads
**6 to add, 1 to change**: the five extra creates are Phase 5's and Phase
6's deliberately torn-down infrastructure — the Lakebase instance
(~$12.06/day), the Vector Search endpoint (~$6.72/day, no scale-to-zero),
its index, the streaming job and its volume. Phase 6's teardown notes
already recorded that "a bare `apply` plans to recreate the Lakebase
instance"; this is that trap, met again in the opposite direction. Apply
with `-target` on the schema and the endpoint only.
**Commit:** `feat(serving): log inference requests, so page 2 has a series to plot`

## Task 2: Extract column lineage the way this repo's own storage requires

**The trap is already measured** (§4.7): **4,133 of Almanac's 4,778
column-lineage rows — 86.5% — carry only `source_path`, never
`source_table_full_name`**, because Bronze, Silver and features are
external Delta paths. A query filtering on table name returns 13.5% of the
graph **and reports no error**.

**The change:** a module that reads `system.access.column_lineage` and
`system.access.table_lineage` and resolves edges by `source_path` /
`target_path` **as well as** by three-part name, normalising both into one
edge list keyed on the medallion tier.

**Tests:** a fixture containing a path-only edge, a name-only edge, and one
of each pointing at the same logical table, asserting all three resolve to
the same node. **The path-only case is the regression test for the 86.5%**
— it must fail if someone later "simplifies" the query to names only.

**Done when:** the extractor returns edges for every medallion tier, and
the path-only test fails on a name-only implementation.
**Commit:** `feat(lineage): resolve UC lineage by path, not just by table name`

## Task 3: The lineage artifact, and an honest statement of its blind spots

**The change:** generate a committed artifact — edge list plus a rendered
graph — for the critical path raw → Bronze → Silver → Gold → features,
with a `make lineage` target that regenerates it.

**The blind spots ship *in* the artifact, not in a footnote elsewhere:**
local Spark runs are invisible to UC (which is the entire test suite), and
the system tables hold a **rolling 1-year window** (Catalog Explorer and
the lineage API retain indefinitely for lineage captured after
2024-09-01). Both are stated on the artifact itself, where a reader
looking at the graph will actually see them.

**Test:** the generated artifact is compared against a checked-in
expectation for the tier-to-tier edges, so a silent loss of a whole tier
fails CI rather than producing a quietly smaller picture.

**Done when:** `make lineage` reproduces the artifact byte-for-byte, and
every medallion tier appears with its edges.
**Commit:** `feat(lineage): a column-lineage artifact that states what it cannot see`

## Task 4: Contracts on the surfaces that have none

**Not a rebuild.** Gold is already contract-enforced and already has a
break-it test. The gap is the **feature tier and streaming Silver**, which
carry no contract at all.

**The change:** schema + semantic contracts for the published feature
tables and the streaming Silver output, enforced by a check that fails
`make check`.

**Test, in this project's established style: watch it fail.** A deliberate
breaking change (drop a column, widen a type, null a key) is applied on a
scratch branch and the check is confirmed red before the change is
reverted — the same discipline `test_gold_contracts.py` already uses, and
the reason that test is trustworthy.

**Done when:** a deliberate breach fails CI, demonstrated, not asserted.
**Commit:** `feat(contracts): enforce the feature and streaming surfaces, proven by a breach`

## Task 5: The data contract + SLA document

§10's *Documentation* list names this and **nothing in the repo provides
it in any form.** Distinct from Task 4: that makes contracts fail a build,
this states what a consumer is entitled to rely on — freshness, retention,
schema stability, what may change without notice and what may not.

**Grounded in measured numbers, not aspirations.** Freshness comes from
Phase 6's measured end-to-end p50 of 561s **with its 302s upstream floor
stated**, because an SLA that silently promises to beat GitHub's own feed
delay is a promise the system cannot keep.

**Done when:** every number in it cites the finding that measured it.
**Commit:** `docs: the data contract and SLA, with every number sourced`

## Task 6: One command up, one command down

**The deliverable the user added to §9's scope**, and the thing that makes
Phase 8 affordable enough to run more than once.

**The change:** a documented bring-up / tear-down path covering the
warehouse and dashboards, with teardown shipping alongside provisioning
rather than as a follow-up (§11's standing rule).

**It carries Phase 6's four teardown traps as executable checks, not
prose** — blank `terraform output` after a partial destroy presenting as an
*auth* error; an external location citing dependents that no longer exist;
`force_destroy` inert until an `apply` writes it to state; a bare `apply`
planning to **recreate** a destroyed resource. Each cost real time in Phase
6; each is cheap to guard against once known.

**Done when:** proven by being the mechanism Task 10 actually uses. Not
tested separately — used.
**Commit:** `feat(infra): one command up, one command down, with Phase 6's traps guarded`

## Task 7: Measure coverage, then decide the gate

§10 asks for **≥70% on transformation and feature logic** — a scoped
figure, not repo-wide, so the measurement must scope to `pipeline/`,
`features/` and `gold/` rather than quoting a flattering global number.

**Measure first, gate second.** If it already clears 70%, wire the
threshold into CI so it cannot regress. If it does not, **report the real
number and raise coverage where the gap is genuine risk** — this project
does not move a threshold to meet a number.

**Done when:** the real figure is published and either gated or explained.
**Commit:** `test: measure coverage on transformation and feature logic, and gate it`

## Task 8: The pseudonymization audit

§10: no actor identity in any published artifact. **The dashboards raise
the stakes** — page 3 renders contributor data by design, and `dim_repo`
carries real repo and actor names.

**The change:** an automated check across published surfaces (dashboard
JSON, README, `docs/`, images) plus a documented masking rule for what the
dashboards display. `2026-09-06-console-evidence.md` already masks
`Run as` / `Created by`; this generalises that from a manual habit into a
check.

**Done when:** the check runs in CI and fails on a planted identifier.
**Commit:** `feat(governance): fail the build on an actor identifier in a published artifact`

## Task 9: Dashboards as code

**Pages 1–2 as `databricks_dashboard` resources** with their JSON
committed and referenced by `file_path`, so they are diffable, reviewable
and destroyable like every other resource here.

**Page 2 ships with two panels visibly marked unavailable**, not faked and
not quietly dropped: feature freshness lag and training/serving skew
against the online store both need Phase 6's stack, which is destroyed.
Marking them is the same discipline the Photon A/B used when it withheld
an indeterminate arm.

Panels with real data today, verified 2026-09-07: the Bronze→Silver→Gold
row funnel, quarantine rate by rule, schema-version distribution showing
the 2015 break, run duration and cost per run (`system.lakeflow.
job_run_timeline`, 63 runs from 09-01), model metrics from MLflow, and —
after Task 1 — prediction volume and latency.

**Feature-tier panels need the feature tables registered in UC first**;
they are ADLS paths today. That registration is part of this task, or the
drift panels join the marked-unavailable list. **Decide by checking, not
by assuming.**

**Done when:** `terraform plan` shows both dashboards, and every panel
either has real data or is marked.
**Commit:** `feat(reporting): SLA-risk and platform-health dashboards, defined as code`

## Task 10: The narrow window — dashboards against real Gold, then down

**The only billable step.** Warehouse up via Task 6's path, dashboards
pointed at real Gold, evidence captured, everything down.

**Capture before the irreversible step**, the rule Phase 6 earned twice:
screenshots and every measured number are recorded **before** teardown,
and anything needed *after* teardown (ids, run references) is captured too
— the addition Phase 6 had to make when a workspace id was nearly lost.

**Done when:** dashboards demonstrated against real data, evidence added
to `2026-09-06-console-evidence.md`'s successor, warehouse down, spend
measured and reported.
**Commit:** `feat(reporting): dashboards demonstrated against real Gold, warehouse torn down`

## Task 11: Six to eight ADRs

**Retrospective, and that is the honest framing** — these decisions were
made and recorded across findings docs as they happened; the ADRs
consolidate them into the standard form. Written to be *found*, not to
perform process.

Strong candidates, each with a real rejected alternative already on
record: hand-rolled as-of join over Feast/Databricks FE; Vector Search
over FAISS; **do not enable Photon** (break-even 1.55–2.16 against a ~2x
multiplier); dedup-on-write over watermark dedup; UC lineage over
OpenLineage; AI/BI over Power BI; Lakebase's forced region split.

**Done when:** every ADR names an alternative that was genuinely
considered and cites the finding that settled it.
**Commit:** `docs: six to eight ADRs, each citing the finding that settled it`

## Task 12: `docs/limitations.md`

§12's traps stated plainly, plus what this project itself cannot do:
~7–9% live capture against the firehose (**a politeness choice, not a rate
limit** — the distinction is the whole point); the reduced-era payload
making live label computation impossible; right-censoring; bot
classification's measured false-positive rate; UC lineage's blind spots
from Task 3; the two dashboard panels from Task 9.

**Done when:** a skeptical reader finds their objection already written
down, in this project's own words, before they raise it.
**Commit:** `docs: limitations, stated before someone else has to find them`

## Task 13: The decision memo and the postmortem

**Memo:** one recommendation, **with a stated confidence level** — §10 is
specific about that, because a recommendation without a confidence is an
opinion wearing a suit.

**Postmortem: the watermark data loss** (Phase 6 Task 9→10). It is the
strongest candidate by some distance and it has the property a good
postmortem needs: the **prediction was recorded before the confirming
run**, so the causal claim is checkable rather than reconstructed. Cold
checkpoint lost 0, resumed checkpoint lost 161, two independent
measurements agreeing at 217 late events. Runners-up: the embedding
fork-deadlock, and the SKU-search error that produced a false pattern from
`n=2`.

**Done when:** the postmortem states what would have caught it earlier —
and, being honest, that the repo's own test asserted the buggy behavior
with a comment justifying it.
**Commit:** `docs: a decision memo with a confidence level, and one real postmortem`

## Task 14: Reconcile §10 against reality, and time the clone

**Two halves of one honesty check.**

First, walk §10's goal checklist item by item and mark each **done, partly
done, or not done, with evidence** — no item ticked without something that
proves it. This is where an overstated claim surfaces, and it is
deliberately last, after every other task has either produced its evidence
or failed to.

Second, **§9's actual exit gate**: a stranger clones and runs locally in
under 15 minutes. **Timed on a genuinely fresh clone with a cold cache and
the real number published**, pass or fail — not asserted, not measured on
a warm machine that already has every dependency. If it comes in over 15
minutes, the number is published and the gap is named. This project does
not round a measurement toward its target.

**Done when:** §10 reflects reality, and the clone-to-running time is a
published measurement.
**Commit:** `docs: reconcile the goal checklist with reality, and time the fresh-clone run`

---

## Exit gate

- [ ] Inference capture on, with rows from a real invocation and the series' true start recorded
- [ ] Column lineage produced for every medallion tier, resolved by path as well as name
- [ ] The lineage artifact states its own blind spots
- [ ] A deliberate contract breach on the feature/streaming surfaces fails CI, demonstrated
- [ ] Data contract + SLA published, every number citing its finding
- [ ] Pages 1–2 exist as Terraform-managed dashboard JSON in the repo
- [ ] Page 3 built in Power BI, carrying the limitations panel
- [ ] Dashboards demonstrated against real Gold in a bounded window, then torn down
- [ ] One command up, one command down — proven by use, not by test
- [ ] Coverage on transformation and feature logic measured, and gated or explained
- [ ] No actor identifier in any published artifact — enforced by a check, not by care
- [ ] 6–8 ADRs, `docs/limitations.md`, one memo with a confidence level, one postmortem
- [ ] §10's checklist reconciled against reality, item by item, with evidence
- [ ] **Fresh clone to working local run, timed, real number published**
- [ ] Every measured claim states its `n`

## Deferred out of Phase 7, on purpose

- **The full-stack live demo** — Phase 8, per §4.7. Requires re-provisioning
  Lakebase and the streaming path together.
- **Phase 6 console evidence** — impossible here; the stack is destroyed.
  Phase 8's window is the only remaining opportunity, and against a *fresh*
  instance, not the one that produced the measurements.
- **Feature freshness and training/serving skew panels** — same dependency;
  shipped marked rather than faked.
- **OpenLineage** — superseded by §4.7, not skipped. The cost is no
  vendor-neutral lineage export, and that is stated where the decision is.
- **A fourth report page** — §11 already settled that page count stops
  carrying signal past three.
