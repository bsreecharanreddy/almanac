# Resume bullets

§10 asks for these and they were the last item recorded as *not done*.
Every figure traces to a findings doc; nothing here is rounded up.

**Rule applied throughout:** claim only what was measured, and prefer the
bullet that survives a follow-up question over the one that sounds
larger.

## Short form — four bullets

- Built an **ML platform for work-queue risk** end to end on 341M real
  events across a schema break: medallion pipeline, point-in-time-correct
  feature store, model, serving endpoint, streaming ingest, and governance
  — solo, on Azure Databricks, for a measured **$11.96** of compute on the
  historical backfill.
- Enforced **point-in-time correctness** as a testable invariant rather
  than a convention — a feature vector computed `as_of` T must be
  byte-identical on recompute from the same Delta version — and **found a
  leakage bug in my own registered model** through it: a random train/test
  split inflated PR-AUC by **24%** (0.612 → 0.4661 once split temporally).
- Shipped a **measured baseline before the model** and published the null
  result when LightGBM lost to a per-segment median by **51%** on the
  regression target; reframed to classification, which beat its baseline
  **1.76×** on a temporal split.
- Took the platform **live and then tore it down**, measuring what most
  projects assume: serving cold start **51.96 s**, warm **p50 263.5 ms**,
  Vector Search idle **$6.72/day** (it does not scale to zero), online
  store idle **$12.06/day**.

## Longer form — with the story behind each

**Point-in-time correctness, and finding my own leakage bug.**
The governing invariant is that no feature may read data at or after its
own timestamp, enforced by a dedicated leakage suite. Writing an honest
limitations document — *not* writing tests — surfaced that the train/test
split was random where the design requires temporal. **The suite was green
throughout and was not wrong**: it tested row time, and the bug was on
split time. Re-scored: **0.612 → 0.4661 PR-AUC**, optimistic by 24%. The
winning hyperparameter configuration changed too, so a random split would
have shipped the wrong *config*, not just an inflated number. The
retracted figure is still published, labelled.
*(`docs/findings/2026-09-08-champion-rescored-temporal-split.md`)*

**Streaming correctness under a real failure.**
A watermark-based dedup silently discarded **distinct late events**, not
just duplicates, losing **161 entities** in a live run. It looked correct
on the first run because a cold checkpoint had not advanced the window. I
wrote the prediction down *before* running the experiment that confirmed
it, then replaced the design with a stateless insert-only merge. Two
independent measurements agreed to the row.

**Cost as an engineering discipline.**
Every cloud resource is provisioned by Terraform and destroyed between
sessions, with the teardown guarded by executable checks rather than a
README. A cross-check against the cloud biller's own API found the
platform's usage table had been understating real spend by nearly half.
Idle rates were measured, not assumed: a "serverless" Vector Search
endpoint drew a flat 4 DBU/hour whether queried or idle.

**Data quality that survives contact with real data.**
341M events across three schema eras, with quality rules that quarantine
by *rule* rather than by boolean, null-safe SCD2 comparison, and
`coalesce(cond, False)` on every rule so a NULL cannot vanish from both
the valid set and the quarantine. Zero events were ever quarantined —
which I report as an explicit zero rather than an empty panel, because
"no data" and "nothing failed" look identical to a reader.

## Numbers, with their sources

| Claim | Value | Source |
|---|---|---|
| Events ingested | 341,060,851 | `2026-09-02-backfill-*` |
| Backfill cost | $11.96 | same |
| PR-AUC, temporal split | 0.4661 (baseline 0.2650) | `2026-09-08-champion-rescored-*` |
| PR-AUC, random split (retracted) | 0.612 | `2026-09-04-classification-*` |
| Serving cold start | 51.96 s | `2026-09-04-model-serving-measured` |
| Serving warm p50 / p95 | 263.5 ms / 376.8 ms | same |
| Live feed capture rate | ~7% | `2026-09-06-events-api-*` |
| Coverage, transform + features | 88%, gated 85% | Phase 7 Task 7 |
| Clone to green run | 4 m 28 s | Phase 7 Task 14 |

**Do not claim:** p99 latency (not measurable at n=20), streaming through
Gold (the live path stops at Silver and the online store), or that the
model works on current data (it reads `is_draft`, which the reduced era
does not carry).

## `v1.1.0` addendum — the agent layer and its grounding verifier

Additive to everything above, not a replacement for it. Same rule: claim
only what was measured.

### Short form — two bullets

- Added a **read-only agent layer** over the platform — four tools behind
  an allow-listed gateway with a per-call audit log, and a bounded
  plan/act/observe agent — then built a **deterministic grounding
  verifier** that checks every number and every claim about that number
  in an agent's answer against the tool call that produced it, with no
  LLM judge in the loop.
- Caught and fixed a **real false claim my own agent produced**: every
  number in one live answer came from a tool, and it still misstated what
  one of them meant. Root-caused to a schema field that didn't say what
  it was versions *of*, fixed with a rename plus a relationship check —
  not a stricter number check — and covered by a golden set that fails
  the build on that exact hallucination today.

### Longer form — with the story behind it

**A grounded answer is not the same as a correct one, and I found the gap
in my own system.** The agent's one live answer during a paid validation
window put every number through a real tool call — a green result under
the project's own "every number from a tool" rule — and still claimed the
champion was "trained on" a Delta version the tools had only *read*. The
fix was not a stricter numeric check; the number was already correct. It
was a second, separate check that a claim of a given type (trained-on,
read-as-of, model-version, score, baseline) traces to the *specific*
field that type means. Built deterministically — regex and structural
traversal over the run's own transcript, no second model — deliberately
against the grain of the dominant 2026 pattern of scoring groundedness
with an LLM judge, on published evidence (GroundEval, arXiv 2606.22737)
that judges can score an ungrounded answer above 0.85.

### Numbers, with their sources (v1.1.0)

| Claim | Value | Source |
|---|---|---|
| Read-only tools shipped | 4 | `docs/STATUS.md`, Phase 9 |
| Model requests per agent run, capped at | 6 | `almanac/agent/bounded_agent.py` |
| Real runs needed to get one live agent answer | 5 | `docs/findings/2026-09-10-agent-layer-window.md` |
| Grounding checks in the verifier | 3 (numeric, relationship, directional) | `docs/STATUS.md`, Phase 10 |
| Retries before an ungroundable answer abstains | 1 | `almanac/agent/bounded_agent.py` |

**Do not claim:** that either configured foundation model can serve the
agent in this workspace (neither can — Llama 3.3 70B answered as a
recorded substitute), that the agent has a live serving endpoint (it runs
in offline validation windows only), or that the grounding verifier's
relationship table covers claim shapes beyond the ones this system's four
tools can actually produce.
