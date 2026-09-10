# The agent layer's window: four tools on real data, one live answer, and two configured models that could not give it

**Date:** 2026-09-10
**Workspace:** `7405615444091260` (westus3), `almanac` profile
**Governed by:** `.claude/skills/almanac-paid-window`, all five gates
**Provisioned:** nothing. Every run was a one-time `jobs submit` on a job
cluster that terminated itself; the serving endpoints were already up.

## What the window was for

Everything in Phase 9 before this ran against local Delta tables and stubs.
The window's job was to put the four tools on the real feature store and the
agent on a live endpoint, and to find whatever a green local suite could not.
It took five runs. The tools held on real data from the third. The agent
answered on the fifth, through a substitute model, because neither configured
endpoint could serve it.

## The runs

| Run | Wheel sha256 (first 12) | Setup | Execution | Outcome |
|---|---|---|---|---|
| `585922117824621` | `b0fb9d93a925` | 532 s | 105 s | failed at import: `lightgbm` not on the cluster |
| `53699732662026` | `b0fb9d93a925` | 141 s | 633 s | tool half ran, then was lost with the process; agent failed: `anyio.run` inside a running loop |
| `827768810656715` | `668221312375` | 561 s | 614 s | tool half landed as evidence; agent refused by the endpoint (403) |
| `1059095487669430` | `c1352b4848e3` | 441 s | 357 s | Llama substitute called `predict` and got a real score; run died on the second audit write |
| `693835076571838` | `fb2c10747695` | 531 s | 362 s | **answered** -- Llama substitute, agent only, evidence complete |

Every wheel was **sha256-verified identical** on both sides before its run,
by exporting the workspace copy back and hashing it. Each cluster was five
`Standard_D4ds_v6` nodes. Cost is not quoted here; see *Cost* below.

## What the tools proved on real data

Run `827768810656715`, against `events/clean` at Delta **v92** and the three
feature tables at **v0**.

- **`versions()` matched the live registry, read back rather than inferred.**
  Champion **v2**, training run `5f6a71bd9e4f4b6d8f7ea0a6454c0ca5`, the same
  pair `model-versions get-by-alias` returned independently before the run.
  The datasource tag was complete and not truncated.
- **The served features came from a later table version than training, and
  the provenance says so.** The champion trained on `events/clean` **v91**;
  every tool call in runs 3, 4 and 5 read **v92**. Both numbers travel with
  their answers, so a reader can see the gap rather than assume there is
  none.
- **Point-in-time holds at two instants on one real entity.** Repo
  `678894831`, PR `391628`, opened `2025-08-04T00:00:12Z`. At one hour and at
  twenty-four hours after opening, three features moved:
  `events_total_to_date`, `prior_pr_count`, `prs_opened_to_date`.
  Recomputing the earlier instant returned a **byte-identical** vector.
- **`predict` scored it at 0.0038**, and `explain` returned the champion's
  own contributions against a baseline of **-0.9408**, strongest pull first:
  `prior_pr_count` -2.46, `prior_merge_rate` -1.05, `prs_opened_to_date` -1.02,
  all decreasing risk.
- **The same number came back through a different path, on a different
  cluster.** Run 5's agent reached `predict` and `explain` through MCP, the
  tool gateway and a live model, on a separate job cluster, and got
  **0.0038260014552166737** -- identical to run 3's direct call in every
  digit -- with the same baseline and all five contributions identical. Run
  4's agent got the same score before it died. Same Delta versions and the
  same `as_of`, so the same numbers: the reproducibility invariant CLAUDE.md
  states, seen across three clusters rather than asserted inside one process.
  `n = 1` entity, three runs.
- **Both scoring tools refused a real reduced-era entity** rather than scoring
  it. Repo `1103012935`, PR `138713`, opened `2026-09-05T00:07:36Z`. Both
  named `bot_events_to_date` as the missing feature.

### The refusal is real, and its cause is not the one the design doc predicted

Design doc S4.2 records `is_draft` as the feature the reduced era loses.
This refusal named `bot_events_to_date`, so the cause was measured rather
than assumed:

| Repo `1103012935` in Silver | |
|---|---|
| Events before 2025-10-01 | **0** |
| Events in 2026 | 7,863 |
| First seen | `2026-09-05T00:00:39Z` |

The feature tables were built over Q3 2025. A repo first seen in 2026 has no
`repo_activity` row before its `as_of`, so every repo feature is null. **The
refusal fired correctly -- a feature the champion reads was absent -- but it
demonstrates feature-table coverage, not the `is_draft` drift.** The refusal
reports only the first missing feature in sorted order, so it does not say
whether `is_draft` was also missing. `n = 1` entity. Isolating S4.2's cause
on real data needs feature tables built over the reduced-era window, which
neither Phase 8 nor this window built. The mechanism itself is proven on the
isolated cause by the local fixture, where `is_draft` is the only difference
between the two entities.

### What the tools did not go through

`predict` and `explain` scored with the champion loaded in process from
Unity Catalog at `@champion`, not through the serving endpoint the plan
names, `almanac-pr-review-sla-risk`. `explain` reads contributions from the
native booster (`pred_contrib`), and one model object keeps the two tools'
numbers consistent with each other. So this window says nothing about what
that endpoint serves: `versions()` read the registry alias, and the
endpoint's own served version was not checked here.

## What the agent did on real data

Run `693835076571838`, agent only: the tool demonstrations were already
evidence from run 3, so they were not paid for twice. The question:

> What is the breach risk for pull request 391628 in repository 678894831 as
> of 2025-08-04T01:00:12+00:00, and which features drive it? Report the model
> version and the Delta versions behind your answer.

It made **three** model requests and **two** tool calls -- `predict`, then
`explain` -- and never called `versions`. The answer:

> The breach risk for pull request 391628 in repository 678894831 as of
> 2025-08-04T01:00:12+00:00 is 0.003826. This score is driven by several
> features, with the strongest contributions coming from "prior_pr_count",
> "prior_merge_rate", and "prs_opened_to_date", all of which decrease the risk
> of breach. The model used to generate this score is version 2, and it was
> trained on Delta versions 92 for events, and version 0 for author activity,
> repo activity, and pr static.

- **Every number in it came from a tool return.** 0.003826 is `predict`'s
  0.0038260014552166737, rounded; the three features and their direction are
  `explain`'s top three, in order; 2, 92 and 0 are the provenance both tools
  returned. Rounding is a transformation, and Phase 10's verifier will need a
  rule for it.
- **One claim in it is false, and it is attached to correct numbers.**
  "Trained on Delta versions 92": the champion trained on `events/clean`
  **v91**, per `versions()` in the same evidence. 92 is the version the two
  tools *read*. `ModelProvenance`'s own docstring says so -- "which registered
  model version, reading which Delta versions" -- but a docstring does not
  travel in a tool result. What the model saw was
  `{"model_version": "2", "delta_versions": {...}}`, which reads naturally as
  the model's own training data. It never called `versions`, the one tool
  that returns training versions, so nothing in its context could have
  corrected it. **The rule the instructions state -- every number from a
  tool -- held. The relationship claimed around the number did not.**
  `n = 1` answer.
- **The substitute is on the record, not hidden.** The evidence carries
  `"substitute_for": "databricks-claude-sonnet-5"` beside the endpoint that
  answered. The audit log and the transcript both name
  `meta-llama-3.3-70b-instruct-121024` on every response. The run had no
  fallback configured, so nothing else could have answered.
- **The transcript is committed** as
  `tests/fixtures/transcripts/2026-09-10-live-predict-explain.json`,
  byte-identical to the evidence copy (sha256 `2ab0aa9cab7b`), and
  `tests/unit/test_agent_live_transcript.py` replays it through the agent for
  free. It is the first real fixture Phase 10's grounding verifier inherits,
  and it already carries a claim that verifier has to catch.

## Why neither configured model could answer

The design pairs `databricks-claude-sonnet-5` as primary with
`databricks-gpt-oss-120b` as fallback. Neither can serve the agent in this
workspace. Probed with one tiny request each, through the real gateway code:

| Endpoint | Result |
|---|---|
| `databricks-claude-sonnet-5` | **403** -- "temporarily disabled due to a Databricks-set rate limit of 0" (from the job and from the laptop) |
| `databricks-claude-opus-5` | **403**, same message |
| `databricks-claude-opus-4-8` | **403**, same message |
| `databricks-gpt-oss-120b`, with `max_tokens` | **400** -- `unknown field "max_completion_tokens"` |
| `databricks-gpt-oss-120b`, no settings | served, but **unparseable**: content arrives as a list of reasoning and text parts, and `pydantic-ai`'s `OpenAIChatModel` requires a string |
| `databricks-meta-llama-3-3-70b-instruct` | **OK**, 42 input / 2 output tokens, 685 ms, and tool calls work |

Every endpoint reports `READY`, and none carries an AI Gateway rate limit of
its own -- only usage tracking -- so **the limit of 0 is set above the
endpoint** and is not configuration this project can change.

**The fallback policy did exactly what Task 9 built it to do, on real
infrastructure.** The 403 is permanent, `is_transient` returned false, and
the run raised the primary's own refusal without consulting the fallback.
Under the framework's default of falling back on any `ModelAPIError`, the
403 would have been routed to `gpt-oss-120b`, and the run's error would have
described the fallback's parse failure instead of the primary's refusal --
the misdirection design doc S9.3 finding 2 describes, caught live.

**The substitute was a decision, and it is recorded as one.** Llama 3.3 70B
was the only chat endpoint here that both answered and parsed, so runs 4 and
5 named it as the primary with no fallback, and `window.py` stamps
`substitute_for` into the evidence whenever the primary is not the configured
one. Sonnet 5 is **blocked, not replaced**: the configuration still names it,
and nothing in this window says how Sonnet 5 would have answered.

## Defects the window found that the local suite could not

1. **The pre-flight checked the wheel's content, not the cluster's library
   set.** The wheel does not carry its runtime dependencies; the job's
   `libraries` list does, and it lacked `lightgbm`. Run 1 died at import.
2. **Databricks runs a `spark_python_task` inside an IPython shell that owns
   a running asyncio loop**, and `anyio.run` refuses to nest in one. The same
   shell already cost this repo a `SystemExit` quirk (`cli.run_cli`). Fixed
   with `run_blocking`, and a test reproduces the exact condition.
3. **A failed model call left the audit log empty.** The one call that
   mattered most -- the 403 -- had no record in the log built to hold every
   call. Fixed: the failure is recorded, naming the model that was *asked*,
   then raised. Mutation-tested three ways when it landed in `answer`; it now
   lives in `AuditedModel` with the rest of the model's record (defect 8).
4. **`READY` is not callable.** Preflight passed while every Claude endpoint
   refuses every request. A free check cannot close this: only a request
   reveals the limit. Recorded, not fixed.
5. **`gpt-oss-120b` is not usable through `OpenAIChatModel`**, for two
   separate reasons: it rejects `max_completion_tokens`, which `pydantic-ai`
   sends in place of `max_tokens`, and its reasoning-model reply shape does
   not parse. Design doc S9's "OpenAI-compatible, including function calling
   and structured outputs" does not hold for this endpoint. Task 9's
   capability record covers sampling keys only, so it could not have caught
   the first. Recorded, not fixed.
6. **`system.serving.endpoint_usage` is empty in this workspace** -- zero rows
   ever, measured, despite usage tracking being enabled on every endpoint.
   The reconciliation source design doc S9.3 named does not work here, so the
   cost check falls back to `system.billing.usage`, read after its lag.
7. **A Unity Catalog volume refuses to append to a file that already
   exists.** It is a FUSE mount, and the second audit record of run 4 died
   on `OSError(29, 'Illegal seek')`. The audit log is append-only by
   construction, which is correct on a real filesystem and impossible on
   this one. Fixed in the runner, not the log: the live log sits on the
   driver's own disk and reaches the volume as a whole-file copy, published
   even when the agent fails. Mutation-tested two ways. Overwriting a file
   works on the volume, which is why the record itself survived every run.
8. **The model's audit record covered the run, not the request.** Run 5's
   log holds one `model_request` record, at **212.4 s**, after the two tool
   records at **143.6 s** and **61.4 s**. 212.4 is those two plus about
   7.4 s, and 7.4 s is what the transcript's own timestamps put the three
   model requests at together. The record also sat after the tools the model
   chose, and named only the model that answered last, so a fallback that
   answered an earlier request would have left the primary's name on the
   whole run. Fixed: `AuditedModel` records each request as it happens, with
   its own latency, its own model name and its own failure; `build_agent`
   applies it, so no agent can be built unaudited; and a streamed request is
   refused rather than passed through with no record. Mutation-tested six
   ways, all six killed against a green baseline: an answering request left
   unrecorded, the record naming the configured chain instead of the
   response, the timer started once instead of per request, a failed request
   left unrecorded, a stream passed through, and `build_agent` not wrapping
   the model. **Verified locally, not live**: the fix landed after the last
   run, and a sixth run to watch it was not bought.
9. **An answer attached a correct number to a false claim** (above).
   Recorded, not fixed. Naming the field for what it is -- the versions
   *read* -- is a contract change across four tools, not a window fix; and
   checking the relationship a number is claimed to have, not only the
   number, is the grounding verifier, which is Phase 10's.

Two more were caught before any run, by the offline pre-flight:
`pick_entity` stamped UTC onto a naive datetime Spark had already converted
to the driver's zone, moving the demo's instant by **four hours**; and its
neighbour join was unbounded against the whole 341M-row table.

## A ceiling the window measured

`predict` through the gateway took **145.2 s** in run 4 and **143.6 s** in
run 5, and `explain` took **61.4 s** in run 5, all on five nodes (`n = 2` and
`n = 1`). In run 5 the tools took 205.0 s and the model about 7.4 s: **the
agent is slow because its tools are, not because its model is.** Silver is
partitioned by `event_date` and `event_hour` only, with no clustering, so a
read for one `(repo_id, pr_number)` cannot prune partitions and leans on file
statistics that a random key barely narrows. That is fine for a
demonstration and wrong for an interactive agent. The upgrade path is
clustering Silver on `repo_id`, or giving the tools a per-entity index to
read instead of the event table.

## Cost: read tomorrow, and not reconciled the way the plan said

Task 11 asks for the gateway's own token accounting, reconciled against
`system.serving.endpoint_usage`. Neither side exists in the form the plan
assumed:

- **The gateway keeps no token count.** The transcript does -- `pydantic-ai`
  records usage on every response -- and by its count run 5's three requests
  used **5,865** input and **234** output tokens. The audit log carries none,
  so "the gateway's own token accounting" does not exist yet.
- **`system.serving.endpoint_usage` has no rows in this workspace**
  (defect 6), so there is nothing to reconcile against.

What remains is `system.billing.usage`, which lags, read on or after
**2026-09-11** for workspace `7405615444091260`: the job-cluster DBUs for the
five runs, and any foundation-model serving usage from the endpoint calls
and probes. **No cost is quoted until that read**, and the exit-gate row for
reconciliation stays open.

## Teardown

Verified twice after the last run, a pause apart: at **14:24:47Z** and again
at **14:29:07Z**, **0** clusters not `TERMINATED`, **0** active runs, run 5's
job cluster `TERMINATED` with `JOB_FINISHED`, and the SQL warehouse
`STOPPED`. Nothing was provisioned, so there was nothing to destroy.
