# Almanac agent layer — design

**Date:** 2026-09-09
**Status:** proposed, awaiting approval
**Supersedes:** §9's Phase 8 gate ("Tag `v1.0`. Stop.") — see §10 below
**Scope:** Phases 9 and 10, shipping as `v1.1.0`
**Open decisions:** none remaining; see §9

---

## 1. The decision, stated plainly

`v1.0` is tagged and the tag is immutable. The ML platform it names can be
cloned and run exactly as it shipped, forever, with nothing agent-shaped
in it. Continuing on `main` does not reopen that release; it preserves it
and builds past it.

**What changes:** the project's scope. `v1.0` was an ML platform. `v1.1.0`
adds an agent layer on top of it, in the same repo, on the same test
suite, under the same conventions.

**What does not change:** the medallion, the feature platform, the
champion, the streaming path, the contracts. No retraining, no new data
sources, no new tables. The agent layer is strictly additive and strictly
read-only.

## 2. Why extend, rather than stop or start elsewhere

**The repo already states the principle and never enforced it.**
`CLAUDE.md` says: *"An LLM never produces a number that a decision depends
on. If a natural-language explanation is generated, it is generated from
the model's actual feature contributions."* §6 lists that explanation as
an optional nice-to-have. Nothing in `src/` implements it, and nothing
mechanically prevents its violation. A principle enforced only by
intention is the same category of control as `pseudonymity.py` before
Phase 8: green, and aimed at nothing.

**A separate repo buys the wrong thing.** It costs its own CI, packaging,
Spark fixture harness and docs tree — roughly a week producing no signal —
and it still needs this repo's feature store to be worth anything. The
value of the tool layer is entirely that it calls the *real* as-of join
against the *real* champion. A mock feature store demonstrates nothing.

**The counter-argument, recorded rather than dismissed:** extending a
release that documented a stopping point is a scope change, and this repo
has seven recorded instances of a document that was true when written and
quietly stopped being true. That is why this doc exists and why §10
amends the system design in place. The failure mode is not extending; it
is extending silently.

## 3. What an agent platform is here

**Not an agent.** The deliverable is the infrastructure another engineer
would use to build, run and trust an agent over this platform: a typed
tool surface, a gateway that governs access to it, a bounded execution
loop, and an evaluation harness that fails the build when output is not
grounded.

The demo agent is the smallest thing that exercises those four. It is
evidence, not product.

## 4. Measured prerequisites and constraints

Three facts from the current repo, each measured rather than assumed.
Every one of them constrains the design.

### 4.1 Feature contributions do not exist

```
grep -rn "shap|contribution|pred_contrib|importance" src/ tests/
```

Zero hits across the entire source and test tree (`n` = all of `src/` and
`tests/`, 2026-09-09). §6 promises the score endpoint returns "top feature
contributions"; it was never built. The grounding verifier's premise is
that every number originates in a computed contribution, so this is a
hard prerequisite, not an enhancement. It becomes Phase 9's first task,
and it closes an open §6 promise on its own merits.

LightGBM supports this natively. `predict(..., pred_contrib=True)` returns
`[n_samples, n_features + 1]`, where **the last column is the expected
value**, not a feature — the offset from the shap package's convention,
and an off-by-one waiting to happen if taken on trust. Checked against the
primary source, 2026-09-09.

### 4.2 The champion cannot score current data, and that is a feature

Phase 8's drift run (`docs/findings/2026-09-09-drift-training-vs-scoring-window.md`)
found `pr_draft` **null on 100%** of the 2026-09-05 window. The champion
reads `is_draft`. The recorded response is: *refuse to serve rather than
re-baseline*.

That refusal currently lives in a findings document. **Phase 9 wires it
into the tool layer**, so `predict` on a window whose schema the champion
cannot read returns a structured refusal rather than a number. This is
the most valuable single behaviour in the whole design: the platform makes
fabrication structurally impossible rather than discouraged, and an agent
that must answer "I cannot score that window, and here is why" is a
better demonstration than one that always produces something.

### 4.3 The champion is modest, and the claim is not about accuracy

Version 2, PR-AUC **0.4661** against a **0.2650** baseline, a **1.76x**
lift, temporally split. The agent narrates this model. The platform claim
is traceability and reproducibility, never predictive strength, and
`docs/resume-bullets.md` already carries the do-not-claim list. Nothing in
this design may be phrased so as to imply otherwise.

## 5. Phase 9 — tool surface, gateway, agent SDK

| # | Task | Wraps |
|---|---|---|
| 1 | Feature contributions over the registered champion | new, `pred_contrib=True` |
| 2 | Tool: point-in-time features | `features/join.py` `as_of_join` |
| 3 | Tool: prediction, with the drift refusal from §4.2 | `model/score.py`, `model/drift.py` |
| 4 | Tool: explanation, from task 1's contributions | task 1 |
| 5 | Tool: versions and provenance | `model/registry.py`, MLflow `sparkDatasourceInfo` |
| 6 | MCP server exposing tasks 2–5 | `mcp` SDK |
| 7 | Gateway: allow-list, read-only enforcement, per-call audit record | new |
| 8 | Agent SDK: plan/act/observe, bounded turns, typed signatures | `pydantic-ai` |
| 9 | Model gateway: one seam, timeout, retry, token and cost accounting, one fallback | new |

**Every tool returns structured data, never prose.** That is what makes
the verifier in Phase 10 possible: prose cannot be checked against a
number it does not contain.

**Task 5 is nearly free and disproportionately valuable.** MLflow already
records the exact Delta version of every input read on each run — Phase 4
verified `events/clean` v91, three feature tables at v0, Gold's fact at
v1. Surfacing that as a tool makes a live answer traceable to the byte-level
state of the data that produced it.

**Task 9 is scoped honestly.** You need an LLM client regardless; making it
a single named seam with timeout, retry, cost accounting and one fallback
costs about a day. This is the model-gateway *pattern* at portfolio scale,
not a multi-tenant quota-enforcing router, and the doc says so rather than
implying the larger thing.

**Gate:** an agent answers a real question by calling tools; the audit log
records every call with arguments and result; a test proves no write path
exists; `predict` against a reduced-era window refuses rather than returns.

## 6. Phase 10 — grounding verifier and evaluation

| # | Task |
|---|---|
| 1 | Verifier: every numeric literal in output appears in that run's tool results |
| 2 | Retry-once-then-abstain policy |
| 3 | Golden set: questions with expected tool-call sequences, committed as fixtures |
| 4 | Directional assertions: a claim that a factor raises risk must not cite a negative contribution |
| 5 | Mutation testing of the verifier |
| 6 | CI gate: an ungrounded number fails the build |
| 7 | Per-run traces |

**Deterministic, not LLM-as-judge.** This runs against the grain of 2026
practice, where groundedness is typically scored by a small judge model.
The choice is deliberate: a judge returns a probability, and a build gate
needs a decision. It is also externally supported — Flynt's *GroundEval*
(arXiv 2606.22737, revised 2026-07-02) reports frontier judges scoring an
agent response above 0.85 where the trace showed the agent never retrieved
the artifacts at all, scoring 0.000 deterministically. Two of its three
named failure modes are ours exactly: whether an agent verified before
claiming absence, and whether it **reasoned only from time-appropriate
evidence** — which is this project's governing principle, arrived at
independently.

**Task 5 is not optional.** This repo's own record is that a test written
after the fact goes green on the first run and proves nothing;
`temporal_split` was broken three ways and `canonicalize` five of seven
before either was trusted. A verifier nobody tried to break is a control
of unknown scope, which is the `pseudonymity.py` failure again.

**Gate:** a deliberately hallucinated number fails CI.

## 7. Non-goals

Stated so they cannot be re-litigated mid-phase.

- **No assistance API.** Product surface, app-shaped, least differentiating
  of the five components on the reference job description.
- **No chat interface and no frontend.**
- **No write tools of any kind.** Read-only is the enforcement point, not
  a limitation to apologise for.
- **No multi-agent orchestration.**
- **No fine-tuning.**
- **No document retrieval.** The vector index here is feature
  infrastructure whose success metric is downstream model lift, per
  `CLAUDE.md`. Repurposing it as a chatbot corpus contradicts a stated
  principle.
- **No new data sources, no new medallion layers, no retraining.** If the
  agent layer reveals the model should improve, that is a finding, not a
  phase.
- **No multi-tenancy, quotas, rate limiting, or streaming responses.**

## 8. Live validation record — gate 2

All checked 2026-09-09 against primary sources.

| Thing | Finding | Source |
|---|---|---|
| MCP specification | Current revision **2026-07-28**: stateless protocol core, formal extensions framework, authorization hardening, Tasks extension for long-running operations | modelcontextprotocol.io |
| `mcp` Python SDK | **2.2.0**, requires Python >=3.10. The v2 line is *a major rework* built for the 2026-07-28 spec — so the version floor is load-bearing, not cosmetic | PyPI JSON API |
| `pydantic-ai` | **2.42.0**, requires Python >=3.10 | PyPI JSON API |
| LightGBM `pred_contrib` | Returns `n_features + 1` columns; last column is the expected value, unlike the shap package | LightGBM API docs |
| Grounding evaluation practice | Dominant 2026 pattern is a small judge model scoring groundedness; deterministic trace-based verification is the minority position, with published evidence that judges miss evidence-path failures | arXiv 2606.22737 |

**One discrepancy worth recording, because it is this repo's own gate-1
lesson happening live.** Search results reported `pydantic-ai` at v2.6.0.
The PyPI API returns **2.42.0**. The aggregated answer was wrong and the
first-party API was right, which is exactly why gate 2 says read the
primary source rather than the summary.

Both SDKs require Python >=3.10 and this project pins >=3.12, so neither
constrains the toolchain.

## 9. Decisions taken 2026-09-09

### 9.1 Where the tools read from: a bounded paid window

Settled. The window follows `.claude/skills/almanac-paid-window`'s five
gates, which exist because of a single night that produced a 44-minute
hang misread as a vendor outage, screenshots nearly taken from a panel
340 days wrong, and a teardown verified once that was wrong.

**The window is far cheaper than assumed, because two things are already
live.** Read back from the workspace, not from a document:

| Resource | State | Idle cost |
|---|---|---|
| `almanac-pr-review-sla-risk` | `READY`, deployment `Scaled to zero`, serving `pr_review_sla_risk` **v2** | none |
| 11 chat foundation-model endpoints | all `READY`, pre-provisioned | none, pay-per-token |
| 3 embedding endpoints | all `READY` | none, pay-per-token |

So the prediction tool has a live target with no `terraform apply` at all,
and the agent has a model with no provisioning step. The window is needed
for the *feature* reads against Delta and for contributions, not for
serving or inference.

### 9.2 Provider: Databricks Foundation Model APIs, in this workspace

Settled by measurement. `databricks serving-endpoints list --profile
almanac` returns **11** chat endpoints already `READY` (`n` = the live
workspace, 2026-09-09):

```
databricks-claude-opus-5        databricks-claude-opus-4-8
databricks-claude-sonnet-5      databricks-llama-4-maverick
databricks-gpt-oss-120b         databricks-gpt-oss-20b
databricks-gemma-3-12b          databricks-qwen35-122b-a10b
databricks-qwen3-next-80b-a3b-instruct
databricks-meta-llama-3-3-70b-instruct
databricks-meta-llama-3-1-8b-instruct
```

**Primary `databricks-claude-sonnet-5`, fallback
`databricks-gpt-oss-120b`.** The fallback is deliberately a different
vendor family with open weights, so the two do not share a failure mode.
Canopica's tiered inference client is the prior art, and its recorded
lesson is that a fallback which only ever caught one failure shape was
not really tested.

**Four properties make this the right choice, and the fourth is the one
that matters:**

1. One authentication path, already configured. No new secret, no second
   vendor account.
2. OpenAI-compatible, including **function calling and structured
   outputs**, so `pydantic-ai` drives it through `OpenAIChatModel` against
   a custom base URL. Prompt caching is available for the Claude models.
3. Metered in DBUs on the **same Azure bill** as everything else, so
   inference spend lands inside the existing cost discipline rather than
   beside it.
4. **The cost accounting in Phase 9 task 9 becomes verifiable.** Spend
   appears in `system.billing.usage`, the table this project already
   queries. The gateway's own token and cost numbers can be reconciled
   against the platform's billing record instead of self-reported. Almost
   nothing in a portfolio agent project can do that, and it is exactly
   this repo's *never quote a number that was not measured* rule applied
   to a layer that usually gets away without it.

**Rejected, with reasons.** Anthropic or OpenAI direct: separate billing
outside the Azure spend, a second secret to manage, and the loss of
property 4. Azure OpenAI or AI Foundry: a service to provision during a
paid window when eleven endpoints are already running at zero idle cost.

**A Claude Pro subscription cannot back this, and the reasons are worth
recording because the question will come back.** Checked live 2026-09-09.

It is *technically* possible: `claude setup-token` issues an OAuth token
that the **Claude Agent SDK** accepts via `CLAUDE_CODE_OAUTH_TOKEN`, and
as of 2026-06-15 that usage draws from the subscription's own limits, the
separate-credit change having been paused. A Pro plan issues **no API
key** and includes **no API credits**; the Anthropic API is a separate,
separately-billed product.

Four reasons it is the wrong runtime here, in increasing order of how much
they cost:

1. **It forces a different framework.** The token works with the Claude
   Agent SDK, not with an OpenAI-compatible or Anthropic API client. §5
   task 8 is scoped on `pydantic-ai`; this would rewrite it.
2. **The gateway seam collapses.** No API key means no uniform client, so
   the fallback in §9.2 stops being a swap of one endpoint for another
   and becomes two unrelated code paths.
3. **It breaks the clone-and-run gate.** §9's Phase 7 gate is *a stranger
   clones and runs locally in under 15 minutes*, measured at 4 m 28 s.
   A subscription token is licensed for individual use and tied to one
   personal account. An interviewer could not run the project at all, and
   a public repo whose documented setup step is "paste your Claude Code
   OAuth token" is worse than one that asks for an API key.
4. **It destroys the property that justified the choice.** No per-request
   token accounting, nothing in `system.serving.endpoint_usage`, nothing
   to reconcile against. Property 4 above was the load-bearing reason for
   Databricks, and subscription usage is opaque to the application.

**The premise in the question is also worth correcting: nothing is
purchased.** Pay-per-token carries no commitment and no upfront spend. The
eleven endpoints are already provisioned and idle at zero. The only cost
is tokens actually sent, metered per second in DBUs, and it will be
measured and reported like every other number in this repo rather than
estimated here.

**Where the subscription does belong:** writing this project, through
Claude Code. It is already paying for itself on the build side. It just
cannot also be the runtime.

### 9.3 Three live findings, and how each is fixed

*(Corrected in place 2026-09-09, before approval. This section first read
"`pydantic-ai` sends `ModelSettings` by default", which is wrong: unset
settings are omitted from the request. Checking that claim is what
surfaced finding 2, which is the more serious of the three. The original
wording is kept here rather than deleted, because the correction is the
reason the section is worth reading.)*

#### Finding 1 — Claude Sonnet 5 rejects sampling parameters

`databricks-claude-sonnet-5` returns **400** for `temperature`, `top_p`
or `top_k`. `pydantic-ai` omits unset settings, so the default path is
safe; the exposure is any `ModelSettings` configured at agent or run
level, which then applies to whichever model in the chain runs.

**Fix: make the endpoint declare what it accepts, and have the gateway
conform requests to that.** Not a comment saying "don't set temperature",
which is a convention, and this repo's whole record is that a convention
living only in practice does not survive a context boundary. A per-endpoint
capability record, and the gateway strips or refuses anything outside it.

This is `contracts.py`'s pattern pointed at requests instead of frames:
*refuse a shape the consumer could not have expected, before the write.*
The vendor quirk stops being a special case and becomes the reason the
gateway exists.

**Test on the request, not the response**, so it needs no paid endpoint:
assert the serialized body for the Sonnet 5 endpoint carries no sampling
keys. Mutation-test it by deleting the strip and confirming the test goes
red.

#### Finding 2 — the fallback would hide finding 1 completely

This is the one that matters, and it only appeared because finding 1 was
checked rather than assumed.

`FallbackModel` triggers by default on `ModelAPIError`, **which includes
4xx**. So the sequence is: a stray agent-level `temperature` makes every
Sonnet 5 call 400, the fallback catches it, `databricks-gpt-oss-120b`
answers, and **the run succeeds**. Every call silently runs on the
fallback model, no error surfaces, and the evaluation suite in Phase 10
would score a model nobody chose.

A 400 for an unsupported parameter is permanent and is *our* defect. A
fallback is for transient failure. Conflating the two builds a system that
routes around its own misconfiguration.

**Fix, three parts:**

1. Set `fallback_on` explicitly to transient conditions only — timeouts,
   429, 5xx. A 4xx raises. The framework supports this; the default is
   simply wrong for this use.
2. **Record which model actually answered on every call**, in the same
   audit record as the tool calls. Which model served a response is never
   inferred, the same way §4.2's serving-version defect was found by
   reading the endpoint back rather than trusting the pin.
3. Test the fallback against **more than one failure shape**. Canopica's
   recorded lesson is a fallback that only ever caught one shape and was
   therefore never really tested. At minimum: a timeout, a 429, a 5xx,
   and a 400 that must **not** fall back.

#### Finding 3 — the docs table and the live workspace disagree

The "General purpose" list on the query page omits
`databricks-claude-sonnet-5`, while the endpoint reports `READY` here.
Probably documentation lag; the point is not to adjudicate it.

**Fix: never depend on the table.** A preflight that lists serving
endpoints and refuses to run unless the configured primary and fallback
both exist and are `READY`, failing with a message that names what *is*
available.

Same shape as `almanac.infra.lakebase_window.check_region`, which refuses
an unsupported region before terraform is invoked, and whose message names
the *symptom* because the failure otherwise looks like an outage. Here the
symptom is a 404 on the first call inside a paid window.

**This is a standing guard, not a one-time check, because models retire.**
Databricks announces a retirement date at least **three months** out, and
Claude Sonnet 4 already carries **October 9, 2026**. Both choices are
clear as of 2026-09-09: neither `databricks-claude-sonnet-5` nor
`databricks-gpt-oss-120b` appears on the retirement list, and GPT OSS 120B
is itself the recommended replacement for Llama 3.1 405B.

#### A better reconciliation source than §9.2 named

§9.2 proposed checking gateway cost accounting against
`system.billing.usage`. There is a more direct one:
`system.serving.endpoint_usage` joined to `system.serving.served_entities`
carries **per-request input and output token counts** plus the requester
and endpoint. Databricks publishes it as the query for finding workloads
on retired models, which makes it serve both purposes at once: it verifies
the gateway's own token numbers, and it is how finding 3's guard learns
what this workspace actually uses.

### 9.4 One honest scoping note about the word "gateway"

Databricks routes foundation-model requests through **Unity Gateway**,
which already applies rate limits, budgets and guardrails. Phase 9 task 9
therefore sits in front of something that is itself a gateway. The doc
says so rather than implying the whole capability was built here. What
task 9 adds is the seam this project controls: timeout, retry, fallback
policy, and cost accounting reconciled against billing.

### 9.5 Version number

`v1.1.0`, settled. No existing interface breaks, so semver is the honest
answer and the earlier `v2.0` framing is dropped.

## 10. Amendment to the system design doc

§9's Phase 8 row reads "Tag `v1.0`. Stop." That was correct when written
and remains correct as a description of `v1.0`. On approval of this
document it gets a dated in-place amendment noting that the project
continues at Phase 9 under this design, with a pointer here — appended,
not rewritten, per the correcting-a-recorded-decision rule.

Leaving §9 as-is while building Phase 9 would be the eighth instance of
the failure this repo keeps counting, and the first one committed
knowingly.
