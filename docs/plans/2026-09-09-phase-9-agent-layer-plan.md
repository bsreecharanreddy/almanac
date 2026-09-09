# Phase 9 — the tool surface, the two gateways, and a bounded agent

Written from `docs/design/2026-09-09-almanac-agent-layer-design.md`, which
records the decisions this plan executes: same repo, `v1.1.0`, Databricks
pay-per-token, primary `databricks-claude-sonnet-5` with fallback
`databricks-gpt-oss-120b`, three components not five.

**This is a new subsystem**, so unlike Phase 8 every task here builds
something that does not exist. What it must *not* do is change anything
underneath it. The medallion, the feature platform, the champion, the
streaming path and the contracts are read-only inputs to this phase.
A diff to any of them is a signal that a task has drifted out of scope.

Branch `phase-9-agent-layer`, carrying the whole phase, one push, one PR.
**One commit per task, `docs/STATUS.md` updated in the same commit,
`make check-fast` per task and the full `make check` before the push.**

---

## Sequencing, and why

**Everything offline first; one paid window at the end.** Phase 8's Task 7
is the precedent: pre-flight caught four dead table references that would
otherwise have been found inside a billable window.

Two things make that easier here than in earlier phases.

**The serving endpoint is already live.** `almanac-pr-review-sla-risk` is
`READY` and `Scaled to zero` on champion **v2**, so predictions need no
`terraform apply` at all. Nothing in this plan applies infrastructure.

**But LLM calls bill from the very first one.** Unlike every previous
phase, where local work was free, an agent loop under development spends
money on each iteration. So the loop is developed against **recorded
transcripts**, captured once in Task 10 and replayed thereafter. That is
not only thrift: it is what makes Phase 10's evaluation suite
deterministic, and it is why Task 10 comes before Task 11 rather than
after.

**One ordering constraint is load-bearing.** Task 8 (the model gateway)
must land before Task 10 (the agent), because §9.3's finding 2 is that a
default `FallbackModel` silently absorbs a 400 and answers every call on
the fallback model. Building the agent first means every transcript
recorded in Task 10 could come from a model nobody chose, and nothing
would say so.

**Order: 1 → 2 → 3, 4, 5, 6 → 7 → 8 → 9 → 10 → 11 (billable) → 12.**

---

## Module layout

New code lives in `src/almanac/agent/`, except contributions, which belong
to the model:

| Module | Holds |
|---|---|
| `src/almanac/model/contributions.py` | per-feature contributions from the champion |
| `src/almanac/agent/schemas.py` | typed tool inputs and outputs |
| `src/almanac/agent/tools.py` | the four tool implementations |
| `src/almanac/agent/mcp_server.py` | MCP server exposing the tools |
| `src/almanac/agent/gateway.py` | tool gateway: allow-list, read-only, audit |
| `src/almanac/agent/models.py` | model gateway: capabilities, fallback, preflight |
| `src/almanac/agent/loop.py` | the bounded agent |

A new `agent` optional-dependency group in `pyproject.toml`, floors
checked live at Task 1 rather than copied from this document. The two that
matter today: `mcp>=2.2.0` (the v2 line is a rework built for the
2026-07-28 spec, so the floor is load-bearing, not cosmetic) and
`pydantic-ai>=2.42.0`.

---

## Task 1 — Feature contributions

**Why.** They do not exist. `grep -rn "shap|contribution|pred_contrib|importance"
src/ tests/` returns nothing across the whole tree. Design doc §6 promised
"top feature contributions" from the score endpoint and it was never built,
so this is an integrity debt the project already owed. Everything in
Phase 10 rests on it: the verifier's premise is that every number the agent
emits originates in a computed contribution.

**Do.** A pure function in `src/almanac/model/contributions.py`: booster
plus feature frame in, per-feature contributions plus the baseline out.
No I/O, per the repo's structural rule.

**The trap, and it is a real one.** `predict(pred_contrib=True)` returns
`[n_samples, n_features + 1]`, and **the last column is the expected
value, not a feature** — LightGBM's documented departure from the shap
package's convention. Taken on trust that is an off-by-one that silently
mislabels every contribution.

**Done when.** Contributions for a row sum to the raw prediction score
within tolerance, which is the property that catches the off-by-one
without hard-coding column positions. Mutation-tested: drop the last
column, keep it as a feature, and reverse the column order — each must
turn a test red.

**Cost.** None. Local, against a fixture model.

---

## Task 2 — Tool schemas

**Why.** Every tool returns structured data and never prose. That is the
single decision that makes Phase 10 possible: prose cannot be checked
against a number it does not contain.

**Do.** Pydantic v2 models for each tool's input and output in
`schemas.py`. Every numeric field carries its units and its provenance
(which Delta version, which model version) as fields, not as prose.

**This is `contracts.py`'s pattern pointed at a new surface** — *refuse a
shape the consumer could not have expected* — and the plan should reuse
its vocabulary rather than invent a parallel one.

**Done when.** Round-trip serialization tested; a response missing
provenance fails validation rather than serializing with a null.

**Cost.** None.

---

## Task 3 — Tool: point-in-time features

**Why.** The platform tool. §6 calls `/features/{id}?as_of=` "the demo that
carries the interview".

**Do.** `get_features(entity_key, as_of)` reading through the real
`features/join.py:as_of_join`. No reimplementation of the join, no
shortcut around it — if the tool computes features by any path other than
the platform's own, the demo proves nothing.

**Done when.** Two calls at different `as_of` values on the same entity
return different vectors, and the earlier one provably contains nothing
after its own timestamp. That is the leakage suite's assertion, applied at
the tool boundary. `.claude/skills/almanac-leakage-review` gate 1 applies:
state which leakage axis this covers, which is row time, and say plainly
that it covers no other.

**Cost.** None locally; real tables in Task 11.

---

## Task 4 — Tool: predict, and the refusal

**Why.** §4.2 of the design doc. Phase 8's drift run found `pr_draft`
**null on 100%** of the 2026-09-05 window, the champion reads `is_draft`,
and the recorded response is *refuse to serve rather than re-baseline*.
That response currently lives only in a findings document.

**Do.** `predict(entity_key, as_of)` over the registered champion, with the
drift check from `model/drift.py` in front of it. When the features the
champion needs are not present in the window, return a **structured
refusal** naming the feature and the reason — never a number, and never
prose explaining why there is no number.

**This is the most valuable behaviour in the phase.** An agent that must
sometimes say "I cannot score that window, and here is which feature is
missing" demonstrates more than one that always produces something. It
also makes fabrication structurally impossible rather than discouraged.

**Locally, inject a fake.** `model/score.py` already defines a
`ProbabilityModel` Protocol, so tests supply a stub and no endpoint is
called until Task 11.

**Done when.** A reduced-era window returns a refusal carrying the
offending feature name; a rich-era window returns a score. Mutation-tested
by removing the drift check, which must turn the refusal test red.

**Cost.** None until Task 11.

---

## Task 5 — Tool: explain

**Why.** The tool the grounding verifier checks against.

**Do.** `explain(entity_key, as_of)` returning Task 1's contributions as
structured rows: feature name, contribution, direction, and the baseline.
Sorted, truncated to top-k, with k explicit in the response rather than
implied.

**Done when.** Every contribution returned traces to Task 1's output;
directions match contribution signs. That second assertion is what Phase
10's directional check will build on.

**Cost.** None.

---

## Task 6 — Tool: versions and provenance

**Why.** Nearly free, and disproportionately valuable. MLflow's
`sparkDatasourceInfo` tag already records the exact Delta version of every
input a run read — Phase 4 verified `events/clean` v91, three feature
tables at v0, Gold's fact at v1. Surfacing it makes a live answer traceable
to the byte-level state of the data that produced it.

**Do.** `versions()` returning registered model name and version, the
training run id, the training-data Delta versions, and the feature-set
version.

**Read the registry back, never infer it.** Phase 8 found the serving
endpoint *actually serving* the retracted v1 while the alias had moved to
v2. This tool must report what is live, not what a config says.

**Done when.** The version this tool reports matches what
`databricks model-versions list` returns for the endpoint, asserted rather
than eyeballed.

**Cost.** None locally; verified live in Task 11.

---

## Task 7 — MCP server

**Why.** The tool surface has to be reachable by something other than
Python imports, or it is a library rather than a platform.

**Do.** An MCP server over `mcp>=2.2.0` exposing Tasks 3–6. Check the
floor live: v2 is a rework for the 2026-07-28 spec, which made the
protocol stateless and moved protocol version, client identity and
capabilities into a `_meta` parameter per request.

**Done when.** A client lists exactly four tools with their schemas and
calls each one. Schemas come from Task 2's models, not hand-written twice
— two statements of one fact is the duplication `CLAUDE.md` item 4 names,
and this repo has already been bitten by it three times.

**Cost.** None.

---

## Task 8 — Tool gateway: allow-list, read-only, audit

**Why.** This is the security story, and read-only is the enforcement
point rather than a limitation to apologise for.

**Do.** A broker in front of the MCP server that (a) refuses any tool not
on an explicit allow-list, (b) refuses any call that could write, and (c)
records every call — tool, arguments, result, latency, timestamp — to an
append-only audit log.

**Enforce structurally, not by inspection.** A test that greps tool source
for "write" is theatre. The tools are constructed from a registry that has
no write path to construct, and the test asserts the registry cannot
produce one.

**Done when.** An unregistered tool name is refused with a message naming
what *is* available; every call in a session appears in the audit log in
order; mutation-tested by adding a write-capable tool, which must be
refused rather than logged.

**Cost.** None.

---

## Task 9 — Model gateway, and the three findings it exists to fix

**Why.** §9.3 of the design doc. This task is where all three live
findings get fixed, and it must land before the agent for the reason in
Sequencing.

**Do, in four parts.**

**9a — Endpoint capability records.** Each endpoint declares which
sampling parameters it accepts. `databricks-claude-sonnet-5` returns
**400** for `temperature`, `top_p` and `top_k`. The gateway conforms
requests to the record. Not a comment saying "don't set temperature",
which is a convention, and this repo's own record is that a convention
living only in practice does not survive a context boundary — see the
em-dash rule that held for six commits purely by pattern-matching and
broke on the seventh.

**9b — Fallback policy.** Set `fallback_on` explicitly to transient
conditions only: timeouts, 429, 5xx. A **4xx must raise**. The framework's
default triggers on any model API error including 4xx, which means one
stray agent-level `temperature` makes every primary call fail, the
fallback answers, the run succeeds, and every response silently comes from
a model nobody chose.

**9c — Record which model answered**, on every call, in Task 8's audit
record. Never inferred. Same discipline as Task 6.

**9d — Preflight.** Before any run, list serving endpoints and refuse
unless both configured models exist and are `READY`, failing with a
message naming what *is* available. Same shape as
`almanac.infra.lakebase_window.check_region`, which refuses an unsupported
region before terraform is invoked and whose message names the *symptom*,
because the failure otherwise looks like an outage. Here the symptom is a
404 on the first call inside a paid window.

**Done when.** The serialized request body for Sonnet 5 carries no
sampling keys — a test on the **request**, so it needs no paid endpoint.
The fallback is tested against **four** failure shapes: a timeout, a 429
and a 5xx that must fall back, and a 400 that must **not**. Canopica's
recorded lesson is a fallback that only ever caught one shape and was
therefore never really tested. Preflight refuses a made-up endpoint name.

**Cost.** None. Every assertion here is on requests and stubs.

---

## Task 10 — The bounded agent, and the recorded transcripts

**Why.** The smallest thing that exercises Tasks 3–9. Evidence, not
product.

**Do.** A plan/act/observe loop on `pydantic-ai>=2.42.0` with a hard turn
bound, typed tool signatures from Task 2, driving the Databricks endpoints
through `OpenAIChatModel` against a custom base URL — the Databricks
serving API is OpenAI-compatible, including function calling and
structured outputs.

**Capture transcripts on the first live run and replay them thereafter.**
Every subsequent test is deterministic and free, and Phase 10's evaluation
suite inherits the same fixtures.

**Done when.** The agent answers a question by calling tools; the turn
bound is enforced and tested at the boundary; a run that hits the bound
terminates with a structured incomplete result rather than a truncated
answer.

**Cost.** The first billable step, and small. Measured and reported like
every other number, not estimated here.

---

## Task 11 — The window (the phase's only provisioned step)

**Why.** Everything above runs against local tables and stubs. This is
where the tools read the real feature store.

**Follow `.claude/skills/almanac-paid-window`'s five gates.** They exist
because of a single night that produced a 44-minute hang written up as a
vendor outage when `docs/STATUS.md` had said for two days that Lakebase is
not offered in that region, screenshots nearly taken from a panel 340 days
wrong, and a teardown verified once that a re-read contradicted.

**Do.** Read the record first. Pre-flight offline: Task 9d's endpoint
check, plus every table reference resolved live before anything runs.
Then the tools against real Delta tables and the live serving endpoint,
the `as_of` demo at two timestamps, the refusal demonstrated on a real
reduced-era window, and `versions()` reconciled against the live registry.

**Capture perishable evidence before teardown, and verify what you are
about to capture.** Then verify teardown independently, twice.

**Reconcile the cost.** The gateway's own token accounting against
`system.serving.endpoint_usage` joined to `system.serving.served_entities`,
which carries per-request input and output token counts. A cost number
this project publishes should be checkable against the platform's own
record, which is the *never quote a number that was not measured* rule
applied to a layer that usually escapes it.

**Done when.** Every claim in this phase is demonstrated on real data, the
evidence is captured, the cost is measured and reconciled, and teardown is
verified twice.

**Cost.** Measured, not estimated.

---

## Task 12 — Close out

**Do.** `docs/STATUS.md` rows already landed per task. Here: a findings
doc for the window, `README.md` updated including its architecture diagram
if the diagram's subject changed, `CLAUDE.md` updated for any convention
this phase set, and the story-bank gist updated.

**The story bank trigger is unreliable by default** — documented as never
having self-fired across roughly a dozen sessions on a prior project. This
task exists so it does not depend on remembering.

**Done when.** All three reader-facing artifacts answer *what changed
today that a reader would want to know?* — the positive check, not "is it
still accurate?", which a stale file passes trivially.

---

## Exit gate

- [ ] Contributions sum to the prediction; off-by-one mutation-tested
- [ ] Four tools, structured returns, schemas defined once
- [ ] `as_of` at two timestamps returns different vectors, earlier one leakage-clean
- [ ] A reduced-era window returns a structured refusal naming the feature
- [ ] `versions()` matches the live registry, read back not inferred
- [ ] MCP client lists and calls all four tools
- [ ] No write path exists, enforced structurally and mutation-tested
- [ ] Every tool call and every model choice appears in the audit log
- [ ] Sonnet 5 requests carry no sampling parameters, asserted on the request
- [ ] Fallback tested on four failure shapes; the 400 does **not** fall back
- [ ] Preflight refuses a missing endpoint, naming what is available
- [ ] Turn bound enforced; hitting it yields a structured incomplete result
- [ ] Real tables and the live endpoint exercised in one window
- [ ] Token cost reconciled against `system.serving.endpoint_usage`
- [ ] Teardown verified twice
- [ ] Full `make check` green before the push

---

## Deferred out of Phase 9, on purpose

**To Phase 10:** the grounding verifier, the golden set, directional
assertions, mutation testing of the verifier, evaluation as a CI gate, and
per-run traces. Phase 9 produces the transcripts Phase 10 scores.

**Out of scope entirely**, per design doc §7 and not to be re-litigated
mid-phase: assistance API, chat interface, any write tool, multi-agent
orchestration, fine-tuning, document retrieval, new data sources, new
medallion layers, retraining, multi-tenancy, quotas, rate limiting,
streaming responses.

**One thing deliberately not deferred, and it is worth naming.** Task 1
closes §6's unbuilt promise of feature contributions. It would have been
easy to scope this phase as purely additive and leave that debt standing,
which is how a promise made in a design doc becomes a claim nobody checks.
