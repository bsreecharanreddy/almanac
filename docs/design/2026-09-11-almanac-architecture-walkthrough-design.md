# Architecture walkthrough — a fifth tab (design)

Addendum to `2026-09-11-almanac-local-demo-design.md`, not a replacement.

**This supersedes that doc's §11 row rejecting a fifth panel**, and says
so rather than quietly overriding it. §11 rejected *"the point-in-time
panel as a fifth"* because §4.1's coverage panel already made that
argument — a different, narrower proposal than this one. Weighed here as
a real tradeoff (a separate `st.navigation` page vs. a fifth `st.tabs()`
entry): a page keeps the two interaction modes cleanly apart and gives
the walkthrough its own shareable URL, at the cost of one more click to
discover it exists. A fifth tab is immediately visible to every visitor
with no extra step, which matters more for a hiring-manager-facing demo
than the separation — decided explicitly in favor of the tab on that
basis, 2026-09-11.

## 1. What this is

The four existing tabs show **the system working now**: a live queue,
live per-feature contributions, the agent's verified answer, the
medallion counts. Nothing in the demo shows **how it got built and what
was learned** — the OpenMP segfault, the leakage bug in the registered
champion, the watermark postmortem, the cost discipline. That story
already exists, in the README, eight ADRs, `docs/findings/`, and the
story-bank gist. This tab is an interactive index over it, not a new
narrative: click a pipeline stage, get a one-line summary and a link to
where the real story is recorded.

## 2. The one governing rule

**A node names a fact; it never restates one.** A node may say "found a
leakage bug in the registered champion" and link to
`docs/findings/2026-09-08-champion-rescored-temporal-split.md`. It may
not also say "0.612 → 0.4661, optimistic by 24%" — that number lives in
exactly one place already, and a second copy is the failure mode this
repo has recorded nine times (`CLAUDE.md`, Current status). Enforced by a
test, not a convention: no node's summary text may contain a digit
outside markup like a stage name (`v1.0`), because the moment it does,
it is asserting a measurement rather than pointing at one.

## 3. Scope — the node/edge graph

One node per pipeline stage, edges following data flow:

```
Bronze -> Silver -> Gold -> Feature Platform -> Model (baseline + champion)
                                              -> Serving (endpoint)
Gold -> Streaming (poller, online store)
Model -> Agent layer -> Grounding verifier
```

Each node's click-through target (an existing doc, ADR, or finding —
nothing new is written for this):

| Node | Links to |
|---|---|
| Bronze / Silver | `docs/limitations.md` (schema eras, quarantine), `docs/adr/` entry on dedup-on-write |
| Gold | `dbt/`, the reduced-era merge defect finding |
| Feature Platform | the leakage suite, `docs/adr/0001` (as-of join) |
| Model | `2026-09-08-champion-rescored-temporal-split.md`, `docs/decision-memo.md` |
| Serving | the live-endpoint-on-retracted-champion finding, skew measurement |
| Streaming | `docs/postmortem-watermark-data-loss.md` |
| Agent layer | the agent-layer window cost finding, Phase 9/10 sections of the README |
| Grounding verifier | the "trained on" ungrounded-claim finding, same as the demo's own Agent tab |

Each node also carries the OpenMP segfault as a *cross-cutting* callout
(it touches Model and this demo specifically), linked to Task 1's
`src/almanac/model/native.py` and its STATUS.md entry, not restated.

## 4. Technical approach

- `streamlit-flow-component` (React Flow wrapper) for the diagram —
  verified to exist and support clickable nodes before this doc was
  written, not assumed.
- One more entry in `demo/app.py`'s existing `st.tabs([...])` call --
  no restructuring of the app's single-script shape, no
  `st.navigation`/`st.Page` migration. The new tab's rendering is its
  own function in `almanac.demo.panels` (or a new sibling module if the
  node/edge graph is sizeable enough to warrant one), matching how the
  other four tabs already separate panel-data-preparation from the
  Streamlit calls themselves.
- `demo/requirements.txt` regenerated from `uv.lock` the same way as
  before, with `streamlit-flow-component` added to the `demo` extra.
- No new data, no new computation — every linked target already exists
  in this repo.

## 5. Non-goals

- Not a replacement for the story-bank gist, the README's "In sixty
  seconds," or the ADRs. It points at all three; it restates none.
- Not a claim-bearing artifact — see §2. If a reviewer wants the number,
  the link is one click away in the source that already has it.
- Not required by the Phase 11 exit gate (`docs/design/2026-09-11-almanac-local-demo-design.md`
  §10) — that gate is already met by the four-panel app. This is
  additive, tracked as its own task, and does not block Task 13's close.

## 6. Exit gate

- [ ] Every node's link target resolves to a real, existing file/anchor in
      this repo (a test walks the node list and checks each path)
- [ ] No node summary contains a digit outside a version string or a
      stage/file name (the §2 rule, enforced as a test)
- [ ] `AppTest` renders the tab with no exception
- [ ] The walkthrough deploys on the same Community Cloud app with no
      separate hosting step or new page route
- [ ] `docs/STATUS.md` records it as its own task, separate from Task 13
