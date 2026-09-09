# Public Repo Readiness — Final-Pass Checklist

**Not a phase.** This is the wrap-up for everything already deferred to
"once active development is done, right before the repo goes public."
Every item below has its own decision recorded elsewhere; consolidated
here so nothing is missed. **Nothing here is new scope.**

**Sequencing.** `docs/plans/2026-09-08-phase-8-ship-plan.md` is the last
real engineering work. This runs after Phase 8 closes and `v1.0` is
tagged — operational and documentation wrap-up, not new capability.

Structure follows the sibling project's flip of 2026-08-31, which worked;
its two hard-won lessons are carried over verbatim and marked **[prior
art]**.

---

## 0. Time-sensitive — do these now, not at the end

- [x] **Nothing billable is running.** `terraform state list` in both root
      modules. `databricks_model_serving.pr_review_sla_risk` **is
      currently up** — it scales to zero and drew zero DBUs on 09-07, so
      it is not costing anything idle, but "believed to scale to zero" and
      "measured at zero" are different claims and only the second one goes
      in a README.
      **Verified 2026-09-09, five ways.** centralus state list: empty.
      westus3 state: workspace + storage only (deliberate — it holds the
      341M-row quarter). SQL warehouse: `STOPPED`. All clusters:
      `TERMINATED`. Vector Search endpoints: none. And the serving
      endpoint reports `deployment_state_message: "Scaled to zero"` on
      **v2** — the measured claim, not the believed one, and v2 confirms
      the retracted champion is no longer served.
- [~] **Azure credit expiry 2026-09-24.** Confirm the balance and whether
      anything still draws on it. Design doc records "credit expiry is a
      budget, not a wall" — restate the position rather than discovering
      it.
      **Half answered 2026-09-09, and the half that is missing is named.**
      Subscription `bscr-az-portfolio` is `Enabled`. *What still draws on
      it*: 1,198 usage records over Sep 1–9, across Compute (836), Storage
      (214), Databricks (115), network (32) and EventGrid (1) — consistent
      with the workspace and storage account being deliberately left up to
      hold the 341M-row quarter, with all compute terminated. *The
      balance*: *not obtained.* `az consumption usage list` returns every
      cost field as the literal string `'None'` for this subscription, so
      the figure needs the portal or the Cost Management API. **Not
      quoted, because it was not measured** — that is the rule, and an
      estimate here would break it on the last day. *(2026-09-09: the
      Databricks half is now known, **$101.79** of DBUs across 8 billed
      days. That is still not the credit balance — Azure-side VM, storage
      and network charges bill separately and are precisely what the CLI
      will not return.)*
      Position restated: credit expiry is a budget, not a wall. Nothing in
      the repo depends on the credit surviving; every billable resource is
      torn down and the remaining storage is a deliberate, stated choice.
- [x] **Re-read the idle rates the day after the Phase 8 window.**
      `system.billing.usage` lags ~24h and cannot be queried during the
      window it measures.
      **Done 2026-09-09 on an explicit decision to spend for it**
      (`docs/findings/2026-09-09-final-billing-read.md`). Warehouse
      started, four queries, stopped and confirmed `STOPPED`.
      **The current idle rate is $0.00/day.** This does *not* retract the
      $12.06/day quoted elsewhere: that measured the Lakebase instance
      while it was running, and remains correct about that. The two
      measure different states.
      Nothing had drawn a DBU in **over 8 hours** at the time of the read.
      Total Databricks spend across the project: **$101.79** over 8 billed
      days, peaking at $25.46 on the Phase 5 embeddings day. Billing is
      also an independent **third** confirmation of the centralus
      teardown: every `US_CENTRAL` SKU stops at exactly
      `2026-09-09T03:00:00Z` and shows nothing since. And the serving
      endpoint, deliberately left up, has drawn nothing since 09-08
      23:48Z — `scale_to_zero` measured rather than believed.

---

## 1. Content and documentation pass

- [~] **PII sweep across the whole history, not the tip.** This repo has
      already paid for this once: a commit trailer leaked a machine name
      containing a personal name and took a `git filter-repo` rewrite over
      **126 commits** to remove (recorded in `.gitignore`). Sweep commit
      messages, docs, notebooks, fixtures, and **the committed images** —
      the dashboard screenshots and run captures in `docs/images/` were
      masked by hand.
      **Run 2026-09-09, and it found three things.** Clean: commit
      *metadata* (every author and committer identity is a GitHub noreply
      address — 199 commits, 198 + 1 dependabot by author, 181 + 18 by
      committer; this line first said "all 375 commits", which was the
      count of identity *fields*, not commits, corrected same night),
      commit *messages*, notebooks (there are none), and secrets/tokens
      (none of any shape). Images spot-checked at **n = 4 of 18** — the
      lineage dialog plus every run-history view, i.e. the only ones
      carrying a `Run as` column — all masked, avatar redacted. **Not**
      clean: a contributor hostname in `pseudonymity.py`, a second in its
      test, and a personal email unredacted in history, at **164
      commit-file hits** (58 + 34 + 72). Tip fixed this commit; the
      `filter-repo` rewrite is the remaining half and blocks `v1.0`.
- [x] **Actor logins.** Phase 7 Task 8 built a check that fails the build
      on an actor identifier in a published artifact. Confirm it covers
      every surface that is about to become public, including the ADRs and
      findings written after it landed.
      **It did not, and this line is why the gap is now closed.** The
      check's surfaces stopped at docs, dashboards, terraform and
      `.gitignore` — `src/` and `tests/` were never scanned, so the guard
      could not see the hostname sitting in its own docstring. Widened to
      `src/**/*.py` + `tests/**/*.py` on 2026-09-09, RED watched, three
      mutations caught. The item was written correctly and left unticked
      while the repo went public; **the checklist named the gap and not
      running it is what let it through.**
- [~] **Every number still traces to its measurement.** The standing rule
      is that nothing unmeasured is ever quoted. The −97% correction
      (Phase 8 Task 3) is the known instance; check for siblings.
      **A sibling was found, in this checklist.** "All 375 commits
      authored by the GitHub noreply address" counted identity *fields*
      (`%ae` + `%ce` across all refs), not commits; the repo has **199**.
      Corrected in place here, in `docs/STATUS.md`, and in the story bank.
      A second was found in the README the same pass: *Running it* said
      **292** non-Spark tests where the suite has **340**. Marked partial
      rather than done, because "checked for siblings" is not a finite
      task and two turned up in a single evening's reading.
- [x] **`docs/limitations.md` reads as current**, including the champion's
      re-scored number and whatever Phase 8 Task 7 did or did not deliver.
      Verified 2026-09-09: §7 carries the correction block with
      `0.612 → 0.4661` and run `817800814439176`.
- [x] **README's "honest limitations" is a section, not a footnote**, and
      states the streaming-to-Gold gap, the `is_draft` ceiling, and the
      window-characterization constraint in plain terms.
      **Added 2026-09-09 as *What it does not do*** — four bullets naming
      exactly those three plus the ~7% capture rate, then the link. Kept
      to 20 lines on purpose: the same night's README work cut a 182-line
      essay for being unreadable, so satisfying this item with prose would
      have undone that.

---

## 2. The demo — what a stranger can actually see

- [x] **Decide what "the demo" is for a signed-out visitor.** Everything
      billable is torn down by design, so the artifact is the captured
      evidence, not a live endpoint. Say that explicitly rather than
      letting a reader expect a URL.
      **Decided: there is no live demo, and that is the correct answer for
      this project rather than a shortfall.** A portfolio system whose own
      cost discipline says "`terraform destroy` between working sessions"
      would contradict itself by leaving an endpoint up to be clicked. The
      evidence is the 18 committed console captures, the measured numbers
      with their run ids, and a clone-to-green path a reader can run
      locally in minutes. The README's *Running it* section is the demo.
- [x] **Screenshots render in the README for a signed-out viewer** —
      relative paths, not workspace links. Verified 2026-09-09: all six
      README image references are repo-relative `docs/images/...` paths.
- [x] **`make check-fast` on a fresh clone still meets §9's gate.**
      Measured 4m28s on 2026-09-08; re-measure after Phase 8's changes.
      **Re-measured 2026-09-09** on a fresh clone of the *rewritten*
      remote: **80 s** with a warm `uv` cache, 339 tests passing, lint and
      `mypy --strict` clean. Not comparable to the 4m28s figure, which was
      cold-cache and stays the number worth quoting; dependencies moved
      only by one lock-only transitive bump (cryptography 49→50) in
      between. Both are far inside the 15-minute gate.

---

## 3. GitHub account and profile

- [x] **Pinned repositories** — no public API; done through the UI.
      **[prior art]** Done 2026-09-09 and verified through the GraphQL
      API rather than trusted to the UI: `pinnedItems` returns 2,
      `almanac` and `canopica`.
- [x] **Profile README** — updated 2026-09-08 (`8e5c5fa`). It had said
      Almanac was *"in early implementation, and private until a model
      serves"*, which was the same stale-record failure this repo has now
      logged five times, on the most public artifact of all. Every number
      in the replacement was grepped against the repo before pushing.
- [x] **Bio** — updated 2026-09-08 and read back from the API rather than
      trusted to the write's exit code. The `[prior art]` note said this
      needs `gh auth refresh -s user` first; it did not, because the token
      already carried the `user` scope. Checked before re-authenticating.

---

## 4. Interview material

- [~] **Final story-bank regeneration**, covering Phase 8 and the flip
      itself. Story 64 added 2026-09-09 (the guard that could not see the
      file it lives in), read back and verified, gist confirmed still
      secret. The §5 architecture walkthrough still ends at Phase 7 and
      wants a Phase 8 section — partial, and said so.
- [x] **Keep the gist. Do not delete it.** Sibling precedent, decided
      explicitly. `.claude/story-bank-gist-id` stays gitignored — verified
      untracked 2026-09-08.
- [x] **Confirm the gist is still secret**, not public, after the repo
      flip. Verified 2026-09-09 via the API: `public=false`.

---

## 5. Loose ends needing an explicit decision

- [x] **`.claude/` and `CLAUDE.md` go public as-is.** Already tracked —
      hooks, `settings.json`, and four skills. The sibling project faced
      the same question and **committed them for real** rather than
      discarding: they are genuine in-use tooling and they document how
      the work was actually done. Re-read them for anything that reads as
      a private note.
      **Re-read 2026-09-09: 9 tracked files, nothing private**, and the
      two files that would be (`settings.local.json`,
      `story-bank-gist-id`) are gitignored, confirmed by
      `git status --ignored`. **And `.claude/` was then added to the
      pseudonymity guard's surfaces** — it was tracked, public, and
      unscanned, which is the identical gap that had just been found for
      `src/`. Clean when checked; the point is that the next thing written
      into it is checked too, rather than re-read by hand.
- [x] **Branch protection: `deletion` + `non_fast_forward` only.**
      **[prior art, and the important one.]** The sibling flip tried
      `required_status_checks` scoped to its always-run jobs and it
      **hard-deadlocked direct pushes** — a required check cannot exist
      for a commit that has not been pushed yet. Caught when the very next
      push was rejected. Do not repeat it. Note that `ci.yml`'s own
      comment already anticipates branch protection becoming available the
      moment this repo goes public.
- [x] **CI on a public repo.** The full suite is ~50 min on a 2-core
      hosted runner. Confirm it stays inside free-tier minutes for a
      public repo, and that the `paths-ignore` and `concurrency` behaviour
      is unchanged.
      **Confirmed 2026-09-09.** GitHub-hosted runners are unlimited for
      public repositories, so the ~50 min suite costs time and not money —
      the constraint that shaped `paths-ignore` no longer binds, and it is
      kept anyway because a docs-only push still should not re-run a suite
      it cannot affect. Both behaviours unchanged and verified by reading
      `ci.yml`: `push` filters `docs/**`, `README.md`, `CLAUDE.md`;
      `pull_request` is deliberately unfiltered so a docs-only PR still
      reports a status; `concurrency` cancels superseded runs per ref.
      Observed three times tonight, each self-inflicted by pushing into a
      live run.
- [x] **The CI badge renders for a signed-out viewer** — `200` to an
      unauthenticated request, checked for both the badge and the repo
      page. **[prior art]** Verified 2026-09-09: CI badge and the codecov
      badge both `200` unauthenticated.
- [x] **Repo visibility itself** — `gh repo edit --visibility public
      --accept-visibility-change-consequences`, last, after everything
      above. Done; confirmed `PUBLIC` with 20 topics and a description.

---

## Definition of done

- [x] Every checkbox above resolved — **done, or explicitly decided and
      recorded**, never silently skipped. The sibling flip surfaced two
      items during the pass that were not in its original list; expect the
      same and record them rather than absorbing them.
      **It surfaced three, and the prediction was right.** (1) The
      pseudonymity guard's surfaces excluded `src/` and `tests/`, so it
      could not see the hostname in its own docstring; (2) the same gap
      held for `.claude/`, found by asking where else the argument
      reached; (3) two quoted numbers were miscounts — "375 commits"
      (identity fields, not commits; the repo has 199) and "164
      commit-file hits" (166). All three recorded, none absorbed.
      **Two items remain open by decision, not oversight**, each stating
      why in place: the `system.billing.usage` idle read needs a SQL
      warehouse started, and the Azure credit *balance* is unobtainable
      from `az` on this subscription.
- [x] `docs/STATUS.md` updated in the same commit as the flip itself, per
      standing discipline. Held for every commit in the sequence.
- [x] Repo is public, CI badge renders signed-out, branch protection
      active in the `deletion` + `non_fast_forward` shape. All four
      re-verified 2026-09-09 *after* the force-push, because the rewrite
      required toggling enforcement off and the restore is the step most
      likely to be skipped.
- [x] `v1.0` tag is visible and points at the commit Phase 8 closed on:
      annotated, `44abc3b`, confirmed present in a fresh clone of the
      public remote rather than read back from the local repo that
      created it.
