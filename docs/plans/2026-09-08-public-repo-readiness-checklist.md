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

- [ ] **Nothing billable is running.** `terraform state list` in both root
      modules. `databricks_model_serving.pr_review_sla_risk` **is
      currently up** — it scales to zero and drew zero DBUs on 09-07, so
      it is not costing anything idle, but "believed to scale to zero" and
      "measured at zero" are different claims and only the second one goes
      in a README.
- [ ] **Azure credit expiry 2026-09-24.** Confirm the balance and whether
      anything still draws on it. Design doc records "credit expiry is a
      budget, not a wall" — restate the position rather than discovering
      it.
- [ ] **Re-read the idle rates the day after the Phase 8 window.**
      `system.billing.usage` lags ~24h and cannot be queried during the
      window it measures.

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
      *metadata* (all 375 commits authored by the GitHub noreply address),
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
- [ ] **Every number still traces to its measurement.** The standing rule
      is that nothing unmeasured is ever quoted. The −97% correction
      (Phase 8 Task 3) is the known instance; check for siblings.
- [ ] **`docs/limitations.md` reads as current**, including the champion's
      re-scored number and whatever Phase 8 Task 7 did or did not deliver.
- [ ] **README's "honest limitations" is a section, not a footnote**, and
      states the streaming-to-Gold gap, the `is_draft` ceiling, and the
      window-characterization constraint in plain terms.

---

## 2. The demo — what a stranger can actually see

- [ ] **Decide what "the demo" is for a signed-out visitor.** Everything
      billable is torn down by design, so the artifact is the captured
      evidence, not a live endpoint. Say that explicitly rather than
      letting a reader expect a URL.
- [ ] **Screenshots render in the README for a signed-out viewer** —
      relative paths, not workspace links.
- [ ] **`make check-fast` on a fresh clone still meets §9's gate.**
      Measured 4m28s on 2026-09-08; re-measure after Phase 8's changes.

---

## 3. GitHub account and profile

- [ ] **Pinned repositories** — no public API; done through the UI.
      **[prior art]**
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

- [ ] **Final story-bank regeneration**, covering Phase 8 and the flip
      itself.
- [ ] **Keep the gist. Do not delete it.** Sibling precedent, decided
      explicitly. `.claude/story-bank-gist-id` stays gitignored — verified
      untracked 2026-09-08.
- [ ] **Confirm the gist is still secret**, not public, after the repo
      flip.

---

## 5. Loose ends needing an explicit decision

- [ ] **`.claude/` and `CLAUDE.md` go public as-is.** Already tracked —
      hooks, `settings.json`, and four skills. The sibling project faced
      the same question and **committed them for real** rather than
      discarding: they are genuine in-use tooling and they document how
      the work was actually done. Re-read them for anything that reads as
      a private note.
- [ ] **Branch protection: `deletion` + `non_fast_forward` only.**
      **[prior art, and the important one.]** The sibling flip tried
      `required_status_checks` scoped to its always-run jobs and it
      **hard-deadlocked direct pushes** — a required check cannot exist
      for a commit that has not been pushed yet. Caught when the very next
      push was rejected. Do not repeat it. Note that `ci.yml`'s own
      comment already anticipates branch protection becoming available the
      moment this repo goes public.
- [ ] **CI on a public repo.** The full suite is ~50 min on a 2-core
      hosted runner. Confirm it stays inside free-tier minutes for a
      public repo, and that the `paths-ignore` and `concurrency` behaviour
      is unchanged.
- [ ] **The CI badge renders for a signed-out viewer** — `200` to an
      unauthenticated request, checked for both the badge and the repo
      page. **[prior art]**
- [ ] **Repo visibility itself** — `gh repo edit --visibility public
      --accept-visibility-change-consequences`, last, after everything
      above.

---

## Definition of done

- [ ] Every checkbox above resolved — **done, or explicitly decided and
      recorded**, never silently skipped. The sibling flip surfaced two
      items during the pass that were not in its original list; expect the
      same and record them rather than absorbing them.
- [ ] `docs/STATUS.md` updated in the same commit as the flip itself, per
      standing discipline.
- [ ] Repo is public, CI badge renders signed-out, branch protection
      active in the `deletion` + `non_fast_forward` shape.
- [ ] `v1.0` tag is visible and points at the commit Phase 8 closed on.
