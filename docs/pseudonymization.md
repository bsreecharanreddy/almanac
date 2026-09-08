# Pseudonymization — what is masked, and what the check cannot see

Design doc §10: *no actor identity in any published artifact*. This document
states the rule; `tests/unit/test_governance_pseudonymity.py` enforces it on
every test run, and `src/almanac/governance/pseudonymity.py` is the check.

## The rule

**A natural person's identity never appears in a published artifact.** That
covers the README, `CLAUDE.md`, everything under `docs/` (including generated
lineage JSON and dashboard definitions), the Terraform under `infra/`, and the
dotfiles a public repo also publishes.

Three categories, treated differently:

| Category | Rule | Why |
|---|---|---|
| **Human GitHub logins** (`actor_login`, `author_login` values) | Never published. Column *names* are fine; values are not. | They identify real people who never opted into this project |
| **Bot logins** (`dependabot`, `renovate`, `github-actions`) | Published freely | Not natural persons. §5's bot-detection work is unreportable otherwise, and 19 such mentions already exist |
| **Repository full names** (`owner/repo`) | Not published, because the owner half is usually a person | The dimension carries them; the artifacts do not |

**Machine and account identity is masked too**, not just dataset identity. The
maintainer's OS login and hostname are as personal as any `actor_login`, and
this project has already paid for that lesson: a commit trailer carrying a
machine name that contained a personal name required a `git filter-repo`
rewrite across 126 commits to remove (Phase 6, 2026-09-06).

## What the dashboards display

Page 3 renders contributor data by design, and `dim_repo` carries real repo and
actor names, so the masking happens at the display layer:

- **Aggregate over identity, never list it.** Contributor concentration and
  bus-factor ranking are computed from `actor_login` and rendered as *counts,
  shares and ranks* — "the top contributor accounts for 34% of PRs", never
  "`<login>` accounts for 34%".
- **Where a row must be identified**, it carries a stable surrogate
  (`repo_id`, or a rank position), not a name.
- **Bot/human is a toggle**, which is a property, not an identity.

## What the check catches

Run on every `pytest` invocation, including `make check-fast`:

1. **Email addresses**, excluding three deliberate non-identities: ADLS storage
   URIs (`abfss://bronze@account.dfs.core.windows.net` matches an email regex
   and is not one), `@users.noreply.github.com` — the pseudonymous address this
   project rewrote its history *to* — and `@example.com`.
2. **This machine's own identifiers**, derived at scan time from the hostname
   and OS login, matched on whole words only.

**The identifiers are derived, never committed.** A denylist of personal
identifiers would have to contain the exact strings it exists to keep out of
the repo. Reading them from the host instead means the check protects whoever
is working, on their own machine, without anyone's name entering git.

It found four real leaks on its first run, in three files plus a `.gitignore`
comment written the same day — in the rule that existed to stop that hostname
leaking. Two were in a postmortem that quoted the identifier it was
documenting the removal of.

## What it cannot see

Stated here rather than left implied, the same discipline `docs/lineage/` uses.

1. **Image contents.** `docs/images/` holds ten screenshots and this check reads
   only text. A console screenshot showing `Run as` or `Created by` is a real
   leak and no regex will find it. Those are masked by hand —
   `docs/findings/2026-09-06-console-evidence.md` already does — and that
   remains a manual review step at the moment a screenshot is added.
2. **A human login that looks like an ordinary word.** The check cannot
   distinguish a GitHub login from prose. It relies on the rule above being
   followed when artifacts are written, and catches only the machine-identity
   and email classes mechanically.
3. **Anything already in git history.** The check reads the working tree. A
   leak that was committed and later masked is still recoverable from history,
   which is why Phase 6's fix was a history rewrite and not an edit.
4. **Data files.** Bronze, Silver, Gold and the fixtures all carry real logins
   by design — they are the dataset. Nothing under `data/` or `tests/fixtures/`
   is a published artifact, and none of it is committed.
