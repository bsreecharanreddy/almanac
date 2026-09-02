#!/usr/bin/env bash
# Shared helpers for this repo's PreToolUse/Bash git-commit hooks.
# Source, don't execute: `source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"`.
#
# Extracted when a third hook needed the same logic a second copy had
# already duplicated -- two was tolerable, three was the actual
# duplication this repo's own conventions ask not to carry.

# Files this commit will actually contain. If the same command also
# stages (`git add`), the index is still empty at PreToolUse time -- the
# hook runs *before* the command does -- so fall back to the working
# tree. Found for real: this repo's first three commits were all made
# with `git add -A && git commit`, and no hook fired on any of them.
files_for_commit() {
  local cmd="$1" staged
  staged="$(git diff --cached --name-only 2>/dev/null || true)"
  if printf '%s' "$cmd" | grep -qE '\bgit\s+add\b'; then
    printf '%s\n%s\n' "$staged" "$(git status --porcelain 2>/dev/null | sed 's/^...//')"
  else
    printf '%s\n' "$staged"
  fi | sed '/^$/d' | sort -u
}
