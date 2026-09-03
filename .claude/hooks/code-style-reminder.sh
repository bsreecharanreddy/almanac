#!/usr/bin/env bash
# PreToolUse/Bash: on a commit touching .py/.sql, remind that the
# almanac-code-style checklist should have been applied. User-directed
# (2026-09-02), not incident-derived -- see the skill's own preamble.
set -euo pipefail

input="$(cat)"
cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty')"

printf '%s' "$cmd" | grep -qE '\bgit\s+commit\b' || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

staged="$(files_for_commit "$cmd")"
[ -z "$staged" ] && exit 0

# Source-only: a docs/config commit has nothing for the checklist to apply to.
printf '%s\n' "$staged" | grep -qE '\.(py|sql)$' || exit 0

msg="This commit touches source code. Confirm the almanac-code-style checklist was applied to what changed: simplified logic, clear naming, edge cases covered, duplication reduced, guard clauses over nested conditionals, composition over inheritance, a mapping over a long if/elif chain on a fixed value set, a generator where the collection need not be materialized, a context manager for anything acquired outside the shared Spark session, and comment/docstring discipline (one-line docstrings, why-not-what comments, no narrative paragraphs or measured-number essays in code -- that story lives in docs/)."
jq -n --arg msg "$msg" '{systemMessage: $msg, hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "allow", permissionDecisionReason: $msg}}'
exit 0
