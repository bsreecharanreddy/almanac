#!/usr/bin/env bash
# PreToolUse/Bash hook: when a commit touches source code, remind that the
# almanac-code-style skill's checklist should have been applied.
#
# Not incident-derived like this repo's other hooks -- a deliberate,
# user-directed exception (2026-09-02) to the "nothing anticipatory"
# rule, recorded as such rather than silently written as if it always
# applied. It exists for the same reason `story-bank-reminder.sh` does:
# "did I apply the checklist" is exactly the kind of thing that is
# unreliable left to memory across a long session, even though the actual
# judgment (is this simpler, is this the right abstraction) has to happen
# in the skill, not here -- this hook cannot and does not try to make
# that call.
set -euo pipefail

input="$(cat)"
cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty')"

printf '%s' "$cmd" | grep -qE '\bgit\s+commit\b' || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

staged="$(files_for_commit "$cmd")"
[ -z "$staged" ] && exit 0

# Only when actual source changed -- a docs/config-only commit has nothing
# for the checklist to apply to, and firing there would just be noise.
printf '%s\n' "$staged" | grep -qE '\.(py|sql)$' || exit 0

msg="This commit touches source code. Before committing, confirm the almanac-code-style skill's checklist was applied to what changed: simplified logic, clear naming, edge cases covered, duplication reduced, guard clauses over nested conditionals, composition over inheritance, a mapping over a long if/elif chain on a fixed set of values, a generator where the whole collection need not be materialized, and a context manager for anything acquired outside the shared Spark session."
jq -n --arg msg "$msg" '{systemMessage: $msg, hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "allow", permissionDecisionReason: $msg}}'
exit 0
