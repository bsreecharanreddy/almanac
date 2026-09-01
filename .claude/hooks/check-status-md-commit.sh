#!/usr/bin/env bash
# PreToolUse/Bash hook: warns before a `git commit` if files outside docs/
# are staged without docs/STATUS.md, since CLAUDE.md requires STATUS.md to
# update "in the same commit as the work it describes -- never as a
# separate follow-up commit, or it drifts and stops being trustworthy."
#
# Non-blocking by design: it warns, it does not refuse. Ported from a prior
# project where this exact drift happened for real; the convention is
# restated in this repo's CLAUDE.md, so the control comes with it.
set -euo pipefail

input="$(cat)"
cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty')"

printf '%s' "$cmd" | grep -qE '\bgit\s+commit\b' || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

staged="$(git diff --cached --name-only 2>/dev/null || true)"
[ -z "$staged" ] && exit 0

# STATUS.md already staged, or nothing outside docs/ changed -> nothing to say.
printf '%s\n' "$staged" | grep -q '^docs/STATUS\.md$' && exit 0
printf '%s\n' "$staged" | grep -qv '^docs/' || exit 0

msg="docs/STATUS.md is not staged, but other files are. CLAUDE.md requires STATUS.md to update in the same commit as the work it describes -- confirm this commit doesn't need a STATUS.md update (a task done, a verification result, or a new decision) before proceeding."
jq -n --arg msg "$msg" '{systemMessage: $msg, hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "allow", permissionDecisionReason: $msg}}'
exit 0
