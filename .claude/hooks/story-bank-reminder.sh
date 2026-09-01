#!/usr/bin/env bash
# PreToolUse/Bash hook: when a commit stages docs/STATUS.md -- this repo's
# marker for "a task just closed out" -- remind that the interview story
# bank gist may need the story before the moment is lost.
#
# Purely incident-derived, and the most-earned control in this repo. On a
# prior project the equivalent trigger was documented failing to self-fire
# on *every* occasion across roughly a dozen sessions; the story bank was
# only ever updated when the user asked directly. Care demonstrably did not
# work, so this is mechanized instead.
#
# Fires only on STATUS.md commits, not on every commit, so it stays
# low-noise -- a reminder that fires constantly is a reminder that gets
# ignored, which is the failure mode it exists to prevent.
set -euo pipefail

input="$(cat)"
cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty')"

printf '%s' "$cmd" | grep -qE '\bgit\s+commit\b' || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

staged="$(git diff --cached --name-only 2>/dev/null || true)"
printf '%s\n' "$staged" | grep -q '^docs/STATUS\.md$' || exit 0

gist_id="$(cat .claude/story-bank-gist-id 2>/dev/null || true)"
target="the interview story bank gist"
[ -n "$gist_id" ] && target="gist $gist_id"

msg="A task is closing out (docs/STATUS.md staged). Did anything here earn a story-bank entry -- a debugging saga root-caused, a decision made and defended, a scope call, a measured result that contradicted an assumption? If so add it to $target now, in STAR format, while the detail is fresh. This trigger is known to be unreliable when left to memory, which is why it is a hook."
jq -n --arg msg "$msg" '{systemMessage: $msg, hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "allow", permissionDecisionReason: $msg}}'
exit 0
