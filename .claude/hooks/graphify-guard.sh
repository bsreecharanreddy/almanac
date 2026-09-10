#!/bin/sh
# Nudges Claude toward `graphify query` instead of raw grep/read, when a
# graph exists. Portable: no machine-specific path baked in, so a clone
# without graphify installed sees this hook no-op rather than error --
# `graphify claude install` writes an absolute local binary path into this
# repo's tracked .claude/settings.json, which breaks (or errors noisily)
# for every other clone. This script is the fix: resolve the interpreter
# the same way the git hooks already do, and exit 0 quietly if it's absent.

mode="$1"

GRAPHIFY_BIN=$(command -v graphify 2>/dev/null)
if [ -n "$GRAPHIFY_BIN" ]; then
    exec "$GRAPHIFY_BIN" hook-guard "$mode"
fi

PYTHON_FILE="${CLAUDE_PROJECT_DIR:-.}/graphify-out/.graphify_python"
if [ -f "$PYTHON_FILE" ]; then
    GRAPHIFY_PYTHON=$(cat "$PYTHON_FILE" 2>/dev/null)
    if [ -n "$GRAPHIFY_PYTHON" ] && [ -x "$GRAPHIFY_PYTHON" ]; then
        exec "$GRAPHIFY_PYTHON" -m graphify hook-guard "$mode"
    fi
fi

exit 0
