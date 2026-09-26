#!/usr/bin/env bash
# SessionStart: if offshoot is installed and a store exists, tell Claude what
# databases and branches are there so it forks before risky work instead of
# discovering offshoot mid-task. Silent (exit 0, no output) otherwise —
# an advisory hook must never break a session.
set -u
command -v offshoot >/dev/null 2>&1 || exit 0
input=$(cat 2>/dev/null || true)
cwd=$(printf '%s' "$input" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("cwd",""))' 2>/dev/null || true)
store="${OFFSHOOT_STORE:-${cwd:-.}/.offshoot}"
case "$store" in
  s3://*|file://*) ;;
  *) [ -d "$store" ] || exit 0 ;;
esac
status=$(OFFSHOOT_STORE="$store" offshoot status 2>/dev/null | head -40) || exit 0
[ -n "$status" ] || exit 0
python3 - "$store" "$status" <<'EOF'
import json, sys
store, status = sys.argv[1], sys.argv[2]
ctx = (
    "offshoot (SQLite branching) is available as MCP tools for the store at "
    f"{store}. Current branches:\n{status}\n"
    "Before a schema migration, bulk delete, or any experiment on a database: "
    "offshoot_fork first and work in the fork; offshoot_checkpoint when tests pass; "
    "offshoot_rollback when they fail; offshoot_promote the fork that worked. "
    "/rewind restores Claude's file edits only, not changes a Bash command made to "
    "a database — use offshoot_rollback for those."
)
print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx}}))
EOF
exit 0
