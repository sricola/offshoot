#!/usr/bin/env bash
# PreToolUse(Bash): when a command looks like destructive SQL against a
# SQLite file (sqlite3 ... DROP/DELETE/UPDATE/ALTER/TRUNCATE), remind Claude
# to checkpoint or work in a fork. Advisory only: never denies, never blocks,
# and stays silent when offshoot is not installed.
set -u
command -v offshoot >/dev/null 2>&1 || exit 0
input=$(cat 2>/dev/null || true)
python3 - "$input" <<'EOF'
import json, re, sys
try:
    data = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)
cmd = (data.get("tool_input") or {}).get("command") or ""
if not re.search(r"sqlite3|\.db\b|\.sqlite\b", cmd):
    sys.exit(0)
if not re.search(r"\b(DROP|DELETE|TRUNCATE|ALTER|UPDATE)\b", cmd, re.I):
    sys.exit(0)
ctx = (
    "This Bash command runs destructive SQL against a SQLite file. If that file is an "
    "offshoot checkout, take offshoot_checkpoint first (or run the command in an "
    "offshoot_fork) so the change can be undone with offshoot_rollback — /rewind "
    "does not undo Bash-made database changes."
)
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": ctx}}))
EOF
exit 0
