#!/usr/bin/env bash
# Validates the Claude Code plugin bundle: every JSON manifest parses and
# has its required fields, the skill has frontmatter, hook scripts are
# executable and exit 0 with no output when offshoot is absent (advisory
# hooks must never break a session). Run by `make check-plugin` and CI.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
fail() { echo "check-plugin: $*" >&2; exit 1; }

python3 - <<'EOF' || fail "manifest validation failed"
import json, sys
mk = json.load(open(".claude-plugin/marketplace.json"))
assert mk["name"] == "offshoot", "marketplace name"
assert mk["plugins"][0]["name"] == "offshoot" and mk["plugins"][0]["source"] == "./plugin", "marketplace plugin entry"
pl = json.load(open("plugin/.claude-plugin/plugin.json"))
for k in ("name", "version", "description", "author", "homepage", "repository", "license"):
    assert k in pl, f"plugin.json missing {k}"
assert pl["name"] == "offshoot"
mcp = json.load(open("plugin/.mcp.json"))
assert mcp["offshoot"]["command"] == "offshoot" and mcp["offshoot"]["args"] == ["mcp"], ".mcp.json server"
hooks = json.load(open("plugin/hooks/hooks.json"))["hooks"]
events = set(hooks)
assert events == {"SessionStart", "PreToolUse"}, f"hook events {events}"
for h in hooks["PreToolUse"]:
    assert h["matcher"] == "Bash", "PreToolUse must match Bash only"
EOF

head -1 plugin/skills/offshoot/SKILL.md | grep -qx -- '---' || fail "SKILL.md must start with frontmatter"
grep -q '^description:' plugin/skills/offshoot/SKILL.md || fail "SKILL.md frontmatter needs description"

for s in plugin/hooks/session-start.sh plugin/hooks/pre-bash-sql-guard.sh; do
  [ -x "$s" ] || fail "$s must be executable"
  bash -n "$s" || fail "$s has a syntax error"
  # With offshoot absent from PATH and no store, hooks must be silent and succeed.
  out=$(echo '{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"sqlite3 app.db \"DROP TABLE t\""},"cwd":"/nonexistent"}' \
    | env -i PATH=/usr/bin:/bin HOME=/tmp bash "$s") || fail "$s exited non-zero without offshoot"
  [ -z "$out" ] || fail "$s must print nothing when offshoot is not installed, got: $out"
done
echo "check-plugin: ok"
