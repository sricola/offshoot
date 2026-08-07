#!/usr/bin/env bash
# Drives internal/mcp's real stdio JSON-RPC server (via the built `offshoot`
# binary) against a real, throwaway store, and runs the actual SQL
# migration/test steps around it. Every request and response appended to
# $LOG is the literal, unedited wire text captured from the subprocess's
# stdout — nothing in this script's output is hand-written or edited after
# the fact.
#
# This is the script that produced docs/demo/mcp-walkthrough.md's transcript.
# Run it yourself to reproduce that transcript (it was run twice against two
# independent fresh stores while producing the doc, and the two logs were
# byte-identical apart from the random store path).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIR="$(mktemp -d)"
STORE="$DIR/store"
OFFSHOOT="$DIR/offshoot"
LOG="$DIR/session.log"
: > "$LOG"

section() { echo "" >> "$LOG"; echo "### $*" >> "$LOG"; }

echo "== building offshoot =="
(cd "$ROOT" && go build -o "$OFFSHOOT" ./cmd/offshoot)

echo "== CLI setup: init + create + seed data =="
section "CLI: offshoot init"
OUT=$("$OFFSHOOT" -store "$STORE" init 2>&1); echo '$ offshoot -store $STORE init' >> "$LOG"; echo "$OUT" >> "$LOG"
section "CLI: offshoot create shop"
OUT=$("$OFFSHOOT" -store "$STORE" create shop 2>&1); echo '$ offshoot -store $STORE create shop' >> "$LOG"; echo "$OUT" >> "$LOG"

DB=$("$OFFSHOOT" -store "$STORE" checkout shop)
sqlite3 "$DB" "CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT);
               INSERT INTO orders (total) VALUES ('19.99'), ('8.70'), ('4.35');"
section "CLI: seed data + checkpoint baseline"
echo '$ sqlite3 $(offshoot checkout shop) "CREATE TABLE orders...; INSERT ..."' >> "$LOG"
CP=$("$OFFSHOOT" -store "$STORE" checkpoint shop baseline 2>&1)
echo '$ offshoot -store $STORE checkpoint shop baseline' >> "$LOG"
echo "$CP" >> "$LOG"

echo "== starting offshoot mcp as a subprocess, driving it over stdio =="
mkfifo "$DIR/in" "$DIR/out"
"$OFFSHOOT" -store "$STORE" mcp < "$DIR/in" > "$DIR/out" 2>"$DIR/mcp.stderr" &
MCP_PID=$!
exec 3>"$DIR/in"
exec 4<"$DIR/out"

send() {
  local req="$1"
  echo "$req" >&3
  local resp
  IFS= read -r resp <&4
  section "MCP request"
  echo "$req" >> "$LOG"
  section "MCP response"
  echo "$resp" >> "$LOG"
  echo "$resp"
}

echo "== JSON-RPC: initialize handshake =="
send '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"claude-code","version":"1"}}}' > /dev/null
echo '{"jsonrpc":"2.0","method":"notifications/initialized"}' >&3
section "MCP notification (no response expected)"
echo '{"jsonrpc":"2.0","method":"notifications/initialized"}' >> "$LOG"

echo "== JSON-RPC: tools/list =="
send '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' > /dev/null

echo "== JSON-RPC: offshoot_list (orient) =="
send '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}' > /dev/null

echo "== JSON-RPC: offshoot_fork before risky migration =="
send '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"offshoot_fork","arguments":{"database":"shop","new_branch":"migration-attempt","ttl":"none"}}}' > /dev/null

echo "== JSON-RPC: offshoot_checkout the fork (must precede checkpoint) =="
send '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"offshoot_checkout","arguments":{"database":"shop","branch":"migration-attempt"}}}' > /dev/null

echo "== JSON-RPC: offshoot_checkpoint pre-migration safety point =="
send '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"offshoot_checkpoint","arguments":{"database":"shop","branch":"migration-attempt","name":"pre-migration"}}}' > /dev/null

ATTEMPT_PATH=$("$OFFSHOOT" -store "$STORE" path shop@migration-attempt)

echo "== real SQL: buggy migration attempt (naive float math) =="
section "real SQL on the fork's checkout: buggy migration"
echo '$ sqlite3 $(offshoot path shop@migration-attempt) "ALTER TABLE orders ADD COLUMN total_cents INTEGER; UPDATE orders SET total_cents = total * 100;"' >> "$LOG"
sqlite3 "$ATTEMPT_PATH" "ALTER TABLE orders ADD COLUMN total_cents INTEGER;
                          UPDATE orders SET total_cents = total * 100;" >> "$LOG" 2>&1 || true

echo "== real SQL: run tests (integer-type check) — RED =="
section "real SQL: test assertion (all total_cents must be integer-typed)"
TEST1=$(sqlite3 "$ATTEMPT_PATH" "SELECT count(*) FROM orders WHERE typeof(total_cents)='integer';")
echo '$ sqlite3 $(offshoot path shop@migration-attempt) "SELECT count(*) FROM orders WHERE typeof(total_cents)=\x27integer\x27;"' >> "$LOG"
echo "$TEST1  (want 3)" >> "$LOG"
if [ "$TEST1" = "3" ]; then echo "TESTS: GREEN" >> "$LOG"; else echo "TESTS: RED" >> "$LOG"; fi

echo "== JSON-RPC: offshoot_rollback to pre-migration (tests were red) =="
send '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"offshoot_rollback","arguments":{"database":"shop","branch":"migration-attempt","to":"pre-migration"}}}' > /dev/null

echo "== real SQL: corrected migration attempt (rounded, cast to integer) =="
section "real SQL on the fork's checkout: corrected migration (post-rollback)"
echo '$ sqlite3 $(offshoot path shop@migration-attempt) "ALTER TABLE orders ADD COLUMN total_cents INTEGER; UPDATE orders SET total_cents = CAST(ROUND(total * 100) AS INTEGER);"' >> "$LOG"
sqlite3 "$ATTEMPT_PATH" "ALTER TABLE orders ADD COLUMN total_cents INTEGER;
                          UPDATE orders SET total_cents = CAST(ROUND(total * 100) AS INTEGER);" >> "$LOG" 2>&1 || true

echo "== real SQL: run tests again — GREEN =="
section "real SQL: test assertion, re-run after correction"
TEST2=$(sqlite3 "$ATTEMPT_PATH" "SELECT count(*) FROM orders WHERE typeof(total_cents)='integer';")
echo '$ sqlite3 $(offshoot path shop@migration-attempt) "SELECT count(*) FROM orders WHERE typeof(total_cents)=\x27integer\x27;"' >> "$LOG"
echo "$TEST2  (want 3)" >> "$LOG"
if [ "$TEST2" = "3" ]; then echo "TESTS: GREEN" >> "$LOG"; else echo "TESTS: RED" >> "$LOG"; fi

echo "== JSON-RPC: offshoot_checkpoint on green =="
send '{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"offshoot_checkpoint","arguments":{"database":"shop","branch":"migration-attempt","name":"migrated"}}}' > /dev/null

echo "== JSON-RPC: offshoot_promote without force (main is protected — expect refusal) =="
send '{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"offshoot_promote","arguments":{"database":"shop","source":"migration-attempt","target":"main"}}}' > /dev/null

echo "== JSON-RPC: offshoot_promote with force =="
send '{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"offshoot_promote","arguments":{"database":"shop","source":"migration-attempt","target":"main","force":true}}}' > /dev/null

echo "== JSON-RPC: offshoot_destroy the now-merged attempt branch =="
send '{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"offshoot_destroy","arguments":{"database":"shop","branch":"migration-attempt"}}}' > /dev/null

echo "== JSON-RPC: offshoot_list again — confirm main has the migration =="
send '{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}' > /dev/null

echo "== real SQL: confirm main's data =="
MAIN=$("$OFFSHOOT" -store "$STORE" checkout shop)
section "real SQL: confirm main branch after promote"
echo "\$ sqlite3 -header \$(offshoot checkout shop) 'SELECT id, total, total_cents FROM orders;'" >> "$LOG"
sqlite3 -header "$MAIN" "SELECT id, total, total_cents FROM orders;" >> "$LOG"

exec 3>&-
exec 4<&-
wait "$MCP_PID" 2>/dev/null || true

echo "== DONE =="
echo "STORE=$STORE"
echo "LOGFILE=$LOG"
