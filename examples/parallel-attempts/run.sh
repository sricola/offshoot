#!/usr/bin/env bash
# Three agents try three migrations on forks of the same database.
# Two of them wreck it. The one that works gets promoted. Nothing else does.
set -euo pipefail

DIR="${OFFSHOOT_DEMO_DIR:-$(mktemp -d)}"
export OFFSHOOT_STORE="$DIR/store"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OFFSHOOT="$DIR/offshoot"

echo "==> building offshoot"
(cd "$ROOT" && go build -o "$OFFSHOOT" ./cmd/offshoot)

echo "==> creating a database with some data"
"$OFFSHOOT" init >/dev/null
"$OFFSHOOT" create shop >/dev/null
DB=$("$OFFSHOOT" checkout shop)
sqlite3 "$DB" "CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT);
               INSERT INTO orders (total) VALUES ('19.99'), ('8.70'), ('4.35');"
"$OFFSHOOT" checkpoint shop before-migration >/dev/null
echo "    3 orders, checkpoint 'before-migration'"

echo "==> keeping the pre-migration state on its own branch"
"$OFFSHOOT" fork shop pre-migration --at before-migration >/dev/null
echo "    forked 'pre-migration' from the 'before-migration' checkpoint — promote wipes main's own checkpoint history, so this fork is what actually survives"

echo "==> forking three attempts (instant, no copy)"
for i in 1 2 3; do "$OFFSHOOT" fork shop "attempt-$i" >/dev/null; done

migrate() { # $1 = attempt, $2 = SQL
  local path; path=$("$OFFSHOOT" checkout "shop@$1")
  if sqlite3 "$path" "$2" 2>/dev/null && \
     [ "$(sqlite3 "$path" "SELECT count(*) FROM orders WHERE typeof(total_cents)='integer';" 2>/dev/null)" = "3" ]; then
    "$OFFSHOOT" checkpoint "shop@$1" migrated >/dev/null
    echo "    $1: PASS"
    return 0
  fi
  echo "    $1: FAIL"
  return 1
}

echo "==> running the migrations in parallel forks"
set +e
migrate attempt-1 "ALTER TABLE orders ADD COLUMN total_cents INTEGER;
                   UPDATE orders SET total_cents = total * 100;"   # naive float math, not an integer
A1=$?
migrate attempt-2 "DROP TABLE orders;"                              # catastrophic
A2=$?
migrate attempt-3 "ALTER TABLE orders ADD COLUMN total_cents INTEGER;
                   UPDATE orders SET total_cents = CAST(ROUND(total * 100) AS INTEGER);"
A3=$?
set -e

WINNER=""
for i in 1 2 3; do
  eval "rc=\$A$i"
  if [ "$rc" = "0" ] && [ -z "$WINNER" ]; then WINNER="attempt-$i"; fi
done
[ -n "$WINNER" ] || { echo "no attempt passed"; exit 1; }
echo "==> winner: $WINNER"

echo "==> promoting the winner onto main"
"$OFFSHOOT" promote "shop@$WINNER" --onto main --force >/dev/null
echo "    promoted"

echo "==> discarding the losers"
for i in 1 2 3; do
  [ "attempt-$i" = "$WINNER" ] || "$OFFSHOOT" destroy "shop@attempt-$i" >/dev/null
done
"$OFFSHOOT" gc --grace 0s >/dev/null
"$OFFSHOOT" gc --grace 0s >/dev/null

echo "==> main now has the migrated data:"
MAIN=$("$OFFSHOOT" checkout shop)
sqlite3 -header "$MAIN" "SELECT id, total, total_cents FROM orders;" | sed 's/^/    /'
echo "==> and the pre-migration state is still one command away, on its own branch:"
echo "    offshoot checkout shop@pre-migration"
