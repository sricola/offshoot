# Branch diff

offshoot doesn't merge branches — see the FAQ's [Why no
merge?](faq.md#why-no-merge) for the stance and why. What it does give you is
a fast way to see WHAT changed between two branches (or two checkpoints, or a
branch and itself over time), so you can decide by hand which fork "won."

Two ways to get there: the `offshoot diff` command below, or the raw recipe
it's a thin wrapper over — `offshoot export` (or `checkout --at`) twice, then
`sqldiff` yourself. Both end up running the exact same `sqldiff` invocation
over the exact same materialized files; the command just saves you the two
manual steps and adds a `sqldiff`-free `--summary` mode.

## `offshoot diff`

```
offshoot diff <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary]
```

Each side is parsed the same triple-`@` target form `offshoot export` uses
(`ops.ParseExportTarget`): `db` alone means `db@main`'s current head;
`db@branch` means that branch's head; `db@branch@checkpoint` means that
named checkpoint's historical state. The two sides can name the same `db` or
two entirely different ones — cross-db diff is a legitimate shape for eval
comparisons (e.g. diffing a golden dataset against a candidate that started
life as a different database).

```
offshoot fork evals attempt-1
offshoot fork evals attempt-2
# ... two agent runs write into attempt-1 and attempt-2's checkouts,
# each checkpointed when done ...
offshoot checkpoint evals@attempt-1 done
offshoot checkpoint evals@attempt-2 done

offshoot diff evals@attempt-1@done evals@attempt-2@done
```

### Default mode: `sqldiff`

With no `--summary`, `offshoot diff` materializes both sides read-only and
runs [`sqldiff`](https://www.sqlite.org/sqldiff.html) DB1 DB2, streaming its
output (SQL statements that would transform the left side into the right
side) straight to stdout.

Before either mode's own output, `offshoot diff` always prints one header
line naming which raw target string is which side — the only place either
mode ever repeats what you typed, so it's the one anchor tying "left"/
"right" (or, in `--summary`, a bare row-count column) back to an actual
`db@branch[@checkpoint]`:

```
left:  evals@attempt-1@done right: evals@attempt-2@done
```

**`sqldiff` is a separate binary from the `sqlite3` CLI** — installing
`sqlite3` alone does not put `sqldiff` on PATH on every platform. If it's
missing, `offshoot diff` fails with a clear, verified-per-OS install hint
rather than a bare "executable file not found":

```
offshoot diff: sqldiff not found on PATH

sqldiff ships separately from the sqlite3 CLI. Install it:
  sudo apt-get install sqlite3-tools   # Debian/Ubuntu: sqldiff ships in this separate package

Or skip sqldiff entirely with 'offshoot diff ... --summary' for a
table-level row-count comparison instead
```

On macOS, the hint is `brew install sqldiff` — verified against a real
install on this project's own dev machine that Homebrew's general `sqlite`
formula (keg-only, and already installed as a dependency of other things
more often than not) does **not** include `sqldiff`; Homebrew ships it as
its own separate formula instead, and installing it puts a working
`sqldiff` straight on PATH (no keg-only PATH surgery needed, unlike the
`sqlite` formula itself).

### `--summary`: no `sqldiff` needed

```
offshoot diff evals@attempt-1@done evals@attempt-2@done --summary
```

```
left:  evals@attempt-1@done right: evals@attempt-2@done
TABLE     evals@attempt-1@done  evals@attempt-2@done  STATUS
attempts  12                    12                    same
results   40                    46                    changed (+6)
scratch   3                     -                     removed
3 tables: 1 same, 1 changed, 0 added, 1 removed
```

(real output, from a store seeded exactly as the walkthrough above describes
— `evals@attempt-1` untouched from a 12/40/3-row base, `evals@attempt-2`
with 6 more `results` rows and `scratch` dropped entirely.)

`--summary` never shells out to `sqldiff` (or any external binary at all) —
it lists both sides' tables from `sqlite_master` and counts rows per table
via `database/sql` + the `mattn/go-sqlite3` driver this binary is already
built with, opened strictly read-only (`file:<path>?mode=ro&immutable=1` —
verified against a `chmod 0444` file, the exact permission `checkout --at
--read-only`/the internal read-only cache produce). Useful when `sqldiff`
isn't installed, or when you just want a quick "did anything change, and
roughly how much" answer before reaching for the full SQL diff.

Each table gets one row: its name, each side's row count (`-` when the
table doesn't exist on that side at all), and a status — `added`/`removed`
for a table on only one side, `changed (+N)`/`changed (-N)` for a
differing row count, `same` otherwise. A trailing totals line answers "did
anything change" without counting rows of output. The two count columns are
headered with the raw target strings themselves (not bare `LEFT`/`RIGHT`),
matching the `left: ... right: ...` line above — a reader scanning just the
table never has to scroll back up to know which count is which side.

**Note:** `--summary` is a row-count diff, not a content diff — a table
with the same row count on both sides but different *values* still reports
`same`. Reach for the default `sqldiff` mode when you need to know exactly
which rows changed.

### How each side is materialized (and the staleness rule)

Both modes materialize through the exact same **read-only** primitives
`offshoot export`/`offshoot checkout --at --read-only` already provide (see
[docs/reference.md](reference.md)) — `offshoot diff` never opens or writes
through a live checkout, and never takes a lease, so it's always safe to run
alongside an open daemon session on either branch.

- **A named checkpoint** (`db@branch@checkpoint`) is materialized via the
  read-only `checkouts-ro` cache (the same one `checkout --at --read-only`
  uses): a checkpoint's content is immutable once created, so a repeat diff
  against the same checkpoint is a cheap cache hit, not a re-materialize.
- **A bare target with no checkpoint** (`db@branch`, meaning the branch's
  current **head**) is exported fresh to a private temporary file on
  *every* `offshoot diff` call, then removed once the diff is done. Head
  moves — a head-keyed entry in the read-only cache could never be an
  idempotent cache the way a checkpoint-keyed one can be, because "the same
  cache key" would silently start meaning "whatever head was at the last
  diff," not "head right now." Rather than teach the checkout-at cache an
  asymmetric "force by default, but only for head" special case, a head
  side is simply never cached: run `offshoot diff app@main app@main@v1`
  twice with a write to `app@main` in between (and a `offshoot checkpoint`
  to make that write durable — diff reads the branch's last **durable**
  state, exactly like `export` does, not a live session's unflushed
  writes) and the second call reflects the new write every time.

## The raw recipe, by hand

`offshoot diff` is not doing anything you can't do yourself with two
commands you already have:

```
offshoot export app@attempt-1@v1 /tmp/left.db
offshoot export app@attempt-2@v1 /tmp/right.db
sqldiff /tmp/left.db /tmp/right.db
```

Or, for a live branch's current head, materialize a read-only historical
checkout instead of exporting (skips the write-to-a-temp-file step, at the
cost of leaving a cache entry behind under `checkouts-ro`):

```
offshoot checkout app@attempt-1 --at v1 --read-only
offshoot checkout app@attempt-2 --at v1 --read-only
sqldiff .offshoot/checkouts-ro/app/attempt-1@v1.db .offshoot/checkouts-ro/app/attempt-2@v1.db
```

Both `export` and `checkout --at --read-only` produce a plain SQLite file
with zero ongoing relationship to the store (no `.sum` sidecar, no lease) —
`sqldiff`, or any other tool that reads SQLite files, doesn't need to know
offshoot exists at all.

## No merge

`offshoot diff` is a *read* tool: it tells you what's different, it never
resolves anything. There is no `offshoot merge`, and there won't be one —
see the FAQ's [Why no merge?](faq.md#why-no-merge) for the full reasoning.
Forks are for pick-a-winner: diff to decide, then `offshoot promote` the one
that's actually correct.
