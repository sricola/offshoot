# Quickstart

Five minutes, no server, no bucket: seed a database, fork it, trash the
fork, prove the original never noticed, and undo the damage. You need an
[installed `offshoot` binary](installation.md) and the `sqlite3` CLI.
Every command and every output line below is from a real run of this
revision — lines starting with `$` are what you type; the rest is what
comes back.

## 1. Create a store and a database

```
$ offshoot init
initialized store at .offshoot
$ offshoot create demo
```

`init` creates the store — a plain directory, `./.offshoot` by default —
and `create` makes a database named `demo` with a `main` branch, protected
by default (`create` confirms with one line).

## 2. Check out and seed it

`checkout` materializes a branch to a real SQLite file and prints the
path. Capture it in a shell variable — it's a plain `.db` file any SQLite
tool opens:

```
$ MAIN=$(offshoot checkout demo)
$ echo "$MAIN"
.offshoot/checkouts/demo/main.db
$ sqlite3 "$MAIN" "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('ada'), ('grace'), ('edsger');"
```

## 3. Checkpoint the seeded state

```
$ offshoot checkpoint demo seeded
checkpoint "seeded" at txid 2 (segment, 2 pages, 0.4 KiB)
```

A checkpoint is a named state you can fork from or roll back to later.
The part in parentheses is what it stored: here a *segment* of the two
pages that changed since `checkout`, diffed against a reflinked shadow of
the checkout. On a filesystem that cannot clone (ext4, tmpfs) there is no
shadow, and the line reads `(snapshot, … MiB)` — the whole database —
instead.

## 4. Fork an experiment branch

```
$ offshoot fork demo experiment
forked demo@main -> demo@experiment at txid 2
```

This returns immediately: a fork is copy-on-write — it shares the
parent's already-durable storage through a base pointer instead of copying
anything. Measured: a shared fork of a 100 MB database adds
[377 bytes](benchmarks.md#added-object-store-bytes-per-fork-100-mb-database)
to the store, and forking a named checkpoint takes ~9–10 ms whether the
database is 12 MB or 1 GB
([benchmarks](benchmarks.md#shared-fork-latency-vs-database-size)).

## 5. Do something destructive — on the fork

```
$ EXP=$(offshoot checkout demo@experiment)
$ sqlite3 "$EXP" "DELETE FROM users;"
```

## 6. Prove the isolation

```
$ sqlite3 "$EXP" "SELECT COUNT(*) FROM users;"
0
$ sqlite3 "$MAIN" "SELECT COUNT(*) FROM users;"
3
```

The fork is empty; `main` never noticed. `status` shows every branch's
state and storage class — `experiment` is `dirty` (un-checkpointed local
edits) and `shared` (a copy-on-write fork, near-zero bytes of its own):

```
$ offshoot status
demo@experiment state=dirty storage=shared txid=2 checkpoints=[fork] checked-out
demo@main state=idle storage=materialized txid=2 checkpoints=[init,seeded] protected checked-out
ro-cache: 2 entries, 12288 bytes used (budget: unlimited)
```

## 7. Diff the two branches

`diff` compares **durable** state (what's in the store), not local
un-checkpointed edits — so checkpoint the experiment first, then diff.
`--summary` needs no extra tools (the default mode streams `sqldiff`
instead):

```
$ offshoot checkpoint demo@experiment wiped
checkpoint "wiped" at txid 3 (segment, 1 pages, 0.2 KiB)
$ offshoot diff demo demo@experiment --summary
left:  demo right: demo@experiment
TABLE  demo  demo@experiment  ADDED  REMOVED  CHANGED  STATUS
users  3     0                0      3        0        changed
1 tables: 0 same, 1 changed, 0 added, 0 removed
```

## 8. Undo it

Every fork gets an auto-created `fork` checkpoint at its fork point.
Roll the experiment back to it:

```
$ offshoot rollback demo@experiment --to fork
.offshoot/checkouts/demo/experiment.db
rolled back demo@experiment to "fork" (shared)
kept the previous demo@experiment head as demo@experiment-pre-rollback (expires in 24h0m0s; undo with: offshoot promote demo@experiment-pre-rollback --onto experiment --force)
$ sqlite3 "$EXP" "SELECT COUNT(*) FROM users;"
3
```

The three rows are back. `(shared)` means the rollback wrote a base
pointer at the `fork` checkpoint rather than copying it; the second line
is why a script that wants just the path takes the first line
(`offshoot rollback ... | head -1`). If the experiment had *worked*, the
other ending is `offshoot promote demo@experiment --onto main --force` —
repoint `main` at the experiment's head and ship it (`--force` because
`main` is protected by default).

## What just happened

- **`init` / `create`** — you made a [store](concepts.md#store) (a plain
  directory; `s3://bucket/prefix` works the same way) and a
  [database](concepts.md#database) with a protected `main`
  [branch](concepts.md#branch).
- **`checkout`** — you materialized a branch to a stock SQLite file
  ([checkout](concepts.md#checkout-the-working-copy)) and wrote to it
  with the ordinary `sqlite3` client; offshoot never proxies your SQL.
- **`checkpoint`** — you named durable states
  ([checkpoint](concepts.md#checkpoint)) you could later fork from, diff
  against, or roll back to. Checkpoints belong to one branch and are not
  inherited by forks — `fork --at <name>` starts a fork from a parent's
  checkpoint — and `promote` replaces the target's checkpoint list with a
  single `promote` entry.
- **`fork`** — you created an isolated branch that
  [shares its parent's storage copy-on-write](concepts.md#base-pointer-copy-on-write)
  via a base pointer: instant, near-zero bytes, fully isolated writes.
- **`diff` / `rollback` / `promote`** — the attempt loop: compare
  attempts, discard a failed one, or
  [promote](concepts.md#promote-rollback-and-compact-the-repointing-operations)
  a winner onto `main`.

## Where next

- **I want the full vocabulary:** [Core concepts](concepts.md).
- **I'm building a test suite or eval harness on this:** the
  [eval-harness tutorial](eval-harness.md) — seed once, fork per test,
  pytest/vitest fixtures, CI.
- **I want my agent to do this itself:** [Agents & MCP](agents.md) — one
  `claude mcp add` away.
- **Continuous capture while an agent writes** (no quiesce, incremental
  flushes): the daemon — see
  [`offshoot serve`](reference.md#offshoot-serve--socket-path--reap-every-duration--gc-grace-duration--flush-every-duration--snapshot-every-n--ro-cache-budget-bytes--fd-budget-n--http-addr--token-token--http-allow-non-loopback)
  in the reference.
