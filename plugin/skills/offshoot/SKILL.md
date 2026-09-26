---
name: offshoot
description: Use when working on a project that has an offshoot store (a .offshoot directory or OFFSHOOT_STORE) and you are about to change a SQLite database — schema migrations, bulk deletes, data experiments, or running several attempts at a fix. Teaches the fork-before-risky-work loop with the offshoot_* MCP tools.
---

# offshoot: branch the database before you change it

offshoot gives every attempt its own real SQLite database — a copy-on-write
fork, not a mock and not a re-seed. Every checkout is a stock `.db` file any
SQLite client opens. You have nine tools; the loop is four calls.

## The loop

1. **Orient:** `offshoot_list` — see databases, branches, checkpoints, and
   which branches are protected (`main` is, by default). A human can
   protect any other branch too (`offshoot protect <db>[@branch]` /
   `offshoot unprotect`, CLI-only — there's no MCP tool for it, so you can
   see the flag here but never change it).
2. **Fork before risk:** `offshoot_fork {database, new_branch, branch?, at?, ttl?, meta?}`
   — instant, two small metadata objects, no data copy. Forks expire after
   the server's default TTL (24h) unless you pass `ttl`. Then
   `offshoot_checkout {database, branch: new_branch}` and point your SQL at
   the returned path.
3. **Checkpoint when tests pass:** `offshoot_checkpoint {database, branch, name, meta?}`.
   Name checkpoints for what they mean (`before-migration`, `migrated`).
4. **Roll back when they fail:** `offshoot_rollback {database, branch, to}`
   returns the branch to a checkpoint, discarding everything since. The
   branch's previous head is kept first as `<branch>-pre-rollback` — the
   result's `backup` field names it — so a rollback is undone by promoting
   that fork back onto the branch (`offshoot_promote {source:
   "<branch>-pre-rollback", target: branch}`). Same TTL'd, one-rolling-slot
   shape as promote's own safety fork below.
5. **Compare before you promote:** `offshoot_diff {database, left:
   "attempt-2@done", right: "main"}` (or against a golden checkpoint) — read
   the per-table added/removed/changed counts; use `table` + `full` to see
   the exact rows.
6. **Promote the winner:** `offshoot_promote {database, source, target, force?}`
   repoints `target` (often `main`) at `source`'s head. Protected targets
   refuse without `force`; that refusal is confirmation you need, not a bug.
   A protected branch cannot be forced through MCP unless the operator
   started `offshoot mcp -allow-force`; ask the human, who can promote from
   the CLI. The target's previous head is kept as `<target>-pre-promote` — the result
   names it — so a promote is undone by promoting that fork back. That
   safety fork always carries a TTL (24h by default) and is one rolling
   slot per target, replaced by the next promote onto that target, so the
   undo window closes when either happens.
7. **Clean up:** `offshoot_destroy` a failed attempt, or let its TTL expire.
   `offshoot_touch {database, branch, ttl?}` keeps a fork alive if a task
   runs long. A protected branch (or one under a live lease) cannot be
   destroyed with `force` through MCP unless the operator started
   `offshoot mcp -allow-force`; ask the human, who can destroy from the CLI.

## Rules

- Never run a migration, `DROP`, `DELETE`, or `UPDATE` without a checkpoint
  or a fork to return to. `/rewind` restores Claude's file edits only; it does
  not undo what a Bash `sqlite3` command did to a database.
- Prefer forking to backing up by hand. Prefer a longer `ttl` to `ttl: "none"`.
- Read results: `structuredContent` carries `txid`, `path`, `ttl`, `backup`
  for you to reuse; the text is the same information for the human.
- If a tool says a daemon session is open on a branch, close it first
  (`offshoot session close`) rather than forcing.
- Work in the fork's checkout path, not `main`'s, while experimenting.

## Rules-file snippet for the project's AGENTS.md or CLAUDE.md

```
Database changes: this project uses offshoot. Before any schema migration,
bulk delete, or data experiment, fork the database (offshoot_fork) and work
in the fork's checkout; checkpoint when tests pass; roll back when they
fail; promote the fork that worked. Never run destructive SQL on main's
checkout directly.
```

Docs: https://sricola.github.io/offshoot/docs/agents/ · machine-readable index:
https://sricola.github.io/offshoot/llms.txt
