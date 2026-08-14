# Introduction

offshoot branches SQLite the way git branches code: `create`, `fork`,
`checkpoint`, `rollback`, `promote` — as stock SQLite files, over a local
directory or an S3-compatible bucket you already control, with one binary.
Every checkout is a plain `.db` file any SQLite tool opens; there is no
forked engine and no custom VFS on the read path.

## Why this exists

An agent attempt or eval run needs a real database it can trash: mocks
aren't real, re-seeding is slow, and container or VM snapshots version a
whole machine to get at one file. Agent frameworks already fork the
*conversation* — what none of them fork is *the world*, the database the
agent's code actually wrote to, so rewinding a chat leaves every inserted
row behind. offshoot makes the database itself the branchable object:
fork per attempt, checkpoint what worked, roll back what didn't, promote
the winner, and let the losers expire on a TTL.

## The mental model in one picture

```
demo@main         ○ init ──── ○ seeded ────────────▶ head
                              │
                              │ fork = a copy-on-write base pointer,
                              │ not a copy (377 B for a 100 MB database)
demo@experiment               ╰──── ○ fork ────────▶ head
```

A fork **shares** its parent's already-durable storage through a base
pointer and writes new objects only as it diverges — so forking is
near-free, and a destructive experiment on `demo@experiment` never touches
`demo@main`. The full model, with measured numbers:
[Core concepts](concepts.md), [Architecture](architecture.md), and
[Benchmarks](benchmarks.md).

## Who it's for

- **Agent harnesses** — fork before risky work, promote what passed, via
  MCP tools, Python/TypeScript SDKs, or the CLI
  ([Agents & MCP](agents.md)).
- **Eval harnesses and test suites** — seed once, fork per test or per
  attempt, with pytest fixtures and a vitest/jest testkit
  ([eval-harness tutorial](eval-harness.md)).
- **CI** — parallel migration attempts against real data, one branch each
  ([CI recipes](ci-recipes.md)).

For a laptop backup, `cp app.db backup.db` is genuinely fine — offshoot is
for the server-side, fork-heavy case where you need lineage, safe
concurrency, and cleanup, not just a copy of the bytes
([why not just `cp`?](faq.md#why-not-just-cp)).

## What offshoot deliberately is not

- **Not multi-writer.** Exactly one leased, epoch-fenced writer per
  branch; two agents writing "at once" get two forks and a `promote`
  ([why](faq.md#why-one-writer-per-branch)).
- **No cross-lineage merge.** The workload is fork-many-keep-one; the
  escape hatch is `offshoot diff` over two checkouts
  ([why](faq.md#can-i-merge-two-branches)).
- **Not a server database, not a cluster.** Your bucket, your binary,
  Apache-2.0 — no replication, failover, or managed service
  ([non-goals](../ROADMAP.md#non-goals-v1)).
- **Pre-1.0.** The storage format may still change before 1.0 — never
  silently, and any break ships with a migration or a documented export
  path in the same release ([Limitations](limitations.md),
  [stability contract](stability.md)).

## Where next

- **I want it running:** [Installation](installation.md)
- **I want the aha moment in five minutes:** [Quickstart](quickstart.md)
- **I want the vocabulary and the model:** [Core concepts](concepts.md)
- **I'm wiring up an agent:** [Agents & MCP](agents.md)
