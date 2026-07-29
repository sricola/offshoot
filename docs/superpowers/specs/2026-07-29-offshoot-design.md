# offshoot — Branchable SQLite (v1 Design)

**Date:** 2026-07-29
**Status:** Approved design, pre-implementation
**License target:** Apache-2.0
**Language:** Go

## One-liner

Branch SQLite like git: one binary and a bucket give every agent, test run, and tenant an instantly forkable, checkpointable database — self-hosted, stock SQLite, no one can deprecate it out from under you.

## Problem statement

Agent frameworks already fork *the agent*: LangGraph has checkpoint time-travel, the Claude Agent SDK has session resume and file checkpointing. What nobody forks is *the world* — the database the agent's code wrote to. When an agent rewinds its conversation, the side effects in SQLite don't rewind with it. The same gap hits eval harnesses (reset the database between runs; today: docker teardowns and fixture reloads) and agent platforms (a database per user/agent, forkable per attempt).

Prior art proves the primitive alone doesn't sell (LiteTree, 2018, dormant). What changed: agents generate fork-heavy workloads at server scale (Neon: 80% of databases and 97% of branches created by agents), and the packaging can now be *stock SQLite plus object storage* (Litestream v0.5's LTX format) rather than a patched engine. Every current competitor requires adopting something big: Turso Cloud (managed, proprietary, GA with instant branching), Cloudflare Durable Objects (Workers lock-in), Rivet (actor framework). Nothing is self-hostable, stock-SQLite, and neutral.

## Positioning

- **Identity:** branchable SQLite — the durable primitive. Agents are the first chapter of the docs, not the name of the product.
- **Positioning sentence:** *LangGraph can rewind the conversation; it can't rewind the database the agent wrote to.*
- **Trust stance:** Apache-2.0, stock SQLite (no fork, no rewrite), your bucket, single binary. The managed-SQLite graveyard (LiteFS Cloud sunset, Turso deprecations, Outerbase absorbed) is the reason neutrality is itself a feature.

## First user

The engineer at an agent-product or eval-infrastructure team who runs N parallel agent attempts server-side against real databases and needs isolation, lineage, and promote semantics. Explicitly **not** the individual laptop user (for whom `cp` and sandbox snapshots are good enough). This persona is named in the README's first paragraph and every doc example targets their workflow.

## Core model

- **Database** — a lineage of LTX transaction segments in object storage (LTX: Fly.io's Apache-2.0 transaction-aware format from Litestream v0.5). Every committed transaction is captured; named checkpoints mark restorable points.
- **Branch** — a named ref object pointing at a lineage. Branches have optional TTLs; session/attempt branches are just branches that expire.
- **Fork** — creates a child branch whose fork point is **materialized**: at fork time the daemon synchronously flushes the parent (WAL → LTX segment → upload), then writes a compacted snapshot of the fork-point state into the *child's own storage prefix*, built from the warm parent checkout on local disk (a file copy at a transaction boundary, ~10ms for typical agent-DB sizes). Children never reference parent segments. Contract: **a fork contains everything committed to SQLite before the fork call returned.** `fork --at <checkpoint>` forks from a named checkpoint instead.
- **Checkout** — a materialized local SQLite file. The agent opens it with any stock SQLite client at native speed; the daemon never proxies SQL. Checkout paths are immutable-per-materialization.
- **Commit** — the daemon captures WAL frames and ships LTX segments to storage on an interval, plus explicit `commit` calls that create named checkpoints. The API exposes per-branch durability ("durable through TXID X") — writes between auto-commits are acknowledged by SQLite but not yet durable in the bucket, and the API never hides that.
- **Rollback** — returns a **new checkout path** materialized at the named checkpoint (internally: fork-at-checkpoint). The old path is dead; we never rewrite a file under a live connection.
- **Promote** — atomically repoints a target branch's ref at the promoted branch's lineage (CAS on the target ref). This is the payoff operation: attempt-3 becomes main.
- **Destroy / GC** — explicit destroy, TTL expiry, and a background collector. Fork-spam is the expected workload; leaking orphan branches into a user's bucket is a launch-killing bug class.

## Storage layout (epoch-fenced)

```
bucket/
  refs/{branch}                     ← ref object: lineage id, current epoch,
                                       head TXID, parent info, TTL (CAS'd)
  data/{lineage}/{epoch}/{txid}.ltx ← segments; epoch in the key is the fence
  data/{lineage}/snapshot-{txid}.ltx← materialized fork points / compactions
  gc/tombstones                     ← two-phase GC marker list
```

**Single-writer per branch** is enforced by compare-and-swap on the ref object (`If-Match` on PUT). **Fencing:** acquiring or reclaiming a branch bumps the epoch via the ref CAS; every segment PUT lands under the current epoch and uses `If-None-Match: *`. A paused-then-resumed stale writer uploads into a dead epoch prefix — garbage, never corruption. When a node loses its lease, its checkout transitions to a `detached` state; the API offers fork-as-new-branch for the orphaned tail, never a silent retry.

**Supported stores:** AWS S3, Cloudflare R2, Tigris, MinIO — gated by a CAS capability probe at bucket attach. Not "any S3-compatible" (GCS's S3-interop API cannot CAS on writes). Probe failure → refuse, or run in explicit local-only mode.

**Local backend:** a plain directory implements the same interface (CAS via atomic rename). This is the quickstart path and the eval-harness path — no bucket required.

## WAL capture and the connection contract

Capture uses Litestream's mechanism — the daemon holds a long-running read transaction on its own connection (preventing foreign checkpoints from resetting the WAL), copies new frames continuously, and owns checkpointing. We **vendor/adapt Litestream's sync code** (Apache-2.0) rather than reimplement the lock dance.

This works with connections we don't own only under an **enforced contract**:

- Checkouts are materialized with WAL mode pre-set (`-wal`/`-shm` present). The daemon polls `PRAGMA journal_mode` and watches for a rollback-journal file appearing; violation hard-fails the branch into an error state (refuse commits, surface loudly) — never silent divergence.
- Requirement stated plainly: **agent and daemon must share a kernel and a local POSIX filesystem** (containers with bind mounts: yes; virtiofs/NFS/gVisor microVM boundaries: no — run offshoot *inside* the guest instead, it's one binary). A checkout-time probe verifies lock and SHM coherence from both sides.
- On daemon restart, any WAL/TXID mismatch (e.g., the agent's autocheckpoint ran while the daemon was down) marks the checkout **dirty**: its tail becomes an orphan fork rather than pretending continuity.
- Every materialization and compaction verifies LTX per-file and cumulative checksums.

## Architecture

One binary (`offshoot`), two modes:

- **CLI / embedded mode** — commands operate directly against a local-directory backend. `curl | sh`, `offshoot init`, `offshoot create app.db`, open with stock `sqlite3`, `offshoot fork app.db attempt-1` — under 60 seconds, no bucket, no server.
- **Daemon mode** — long-running process for server deployments: lifecycle API over unix socket + HTTP (single shared-token auth), continuous WAL capture, TTL/GC loop, S3 backend.

Components: lifecycle API · checkout manager (materialize, cache, FD/eviction, contract enforcement) · sync engine (vendored Litestream capture, WAL→LTX→store, restore) · lease manager (ref CAS, epochs) · GC (TTL expiry, two-phase collect: tombstone → grace period > max-fork-duration → re-list refs → delete). Node-local state lives in offshoot's own SQLite database.

**Not in v1: multi-node.** Nodes sharing a bucket won't corrupt each other (the fencing scheme guarantees that), but there is no placement, failover, or cross-node routing, and we don't use the word "cluster." Cross-node movement = commit + re-checkout elsewhere. Multi-node orchestration, fleet-wide schema migrations, and cross-tenant analytics (DuckDB over the fleet) are the v2 arc.

## Integration surface (v1)

1. **CLI** — the primary interface; everything scriptable.
2. **MCP server, day one** — wraps the lifecycle ops so agents fork before risky work on their own initiative (`claude mcp add offshoot`). This is both an adoption channel and the launch-demo mechanism.
3. **Python + TypeScript SDKs** — thin clients over the lifecycle API.
4. **LangGraph integration** — a checkpointer companion: fork the database when the thread forks. First framework target because its users already have time-travel and have noticed the world doesn't rewind.

## Error handling summary

| Failure | Behavior |
|---|---|
| Contract violation (journal mode, exclusive locking) | Branch → error state; commits refused; loud diagnostics |
| Daemon crash while agent writes | On restart, mismatch detected → checkout marked dirty → tail preserved as orphan fork |
| Lease lost (pause/partition) | Checkout → `detached`; uncommitted tail offered as fork; stale uploads fenced into dead epoch |
| Store without CAS | Refused at attach (probe), or explicit local-only mode |
| GC vs concurrent fork | Two-phase tombstone + grace period; fork re-checks tombstones after writing its ref |
| Restore corruption | LTX checksum verification on every materialization; fail closed |

## Testing strategy

- **Risk spike first (validates the product):** foreign-connection capture loop — a stock `sqlite3` client we don't control writing under load while the daemon captures; `kill -9` both sides in a loop; verify LTX checksums and restored-state equivalence. A negative result here invalidates the design; it is prototype #1.
- Property tests: random workloads applied to (a) plain SQLite and (b) offshoot commit/restore/fork paths must yield byte-equivalent query results.
- Fault injection on the storage layer (torn uploads, CAS races, clock skew on leases, GC racing forks).
- Integration matrix: MinIO + real S3 + R2 + Tigris probe conformance; local backend everywhere in CI.
- Torture CI job: the kill -9 loop runs continuously, not once.

## Launch plan

- **Demo:** an agent (via the MCP server) forks a 10GB database three ways, attempts three migration strategies in parallel, two destroy their copies, the test suite picks the winner, `offshoot promote` — headlined by the primitive benchmark: *"forking a 10GB database took ~40ms and $0."*
- **Docs:** quickstart (local mode, 60s), the eval-harness tutorial (the "serious" one), the connection contract page, and a published invariants page (fencing scheme, durability semantics) — pre-empting the consistency scrutiny this genre attracts.
- **90-day success metrics:** 5 named projects using it in anger (≥2 eval harnesses/agent platforms); forks-per-database p50 > 0 (kill/pivot signal if 0 — that means we built a backup tool); MCP installs; LangGraph adapter listed upstream; ≥3 non-author contributors. Stars are an amplifier, not the goal.
- **Upstream posture:** contribute to superfly/ltx early; pin and wrap the Go API (no stability guarantee); the format spec is the contract.

## v1 scope summary

**In:** create / fork / commit / rollback / promote / destroy · named checkpoints · TTL + two-phase GC · local-dir + S3/R2/Tigris/MinIO backends with CAS probe · epoch fencing · connection-contract enforcement · vendored Litestream capture · CLI + daemon in one binary · MCP server · Python/TS SDKs · LangGraph adapter · single-token auth · checksum verification everywhere.

**Out (v2 arc):** multi-node orchestration, fleet schema migrations, cross-tenant analytics, arbitrary-TXID PITR (named checkpoints only), tiered compaction beyond L0→snapshot, web UI, fine-grained authz, GCS.

## Key risks (ranked)

1. **Wedge thinness:** for laptop users `cp` is good enough; everything must aim at the server-side parallel-attempt persona or the launch lands as neat-but-unnecessary.
2. **Commoditization:** Turso (GA branching, weekly OSS releases) or Litestream's author (owns LTX) could ship forks. Defense: stock SQLite + your bucket + winning the integration layer (MCP/LangGraph/eval fixtures) fast.
3. **Category absorption:** sandboxes and actor runtimes converge on "snapshots included." Counter-story, loud from day one: data outlives compute — checkpoints you can query, diff, and promote to production.
4. **Capture spike fails:** foreign-connection capture proves flaky under torture → the design pivots (e.g., require daemon-brokered connection open) before any other code is written.
