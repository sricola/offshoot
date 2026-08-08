# offshoot — Branchable SQLite (v1 Design)

**Date:** 2026-07-29 (rev 2, post independent review)
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

## Naming model

- **Database** — a named entity (`invoices`); names are unique per store, charset `[a-z0-9-_.]`, max 128 chars.
- **Branch** — a named ref within a database (`invoices@attempt-1`); every database has a default branch `main`. Same charset rules.
- **Checkpoint** — a name within a branch mapping to a TXID. Same charset rules; unique per branch.
- **Checkout path** — a filesystem path returned by `checkout`; never used as an identifier.

`offshoot create <db>` creates a new empty database (refuses if the name exists). `offshoot create <db> --from existing.db` **imports**: snapshots the existing file (at a transaction boundary, source file untouched) as the root lineage. There is no mode that truncates or overwrites an existing user file.

## Core model

- **Lineage** — an append-only sequence of LTX transaction segments in object storage (LTX: Fly.io's Apache-2.0 transaction-aware format from Litestream v0.5). **Invariant: a lineage is only ever written by one branch, for its entire life.** Branches acquire new lineages (via fork/rollback/promote); they never share or adopt another branch's live lineage.
- **Branch** — a ref object holding: lineage id, current epoch, head TXID, checkpoint map (name → TXID), parent info, TTL, protected flag. All mutations to a ref are CAS'd.
- **Commit (background)** — the daemon continuously captures WAL frames and ships LTX segments on an interval. The API exposes per-branch durability ("durable through TXID X"); writes between shipments are acknowledged by SQLite but not yet durable in the bucket, and the API never hides that.
- **Checkpoint (explicit)** — `checkpoint <db>@<branch> <name>` synchronously flushes (WAL → LTX → upload) and records name → TXID in the ref. This is the only operation called "checkpoint"; the background shipping is "commit/sync." Checkpoints are per-branch. **Children do not inherit parent checkpoints** — a child's storage begins at its fork point, so pre-fork states are not materializable from the child; resolve them on the parent.
- **Fork** — creates a child branch with its own lineage. Contract: a fork contains everything committed to SQLite before the fork call returned. *(Superseded in part by the copy-on-write design, [2026-08-07-offshoot-copy-on-write-design.md](2026-08-07-offshoot-copy-on-write-design.md): the common case is now a **shared** fork — a base pointer into the parent's durable chain, zero new objects — rather than this spec's original always-materialized fork point. Mechanics below, rewritten to match what shipped.)*
- **Checkout** — a materialized local SQLite file. The agent opens it with any stock SQLite client at native speed; the daemon never proxies SQL. Checkout paths are immutable-per-materialization. **One writable checkout per branch per daemon**; additional checkouts of the same branch are read-only (`--read-only`) or refused.
- **Rollback** — `rollback <db>@<branch> --to <checkpoint>` repoints the branch at a new lineage seeded from the checkpoint state (internally the fork machinery). The branch's old lineage is orphaned, retained for the GC grace period, then collected. The old checkout (and any acknowledged-but-not-durable writes) is closed into `detached` state; rollback prints what TXID range was abandoned. A **new checkout path** is returned — we never rewrite a file under a live connection.
- **Promote** — `promote <db>@<source> --onto <target>` repoints the target branch at a **new lineage seeded from the source's head** (fork machinery again; source must be flushed first). Because promote creates a fresh lineage, the one-writer-per-lineage invariant holds — no epoch collisions are possible. The target's old lineage is orphaned → grace period → GC. An active checkout on the target transitions to `detached`. The source branch survives unchanged (typically TTL-reaped later). Protected targets require `--force`.
- **Destroy / GC** — explicit destroy, TTL expiry, and a background collector. Destroying a parent is always safe and allowed regardless of live children. *(Post-copy-on-write nuance: children are no longer necessarily storage-independent — a destroyed parent's ref is removed instantly, but its bytes are GC-reclaimed only once no surviving shared child's chain still reads through them; see the copy-on-write spec's destroy-semantics section.)* Destroying a branch with an active leased checkout requires `--force` and transitions that checkout to an error state. Fork-spam is the expected workload; leaking orphan branches into a user's bucket is a launch-killing bug class.

### Fork mechanics and cost

> **Rewritten post-copy-on-write.** This section originally described a
> single fork model: an always-materialized fork point seeded by an
> **asynchronous** snapshot upload, tracked by a `pending` marker on the
> child ref and a **fork pin** that GC honored on the parent's segments
> until the upload landed. That model was superseded by the copy-on-write
> design ([2026-08-07-offshoot-copy-on-write-design.md](2026-08-07-offshoot-copy-on-write-design.md))
> before the async machinery ever became the shipped behavior; the text
> below describes what shipped, so the two models aren't each
> half-described.

Fork has exactly two store-side cost classes, decided at fork time:

| Case | Mechanism | Store cost class |
|---|---|---|
| **Shared fork** (the common case) | flush parent if a session is open → write the child's durable base pointer + ref: an O(1) pointer into the parent's already-durable chain | **zero new data objects**, near-instant |
| **Materialized fork** (the fork-time snapshot floor: the fork point's fully-resolved chain is already at the depth bound) | copy the fork-point snapshot into the child's own lineage — server-side `CopyObject`/reflink when the fork point is a single snapshot object, materialize-and-re-encode otherwise — then write the ref | one full snapshot (~G bytes) |

Local checkout materialization (reflink/`clonefile` with byte-copy
fallback, warm vs cold cache) is unchanged by all of this and orthogonal
to the store cost above.

**Durability window: there is none for a shared fork.** A shared fork
uploads nothing asynchronously — it can only ever point into objects that
are *already durable* (forking a live session flushes the parent first),
and the fork is complete the moment its base pointer and ref land. The
`pending` marker and fork-pin machinery this section originally specified
are therefore **removed** for shared forks: there is no upload to wait
for, no window for GC to pin the parent across, and no `pending` branch
state arising from fork. The full-materialize paths — promote, rollback,
and the fork-time-floor fork above — keep the durability handshake they
already have: the snapshot copy completes synchronously *before* the ref
CAS lands, so there is no pending window there either; nothing about
those paths is deferred to the background.

**What GC honors instead of a fork pin:** reachability. The copy-on-write
GC marks every object any live branch's chain resolves through,
transitively across base pointers — a shared child keeps its ancestors'
shared objects live by construction, permanently rather than for an
upload window (see the copy-on-write spec's GC section).

**Storage amplification, restated honestly:** the original text here said
N materialized forks of a G-byte database cost up to N×G in the bucket,
with TTLs as the mitigation and page-level dedupe as a v2 idea. Shared
forks resolve that for the fork-heavy workload: N forks now cost near-zero
added bytes until each child diverges. The honest costs that *remain*:
promote, rollback, and `compact` still each materialize a full ~G-byte
copy (fork-at-X is free; rollback-to-X is not — a deliberate, documented
asymmetry), and a destroyed parent's bytes linger until no surviving
child's chain reads through them. Content-addressed page-level dedupe
across unrelated databases remains out of scope.

## Storage layout (epoch-fenced, versioned)

```
bucket/
  offshoot.json                            ← store manifest: layout version, created-at
  refs/{db}/{branch}                       ← ref object (schema-versioned, CAS'd)
  data/{lineage}/{epoch}/{txid}.ltx        ← segments
  data/{lineage}/{epoch}/snapshot-{txid}.ltx ← compacted snapshots / fork points
  gc/tombstones                            ← two-phase GC marker list
```

**Single-writer per branch** is enforced by compare-and-swap on the ref object (`If-Match` on PUT). **Fencing:** acquiring or reclaiming a branch bumps the epoch via the ref CAS; **every** object write — segments *and* snapshots — lands under the current epoch and uses `If-None-Match: *`. A paused-then-resumed stale writer uploads into a dead epoch prefix — garbage, never corruption. When a node loses its lease, its checkout transitions to `detached`; the API offers fork-as-new-branch for the orphaned tail, never a silent retry.

**Layout versioning:** `offshoot.json` carries the layout version; ref objects carry a schema version. v1 policy: newer binaries read older layouts; refuse to write a layout newer than they know.

**Supported stores:** AWS S3, Cloudflare R2, Tigris, MinIO — gated by a CAS capability probe at bucket attach. Not "any S3-compatible" (GCS's S3-interop API cannot CAS on writes). Probe failure → refuse, or run in explicit local-only mode.

**Local backend:** a plain directory implements the same interface. CAS is implemented with an `O_CREAT|O_EXCL` lock file per ref: acquire lock → read → verify expected version → atomic rename into place → release. (Bare `rename` is atomic replace, not CAS, and is not sufficient.) This is the quickstart path and the eval-harness path — no bucket required.

## WAL capture and the connection contract

Capture uses Litestream's mechanism — the daemon holds a long-running read transaction on its own connection (preventing foreign checkpoints from resetting the WAL), copies new frames continuously, and owns checkpointing. We **vendor/adapt Litestream's sync code** (Apache-2.0) rather than reimplement the lock dance.

This works with connections we don't own only under an **enforced contract**:

- Checkouts are materialized with WAL mode pre-set (`-wal`/`-shm` present). The daemon polls `PRAGMA journal_mode` and watches for a rollback-journal file appearing; violation hard-fails the branch into an error state (refuse sync, surface loudly) — never silent divergence.
- Requirement stated plainly: **agent and daemon must share a kernel and a local POSIX filesystem** (containers with bind mounts: yes; virtiofs/NFS/gVisor microVM boundaries: no — run offshoot *inside* the guest instead, it's one binary). A checkout-time probe verifies lock and SHM coherence from both sides.
- On daemon restart, any WAL/TXID mismatch (e.g., the agent's autocheckpoint ran while the daemon was down) marks the checkout **dirty**: its tail becomes an orphan fork rather than pretending continuity.
- Every materialization and compaction verifies LTX per-file and cumulative checksums.

## Architecture

One binary (`offshoot`), two modes:

- **CLI mode** — commands operate directly against a local-directory (or bucket) backend, **at rest**: `create`, `checkout`, `checkpoint`, `fork`, `rollback`, `promote`, `destroy` each open the database transiently, perform the read-lock/flush dance once, and exit. There is **no continuous capture in CLI mode**: durability advances only when an explicit `checkpoint`/`fork` runs. If a foreign writer holds a write transaction, CLI operations wait with a busy timeout and then fail cleanly. The 60-second quickstart runs entirely in this mode — no bucket, no server.
- **Daemon mode** — long-running process for server deployments: lifecycle API over unix socket + HTTP, continuous WAL capture and background sync, TTL/GC loop, S3 backend. This is the mode the fork-contract and durability guarantees above describe in full.

Components: lifecycle API · checkout manager (materialize, cache, FD/eviction, contract enforcement) · sync engine (vendored Litestream capture, WAL→LTX→store, restore) · lease manager (ref CAS, epochs) · GC (TTL expiry, two-phase collect: tombstone → grace period > max-fork-duration → re-list refs → delete; a fork that finds its fork point tombstoned after writing its ref fails with a retryable error and deletes its own child ref; partial child prefixes are collected as unreachable). Node-local state lives in offshoot's own SQLite database.

**TTL semantics:** TTL is measured from the last durable write (last shipped segment) or lease renewal, whichever is later; `offshoot touch` resets it explicitly. A branch with an active lease is never reaped — expiry defers until the lease is released or times out (a wedged holder loses the lease first, then TTL applies). Creating a child does not extend the parent. Branches without a TTL live until destroyed.

**Not in v1: multi-node.** Nodes sharing a bucket won't corrupt each other (the fencing scheme guarantees that), but there is no placement, failover, or cross-node routing, and we don't use the word "cluster." Cross-node movement = checkpoint + re-checkout elsewhere. Multi-node orchestration, fleet-wide schema migrations, and cross-tenant analytics (DuckDB over the fleet) are the v2 arc.

## Security posture (v1)

- Daemon HTTP binds loopback-only by default; unix socket permissions scope local access. Single shared token authenticates API calls. No TLS in v1 (loopback); non-loopback binds require explicit opt-in flag acknowledging the risk.
- **Protected branches:** a per-branch flag (default on for `main`) making `destroy` and `promote --onto` require `--force`. Directly relevant to handing the MCP server to an LLM agent — the agent can fork and experiment freely but cannot vaporize `main` with a single unforced call.

## Observability (v1)

- `offshoot status [<db>[@<branch>]]` — branch states (`active`, `detached`, `dirty`, `error`, `pending`), durable-through TXID and its age, lease holder, TTL remaining.
- Prometheus `/metrics` on the daemon: capture lag (frames pending), durable-through age per branch, GC backlog, checkout cache disk usage, fork/checkpoint latencies.
- Structured logs; every branch state transition is logged with cause.

## Resource limits (v1)

- Configurable local disk budget for the checkout/segment cache; LRU eviction of read-only and cold materializations (writable leased checkouts are never evicted).
- FD budget with eviction of idle read-only checkouts.
- No hard caps on branches/databases/DB size in v1; documented soft guidance (agent-session DBs: MBs to low GBs).

## Platform support

Linux and macOS (daemon and CLI). Windows is unsupported in v1 (the capture path and lock/SHM probing are POSIX-dependent) and stated as such in the README.

## Integration surface (v1)

1. **CLI** — the primary interface; everything scriptable.
2. **MCP server, day one** — wraps the lifecycle ops so agents fork before risky work on their own initiative (`claude mcp add offshoot`). This is both an adoption channel and the launch-demo mechanism.
3. **Python + TypeScript SDKs** — thin clients over the lifecycle API.
4. **LangGraph integration** — a checkpointer companion: fork the database when the thread forks. First framework target because its users already have time-travel and have noticed the world doesn't rewind.

## Error handling summary

| Failure | Behavior |
|---|---|
| Contract violation (journal mode, exclusive locking) | Branch → error state; sync refused; loud diagnostics |
| Daemon crash while agent writes | On restart, mismatch detected → checkout marked dirty → tail preserved as orphan fork |
| Lease lost (pause/partition) | Checkout → `detached`; uncommitted tail offered as fork; stale uploads fenced into dead epoch |
| Branch repointed underneath (rollback/promote by another caller) | Active checkout → `detached`; same recovery as lease loss |
| Rollback/promote discarding non-durable acked writes | Old checkout closed to `detached`; abandoned TXID range reported |
| Destroy under active lease | Refused without `--force`; with it, checkout → error state |
| Store without CAS | Refused at attach (probe), or explicit local-only mode |
| GC vs concurrent fork | Two-phase tombstone + grace; tripped fork fails retryably and removes its own ref |
| Restore corruption | LTX checksum verification on every materialization; fail closed |

## Testing strategy

- **Risk spike first (validates the product):** foreign-connection capture loop — a stock `sqlite3` client we don't control writing under load while the daemon captures; `kill -9` both sides in a loop; verify LTX checksums and restored-state equivalence. A negative result here invalidates the design; it is prototype #1.
- Property tests: random workloads applied to (a) plain SQLite and (b) offshoot sync/restore/fork paths must yield byte-equivalent query results.
- Fault injection on the storage layer (torn uploads, CAS races, clock skew on leases, GC racing forks, kill during async fork-snapshot upload).
- Integration matrix: MinIO + real S3 + R2 + Tigris probe conformance; local backend everywhere in CI; reflink and copy fallback paths both exercised.
- Torture CI job: the kill -9 loop runs continuously, not once.

## Launch plan

- **Demo:** an agent (via the MCP server) forks a 10GB database three ways, attempts three migration strategies in parallel, two destroy their copies, the test suite picks the winner, `offshoot promote` — headlined honestly: *"forking a 10GB database took ~40ms"* (local reflink fork; bucket-durable in the background — and the demo says so).
- **Docs:** quickstart (CLI mode, 60s), the eval-harness tutorial (the "serious" one), the connection contract page, and a published invariants page (one-writer-per-lineage, fencing scheme, durability semantics) — pre-empting the consistency scrutiny this genre attracts.
- **90-day success metrics:** 5 named projects using it in anger (≥2 eval harnesses/agent platforms); forks-per-database p50 > 0 (kill/pivot signal if 0 — that means we built a backup tool); MCP installs; LangGraph adapter listed upstream; ≥3 non-author contributors. Stars are an amplifier, not the goal.
- **Upstream posture:** contribute to superfly/ltx early; pin and wrap the Go API (no stability guarantee); the format spec is the contract.

## v1 scope summary

**In:** create (incl. `--from` import) / checkout / fork / checkpoint / rollback / promote / destroy / touch / status · TTL + two-phase GC · local-dir + S3/R2/Tigris/MinIO backends with CAS probe · epoch fencing (segments and snapshots) · reflink fork with copy fallback (the async fork-point upload originally listed here was superseded by copy-on-write shared forks before it shipped — see §Fork mechanics) · connection-contract enforcement · vendored Litestream capture · CLI + daemon in one binary · MCP server · Python/TS SDKs · LangGraph adapter · single-token auth + protected branches · checksum verification everywhere · Prometheus metrics · layout versioning.

**Out (v2 arc):** multi-node orchestration, fleet schema migrations, cross-tenant analytics, arbitrary-TXID PITR (named checkpoints only), tiered compaction beyond L0→snapshot, page-level dedupe, web UI, fine-grained authz, GCS, Windows.

## Key risks (ranked)

1. **Wedge thinness:** for laptop users `cp` is good enough; everything must aim at the server-side parallel-attempt persona or the launch lands as neat-but-unnecessary.
2. **Commoditization:** Turso (GA branching, weekly OSS releases) or Litestream's author (owns LTX) could ship forks. Defense: stock SQLite + your bucket + winning the integration layer (MCP/LangGraph/eval fixtures) fast.
3. **Category absorption:** sandboxes and actor runtimes converge on "snapshots included." Counter-story, loud from day one: data outlives compute — checkpoints you can query, diff, and promote to production.
4. **Capture spike fails:** foreign-connection capture proves flaky under torture → the design pivots (e.g., require daemon-brokered connection open) before any other code is written.
5. **Storage amplification:** materialized forks cost N×G; if real workloads fork large DBs at high rates, the bucket bill becomes the complaint. Mitigations: TTLs, `CopyObject` dedupe, v2 page-level dedupe.
