# Architecture

This is offshoot's design document, edited for contributors and adopters
rather than for the internal review it was originally written for. It
describes the **model** — what a branch, a fork, a lineage, a lease actually
are, and the invariants that make concurrent, storage-shared access safe.
Where the model and the current implementation diverge (a feature described
here that isn't built yet, or a guarantee not yet exercised by a real
provider), this doc says so briefly and points to
[docs/status.md](status.md) for the authoritative, up-to-date accounting —
that page, not this one, is where implementation status is tracked, so it
doesn't drift out of sync in two places.

## Problem this solves

Agent frameworks already fork *the agent's conversation*: LangGraph has
checkpoint time-travel, and various agent SDKs support session resume and
file checkpointing. What none of them fork is *the world* — the database the
agent's code actually wrote to. Rewind a conversation and every row an agent
inserted is still there; the side effects don't rewind with the chat.

The same shape of gap shows up in eval harnesses (resetting a database
between runs today usually means container teardowns and fixture reloads)
and in agent platforms (a database per user or per attempt, ideally cheap
enough to fork freely). offshoot's answer is to make the database itself a
first-class branchable object, using **stock SQLite** — no forked engine, no
custom VFS on the read path — over storage you already control.

## Naming model

- **Database** — a named entity (`invoices`). Names are unique per store,
  charset `[a-z0-9-_.]`, max 128 characters.
- **Branch** — a named ref within a database (`invoices@attempt-1`). Every
  database has a default branch `main`. Same naming rules.
- **Checkpoint** — a name within a branch mapping to a transaction id. Same
  naming rules; unique per branch.
- **Checkout path** — a filesystem path returned by `checkout`; never used
  as an identifier, always re-derivable from `db@branch`.

`offshoot create <db>` creates a new empty database (refuses if the name
exists). `offshoot create <db> --from existing.db` **imports**: it snapshots
the existing file (at a transaction boundary, source file untouched) as the
root of a fresh lineage. There is no mode that truncates or overwrites an
existing user file.

## Core model

- **Lineage** — an append-only sequence of LTX transaction segments in
  object storage (LTX: Fly.io's Apache-2.0 transaction-aware format,
  introduced by Litestream v0.5). **Invariant: a lineage is only ever
  written by one branch, for its entire life.** Branches acquire *new*
  lineages via fork/rollback/promote; they never share or adopt another
  branch's live lineage.
- **Branch** — a ref object holding: lineage id, current epoch, head
  transaction id, checkpoint map (name → txid), parent info, TTL, protected
  flag. All mutations to a ref are compare-and-swapped.
- **Commit (background)** — the daemon continuously captures WAL frames and
  ships LTX segments as sessions flush. The API exposes per-branch
  durability ("durable through txid X"); writes between shipments are
  acknowledged by SQLite but not yet durable in the store, and the API never
  hides that (see [Durability](#durability) below).
- **Checkpoint (explicit)** — `checkpoint <db>@<branch> <name>`
  synchronously flushes (WAL → LTX → upload) and records `name → txid` in
  the ref. This is the only operation actually called "checkpoint"; the
  background shipping is "commit/sync." Checkpoints are per-branch.
  **Children do not inherit parent checkpoints** — a child's storage begins
  at its fork point, so pre-fork states are not materializable from the
  child; resolve them on the parent.
- **Fork** — creates a child branch with a **materialized fork point**: the
  child gets its own lineage in its own storage prefix and never references
  parent segments afterward. Contract: a fork contains everything committed
  to SQLite before the fork call returned. See Fork mechanics below.
- **Checkout** — a materialized local SQLite file. The agent opens it with
  any stock SQLite client at native speed; the daemon never proxies SQL.
  Checkout paths are immutable-per-materialization. **One writable checkout
  per branch per daemon**; the design allows read-only checkouts alongside
  it (not yet implemented — see [status.md](status.md)).
- **Rollback** — `rollback <db>@<branch> --to <checkpoint>` repoints the
  branch at a new lineage seeded from the checkpoint state (internally, the
  fork machinery). The branch's old lineage is orphaned, retained for the GC
  grace period, then collected. The old checkout (and any
  acknowledged-but-not-durable writes) is closed out; rollback reports what
  transaction range was abandoned. A **new checkout path** is returned — a
  file is never rewritten under a live connection.
- **Promote** — `promote <db>@<source> --onto <target>` repoints the target
  branch at a **new lineage seeded from the source's head** (fork machinery
  again). Because promote creates a fresh lineage, the one-writer-per-lineage
  invariant holds unconditionally — no epoch collisions are possible. The
  target's old lineage is orphaned → grace period → GC. The source branch
  survives unchanged (typically TTL-reaped later, or destroyed explicitly).
  Protected targets require `--force`.
- **Destroy / GC** — explicit destroy, TTL expiry, and a background
  collector. Destroying a parent is always safe and allowed regardless of
  live children (children are storage-independent once forked). Destroying
  a branch with an active leased checkout requires `--force`. Fork-spam is
  the *expected* workload — leaking orphan branches into a bucket forever is
  the bug class this whole GC design exists to prevent.

### Fork mechanics and cost

| Case | Mechanism | Cost class (design intent) |
|---|---|---|
| fork at head, warm parent checkout | flush parent → clone the checkout file | ~ms (reflink) / ~s per 10GB (copy fallback) |
| fork at head, cold parent (store only) | restore parent to local cache, then as above | restore cost + above |
| `fork --at <checkpoint>` | materialize checkpoint state from parent snapshot + segments, then seed child | restore cost |

**Implementation note:** the design targets reflink/clonefile (APFS
`clonefile`, XFS/btrfs `FICLONE`) with a byte-copy fallback, plus an
asynchronous fork-point upload. Today's implementation is the simple case
only — a synchronous local byte copy and a synchronous fork-point upload,
O(size) in both directions and unbenchmarked at scale. See
[status.md](status.md#daemon-and-durability) for exactly what's shipped.

Concurrent forks of the same parent are meant to be serialized by the
daemon (one flush, then N clones) once the reflink path lands.

**Durability window (design intent):** fork should return once the local
child checkout exists and the child ref is written, with the child's
fork-point snapshot uploading to the store *asynchronously* — until it
completes, the child ref would carry a `pending` marker and a fork pin that
GC honors on the parent's segments. Today the upload is synchronous, inside
the fork call, so there is no `pending` window to reason about yet.

**Storage amplification, stated honestly:** N materialized forks of a
G-byte database cost up to N×G in the store (less where snapshot boundaries
allow server-side `CopyObject` to avoid a re-upload). This is the price of
GC simplicity and parent-independence; TTLs on attempt branches are the
mitigation today. Content-addressed page-level dedupe is a possible future
direction, revisited only if TTLs and `CopyObject` dedupe prove
insufficient in practice — see [docs/faq.md](faq.md#storage-cost-honestly).

## Storage layout (epoch-fenced, versioned)

```
bucket/
  offshoot.json                              ← store manifest: layout version, created-at
  refs/{db}/{branch}                         ← ref object (schema-versioned, CAS'd)
  data/{lineage}/{epoch}/{txid}.ltx          ← segments
  data/{lineage}/{epoch}/snapshot-{txid}.ltx ← compacted snapshots / fork points
  gc/tombstones                              ← two-phase GC marker list
```

**Single-writer per branch** is enforced by compare-and-swap on the ref
object (conditional PUT). **Fencing:** acquiring or reclaiming a branch
bumps the epoch via the ref CAS; **every** object write — segments *and*
snapshots — lands under the current epoch, as a create-only write. A
paused-then-resumed stale writer uploads into a dead epoch prefix — garbage,
never corruption. When a node loses its lease, its checkout is no longer
authoritative for that branch; the branch remains forkable from wherever it
last landed, but the API never offers a silent retry into a dead epoch.

**Layout versioning:** `offshoot.json` carries the layout version; ref
objects carry a schema version. Policy: newer binaries read older layouts;
a binary refuses to write a layout newer than it understands.

**Supported stores:** AWS S3, Cloudflare R2, Tigris, MinIO — gated by a CAS
capability probe at bucket attach. Not "any S3-compatible" endpoint — GCS's
S3-interop API cannot CAS on writes, so it's refused outright rather than
silently degrading. See [status.md](status.md#storage-backends) for which
of these are verified against a real provider today versus expected-to-pass
on the same code path.

**Local backend:** a plain directory implements the same interface. CAS is
implemented with an `O_CREAT|O_EXCL` lock file per ref: acquire lock → read
→ verify expected version → atomic rename into place → release. (A bare
`rename` is atomic *replace*, not CAS, and is not sufficient on its own.)
This is the quickstart path and the eval-harness path — no bucket required.

## WAL capture and the connection contract

Capture uses Litestream's mechanism — the daemon holds a long-running read
transaction on its own connection (preventing foreign checkpoints from
resetting the WAL), copies new frames continuously, and owns checkpointing.
offshoot vendors/adapts Litestream's sync code (Apache-2.0) rather than
reimplementing the lock dance — full credit for that mechanism belongs to
Litestream; see [docs/faq.md](faq.md#why-not-litestream) for how the two
projects relate.

This works with connections offshoot doesn't own only under an **enforced
contract**:

- Checkouts are materialized with WAL mode pre-set (`-wal`/`-shm` present).
  The daemon polls `PRAGMA journal_mode` and watches for a rollback-journal
  file appearing; a violation hard-fails the branch into an error state
  (refuse sync, surface loudly) — never silent divergence.
- **Agent and daemon must share a kernel and a local POSIX filesystem**
  (containers with bind mounts: yes; virtiofs/NFS/gVisor microVM
  boundaries: no — run offshoot *inside* the guest instead, it's one
  binary). A checkout-time probe verifies lock and SHM coherence from both
  sides.
- On daemon restart, any WAL/txid mismatch (e.g. the agent's own
  autocheckpoint ran while the daemon was down) marks the checkout dirty:
  its tail becomes an orphan fork rather than pretending continuity.
- Every materialization and compaction verifies LTX per-file and cumulative
  checksums.

## Architecture

One binary (`offshoot`), two modes:

- **CLI mode** — commands operate directly against a local-directory (or
  bucket) backend, **at rest**: `create`, `checkout`, `checkpoint`, `fork`,
  `rollback`, `promote`, `destroy` each open the database transiently,
  perform the read-lock/flush dance once, and exit. There is **no
  continuous capture in CLI mode**: durability advances only when an
  explicit `checkpoint`/`fork` runs. If a foreign writer holds a write
  transaction, CLI operations wait with a busy timeout and then fail
  cleanly. The 60-second quickstart in the README runs entirely in this
  mode — no bucket, no server.
- **Daemon mode** — a long-running process (`offshoot serve`) for server
  deployments: a lifecycle API over a unix socket, continuous WAL capture
  and background sync, the TTL/GC janitor loop, S3-compatible backends.
  This is the mode the fork-contract and durability guarantees above
  describe in full.

Components: lifecycle API (`internal/daemon`) · checkout materialization
(`internal/ops`) · sync engine (`internal/capture`, `internal/session` —
vendored Litestream-style capture, WAL→LTX→store, restore) · lease manager
(ref CAS, epochs — `internal/ops`, `internal/store`) · GC (TTL expiry,
two-phase collect: tombstone → grace period → re-list refs → delete; a fork
that finds its fork point tombstoned after writing its ref fails with a
retryable error and deletes its own child ref; partial child prefixes are
collected as unreachable).

**TTL semantics:** TTL is measured from the last durable write (last
shipped segment) or lease renewal, whichever is later; `offshoot touch`
resets it explicitly. A branch with an active lease is never reaped —
expiry defers until the lease is released or times out (a wedged holder
loses the lease first, then TTL applies). Creating a child does not extend
the parent. Branches without a TTL live until destroyed.

**Not in scope: multi-node orchestration.** Nodes sharing a bucket won't
corrupt each other (the fencing scheme guarantees that), but there is no
placement, failover, or cross-node routing, and offshoot deliberately
doesn't use the word "cluster." Cross-node movement today is
checkpoint + re-checkout elsewhere.

## Invariants

The properties above amount to a short list of invariants that hold
regardless of implementation status — these are what to check first when
reasoning about a correctness question, and what any change to the storage
or capture path must preserve:

1. **One writer per lineage, for its entire life.** A lineage is never
   shared or adopted across branches. Concurrent writers to the *same*
   lineage never happens by construction — not by locking it out at
   runtime, but because nothing in the API creates that situation.
2. **Every ref mutation is a conditional write.** No branch pointer ever
   moves except via a CAS that names the exact prior state it's replacing.
   A losing writer gets a clean, typed error, never a silently dropped
   update.
3. **Every object write lands under the epoch current at write time, as a
   create-only put.** A writer that pauses, loses its lease, and resumes
   later writes into a dead epoch prefix that no ref will ever point at —
   garbage, collected with the rest of an orphaned lineage, never
   corruption of a live one.
4. **A fork is materialized, not referential.** Once created, a child
   branch's storage never depends on its parent's continued existence.
   Destroying, rolling back, or otherwise mutating a parent can never
   corrupt or orphan a child's data.
5. **Checkout paths are immutable-per-materialization.** A live connection
   is never rewritten out from under itself; any repoint (rollback,
   promote, a foreign checkout) that would change what a path means
   produces a *new* materialization rather than mutating the old one in
   place.
6. **Checksums are verified on every read path.** Materialization and
   compaction verify LTX per-file and cumulative checksums; a torn or
   missing chain member is a loud failure, never a silently short read.
7. **Protected branches require an explicit override for anything
   destructive.** `main` is protected by default; `destroy` and `promote
   --onto` a protected branch refuse without `--force`, uniformly across
   the CLI, the daemon, and the MCP server.
8. **Durability is a reported fact, not an assumption.** Whatever the API
   reports as "durable through txid X" is exactly what round-trips through
   a restore — never optimistic, never stale by more than the caller can
   observe via `status`/`session status`.

## Error handling summary

| Failure | Behavior |
|---|---|
| Contract violation (journal mode, exclusive locking) | Branch's session errors out; sync refused; loud diagnostics |
| Daemon crash while agent writes | On restart, a WAL/txid mismatch is detected → checkout marked dirty → tail preserved as an orphan fork rather than assumed continuous |
| Lease lost (pause/partition) | The branch remains fenced against the stale holder; the API offers fork-as-new-branch for any orphaned tail rather than a silent retry |
| Branch repointed underneath (rollback/promote by another caller) | Any other active checkout is superseded; same recovery path as lease loss |
| Rollback/promote discarding non-durable acknowledged writes | Old checkout state is abandoned; the abandoned transaction range is reported |
| Destroy under an active lease | Refused without `--force`; with it, the checkout transitions to an error state |
| Store without CAS | Refused at attach (the capability probe), full stop |
| GC vs. concurrent fork | Two-phase tombstone + grace period; a tripped fork fails retryably and removes its own ref |
| Restore corruption | LTX checksum verification on every materialization; fails closed |

## Testing strategy

- **Foreign-connection capture torture**, the risk spike that validated the
  whole approach before most other code was written: a stock `sqlite3`
  client the daemon doesn't control writes under load while the daemon
  captures; `kill -9` both sides in a loop; verify LTX checksums and
  restored-state equivalence after every bounce. This class of test lives
  in `internal/capture/torture_test.go` and re-runs on every change to the
  capture path.
- Property tests: random workloads applied to (a) plain SQLite and (b)
  offshoot's sync/restore/fork paths must yield byte-equivalent query
  results.
- Fault injection on the storage layer: torn uploads, CAS races, clock skew
  on leases, GC racing forks.
- Integration matrix: MinIO + (as they come online) real S3/R2/Tigris probe
  conformance; the local backend runs everywhere in CI.

## Security posture

- The daemon's unix socket is mode `0600`, scoping local access to the
  owning user.
- **Protected branches:** a per-branch flag (default on for `main`) making
  `destroy` and `promote --onto` require `--force`. Directly relevant to
  handing the MCP server to an LLM agent — the agent can fork and
  experiment freely but cannot vaporize `main` with a single unforced call.
- An HTTP binding with loopback-by-default access and single-token auth is
  designed but not built — see [status.md](status.md#observability-and-security).
  There is no network listener in the daemon today; it speaks only the
  local unix socket.

## Observability and resource limits (design intent)

The design calls for `offshoot status` reporting richer branch states,
lease-aware durability ages, and a Prometheus `/metrics` endpoint on the
daemon (capture lag, durable-through age per branch, GC backlog, checkout
cache disk usage, fork/checkpoint latencies), plus configurable disk/FD
budgets with LRU eviction of cold read-only materializations. None of the
observability or resource-limit pieces beyond `status`/`session status` are
implemented yet — see [status.md](status.md#observability-and-security) and
[status.md](status.md#resource-behavior) for the current state and the
roadmap milestones tracking each.

## Platform support

Linux and macOS (daemon and CLI). Windows is unsupported (the capture path
and lock/SHM probing are POSIX-dependent) and stated as such rather than
left to a confusing failure.

## Integration surface

1. **CLI** — the primary interface; everything scriptable.
2. **MCP server** — wraps the lifecycle ops so an agent can fork before
   risky work on its own initiative (`claude mcp add offshoot`).
3. **Python + TypeScript SDKs** — thin clients over the daemon's lifecycle
   API.
4. **LangGraph integration** — a checkpointer *companion*: fork the
   database when the thread forks, rather than a `BaseCheckpointSaver`
   replacement.

See [docs/reference.md](reference.md) for the CLI in full and
[docs/status.md](status.md) for exactly which parts of each surface are
implemented today.
