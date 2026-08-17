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
custom VFS on the read path — over storage you already control. (For the
shorter, non-design-doc version of this pitch, see the
[introduction](introduction.md); for the shared vocabulary this page
builds on, [core concepts](concepts.md).)

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
  hides that (see [Invariants](#invariants) below — durability is a
  reported fact, not an assumption).
- **Checkpoint (explicit)** — `checkpoint <db>@<branch> <name>`
  synchronously flushes (WAL → LTX → upload) and records `name → txid` in
  the ref. This is the only operation actually called "checkpoint"; the
  background shipping is "commit/sync." Checkpoints are per-branch.
  **Children do not inherit parent checkpoints** — a child's checkpoint
  map starts fresh at its fork point (just the auto-created `fork`
  checkpoint), so pre-fork states are not addressable from the child;
  resolve them on the parent.
- **Fork** — creates a child branch that **shares** its parent's
  already-durable storage (copy-on-write, since v0.2.0): the child gets its
  own lineage for everything it writes after the fork, plus a durable **base
  pointer** (`data/{lineage}/base.json`) naming the parent lineage and the
  fork-point txid. Reads on the child resolve through the parent's objects
  below the fork point and the child's own objects above it. Contract
  unchanged: a fork contains everything committed to SQLite before the fork
  call returned. See Fork mechanics below for the two snapshot floors that
  keep this bounded, and `compact` for cutting the cord manually.
- **Checkout** — a materialized local SQLite file. The agent opens it with
  any stock SQLite client at native speed; the daemon never proxies SQL.
  Checkout paths are immutable-per-materialization. **One writable checkout
  per branch per daemon**; read-only checkpoint checkouts alongside it are
  shipped as `offshoot checkout --at <checkpoint> --read-only` (a separate
  `checkouts-ro` cache tree, never the writable path).
- **Rollback** — `rollback <db>@<branch> --to <checkpoint>` repoints the
  branch at a new lineage seeded from the checkpoint state (internally, the
  fork machinery). The branch's old lineage is orphaned, retained for the GC
  grace period, then collected. The old checkout (and any
  acknowledged-but-not-durable writes) is closed out; rollback reports what
  transaction range was abandoned. The branch's canonical checkout path is
  then re-materialized fresh (the path itself is stable —
  `checkouts/<db>/<branch>.db`); the old file is closed out and replaced,
  never rewritten under a live connection.
- **Promote** — `promote <db>@<source> --onto <target>` repoints the target
  branch at a **new lineage seeded from the source's head** (fork machinery
  again). Because promote creates a fresh lineage, the one-writer-per-lineage
  invariant holds unconditionally — no epoch collisions are possible. The
  target's old lineage is orphaned → grace period → GC. The source branch
  survives unchanged (typically TTL-reaped later, or destroyed explicitly).
  Protected targets require `--force`.
- **Compact** — `compact <db>@<branch>` turns a shared (copy-on-write) fork
  into a self-contained branch: its full base-following chain at head is
  re-encoded as one snapshot in a fresh lineage and the base pointer is
  dropped, so the branch stops reading through — and stops pinning — its
  ancestors' storage. A no-op on an already self-contained branch. Like
  promote, it resets the checkpoint map (to `{"compact": head}`).
- **Destroy / GC** — explicit destroy, TTL expiry, and a background
  collector. Destroying a parent is always safe, instant, and allowed
  regardless of live children — a parent's destruction can never corrupt a
  child. Under copy-on-write, though, "destroyed" and "reclaimed" are
  different events: a destroyed parent's *bytes* linger for as long as any
  surviving child's chain still resolves through them, and are swept only
  once the last sharing child is destroyed or compacted. Destroying a
  branch with an active leased checkout requires `--force`. Fork-spam is
  the *expected* workload — leaking orphan branches into a bucket forever is
  the bug class this whole GC design exists to prevent.

### Fork mechanics and cost

Fork is copy-on-write (since v0.2.0). The common-case fork **shares**: it
writes a durable base pointer (`data/{lineage}/base.json`, create-only)
into the parent's already-durable chain — zero data objects of its own —
plus the child ref. The child ref also carries a `Base` reporting mirror,
but the base *object* is the resolution source of truth: a shared parent's
ref can be destroyed while descendants still resolve through its lineage.

**Resolution never merges across lineages.** `store.Chain` follows base
pointers transitively under one hard rule: each lineage's half of a chain
is resolved wholly within its own prefix and concatenated at a seam
verified contiguous, so epoch-dedup within one lineage can never let a
parent object win a child's txid range.

**Two automatic snapshot floors keep reads bounded** on any fork spine,
both keyed on the snapshot cadence (`SnapshotEvery`, default 16):

- **Fork-time floor** — a fork whose fully-resolved chain is already at the
  depth bound (`ops.ForkShareMaxDepth`, 16 by default; a daemon substitutes
  its own `-snapshot-every N`) materializes one fresh snapshot in the
  child's own lineage instead of sharing — the pre-copy-on-write fork path,
  now the fallback rather than the default.
- **Divergence floor** — a session's snapshot counter seeds from the
  branch's durable divergence, so the `SnapshotEvery` bound is a property
  of the branch, not the process; a shared child stops touching its parent
  once it has written its own snapshot.

The materialize fallback (and `promote`/`rollback`/`compact`, which always
materialize) uses a snapshot-copy fast path when the source resolves to a
single snapshot: a filesystem clone locally (APFS `clonefile`, Linux
`FICLONE`, plain-copy fallback), a server-side `CopyObject` on S3 —
single-request under 5 GiB, multipart `UploadPartCopy` up to S3's 5 TiB
per-object ceiling above it (since v0.2.4). Multi-member chains
materialize-and-re-encode. Measured numbers: [docs/benchmarks.md](benchmarks.md).

**The two cost models, stated honestly:** fork shares (near-free — N forks
of a G-byte database cost near-zero added store bytes, not N×G);
`promote`, `rollback`, and `compact` each still materialize a full
independent copy. The asymmetry is deliberate, not an oversight —
rollback/promote abandon their old lineage, and base-pointing into a
lineage that is meant to die would pin it forever. Page-level /
content-addressed dedupe remains a non-goal — see
[docs/faq.md](faq.md#storage-cost-honestly).

## Storage layout (epoch-fenced, versioned)

```
bucket/
  offshoot.json                              ← store manifest: layout version, created-at
  refs/{db}/{branch}                         ← ref object (schema-versioned, CAS'd)
  data/{lineage}/{epoch}/segment-{maxTXID}-{minTXID}.ltx ← segments
  data/{lineage}/{epoch}/snapshot-{txid}.ltx ← compacted snapshots / fork points
  data/{lineage}/base.json                   ← copy-on-write base pointer (create-only, immutable)
  gc/tombstones                              ← two-phase GC marker list (object-granular)
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
a binary refuses a layout newer than it understands. This mechanism has
been exercised for real: the first shared fork CAS-bumps a store from
LayoutVersion 1 to 2, and every pre-v0.2.0 binary then refuses the entire
store up front — deliberately, because a 0.1.x binary's lineage-granular
GC cannot see base pointers and would sweep a shared ancestor out from
under live children. There is no downgrade path once a base pointer
exists; a v0.2.x binary reads and writes a v1 (no-base) store unchanged.

**Supported stores:** AWS S3, MinIO — gated by a CAS
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

This works with connections offshoot doesn't own only under a **documented
contract** — two of its clauses are trusted assumptions today, not live
enforcement:

- Checkouts are materialized with WAL mode pre-set (`-wal`/`-shm` present),
  and foreign connections must leave `journal_mode` alone. **This is a
  trust assumption:** there is no live `PRAGMA journal_mode` polling and no
  rollback-journal watcher — a connection that flips a checkout out of WAL
  mid-session is not detected until the restart-time check below. (Live
  contract enforcement is future work; see
  [docs/status.md](status.md)'s connection-contract row.)
- **Agent and daemon must share a kernel and a local POSIX filesystem**
  (containers with bind mounts: yes; virtiofs/NFS/gVisor microVM
  boundaries: no — run offshoot *inside* the guest instead, it's one
  binary). This too is an assumption the deployment must honor, not
  something a probe verifies.
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
plus an **object-granular reachability collector**: the mark phase computes
the live set as the union, over every live branch, of its resolved chain at
head and at every checkpoint — using the same `store.Chain` resolution the
read path uses, never a reimplementation — plus every `base.json` along
each branch's base spine; the sweep is two-phase per object, tombstone →
grace period → re-mark → delete, batched via `DeleteObjects` on S3. Two
hardenings worth naming: the sweep never deletes above a live branch's
head in its own lineage *at or above the ref's current epoch* — a retrying
flush can legitimately re-create exactly that key, while an orphan at a
strictly older epoch is provably fenced and is swept once its tombstone
clears grace — and a fork that finds its fork point tombstoned after
writing its ref fails with a retryable error and deletes its own child
ref. GC fails closed: an incomplete mark deletes nothing, and
`offshoot_gc_errors_total` is the alertable signal for a stalled pass).

**TTL semantics:** TTL is measured from the last durable write (last
shipped segment) or lease expiry, whichever is later; `offshoot touch`
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
4. **Mutating a parent can never corrupt or orphan a child.** Since
   v0.2.0 a fork *is* referential (a shared child reads through its
   parent's durable objects below the fork point), but the safety property
   survives unchanged: a lineage's objects and its base pointer are
   immutable once written, resolution never merges members across
   lineages, and GC's reachability mark keeps every object a live child's
   chain resolves through — so destroying, rolling back, or otherwise
   mutating a parent changes only the parent's *ref*, never any bytes a
   child depends on. A destroyed parent's shared bytes linger until the
   last sharing child is destroyed or compacted.
5. **Checkout paths are immutable-per-materialization.** A live connection
   is never rewritten out from under itself; any repoint (rollback,
   promote, a foreign checkout) that would change what a path means
   produces a *new* materialization rather than mutating the old one in
   place.
6. **Checksums are verified on every read path that touches the store.**
   Materialization and compaction verify LTX per-file and cumulative
   checksums; a torn or missing chain member is a loud failure, never a
   silently short read. One deliberate carve-out: a clean, current,
   already-materialized checkout is served straight from local disk
   without re-touching the chain at all — see
   [docs/status.md](status.md)'s clean-checkout row for that tradeoff.
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
  captures; the foreign writer is repeatedly `kill -9`ed while the capturer
  is gracefully stopped and restarted; verify LTX checksums and restored-state
  equivalence after every bounce. This class of test lives in
  `internal/capture/torture_test.go`, runs nightly, and is required by
  `CONTRIBUTING.md` for capture/flush changes. It does not exercise a
  `SIGKILL` of the capturer itself; [testing.md](testing.md#the-kill--9-torture-harness)
  is the canonical boundary.
- Property tests: random workloads applied to (a) plain SQLite and (b)
  offshoot's sync/restore/fork paths must yield byte-equivalent query
  results.
- Fault injection on the storage layer: torn uploads, CAS races, clock skew
  on leases, GC racing forks.
- Integration matrix: MinIO + (as they come online) real S3 probe
  conformance; the local backend runs everywhere in CI.

## Security posture

- The daemon's unix socket is mode `0600`, scoping local access to the
  owning user.
- **Protected branches:** a per-branch flag (default on for `main`) making
  `destroy` and `promote --onto` require `--force`. Directly relevant to
  handing the MCP server to an LLM agent — the agent can fork and
  experiment freely but cannot vaporize `main` with a single unforced call.
- An HTTP listener is opt-in (`serve -http ADDR`): loopback-by-default,
  single-token Bearer auth, and a non-loopback bind requires both an
  explicit acknowledgment flag and an explicit token. `export` — the one
  op that writes to a client-chosen path on the daemon host — is refused
  over HTTP entirely; it remains unix-socket-only. See
  [docs/operations.md](operations.md#httpauth-threat-model) for the full
  threat model. With `-http` unset (the default) the daemon speaks only
  the local unix socket.

## Observability and resource limits

Shipped: `offshoot status` reports a six-state branch taxonomy
(`active`/`pending`/`error`/`dirty`/`detached`/`idle`) and a per-branch
storage class (`shared`/`materialized`); the daemon exposes a Prometheus
`/metrics` endpoint (capture lag, durable-through age per open session, GC
counters and backlog, fork/flush/checkpoint latencies, ro-cache usage) and
an event stream (socket `subscribe` op / SSE `GET /events`); and
`serve -ro-cache-budget` bounds the read-only checkout cache with LRU
eviction. See [docs/operations.md](operations.md) for the operator
reference. Still deferred: an FD budget with eviction of cold *writable*
checkouts — see [status.md](status.md#resource-behavior).

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
4. **LangGraph integration** — two layers: `OffshootSaver` is a
   `BaseCheckpointSaver` companion that wraps the stock SQLite saver on an
   offshoot checkout; `ThreadForks` keeps an existing checkpointer and forks
   the agent's separate application database when the thread forks.

See [docs/reference.md](reference.md) for the CLI in full and
[docs/status.md](status.md) for exactly which parts of each surface are
implemented today.
