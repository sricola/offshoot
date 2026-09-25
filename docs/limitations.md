# Limitations

What offshoot doesn't do, where the edges are, and what to do about each.
Everything here is stated plainly because the guarantees elsewhere in
these docs are only credible if the boundaries are too. Companion pages:
[status](status.md) (the per-feature shipped/tested accounting),
[stability contract](stability.md) (the pre-1.0 promise in full), and the
[FAQ](faq.md) (why-not-X comparisons).

## One writer per branch

**What:** exactly one leased, epoch-fenced writer per branch at a time —
enforced, not advisory. Two processes cannot write the same branch
concurrently.

**Why:** every branch's lineage is an append-only sequence of storage
objects with a single writer for its entire life; that invariant is what
the whole no-corruption story rests on. Concurrent writers to one lineage
would mean reconciling interleaved WAL frames from two processes — a much
riskier problem than the one offshoot solves
([the full argument](faq.md#why-one-writer-per-branch)).

**Instead:** give each writer its own fork and `promote` the winner. Forks
are copy-on-write and near-free, so fork-per-writer is the intended
pattern, not a workaround.

## One daemon per store

**What:** the supported topology today is exactly one daemon (and its
CLI) per store. Pointing daemons on two machines — or two daemons on one
machine — at the same bucket or directory is not yet safe, even though
each branch has only one leased writer.

**Why:** the epoch-fencing scheme protects the live-session write path,
but three pieces of groundwork for shared-store fleets haven't landed:
the at-rest command paths (`checkpoint`, `rollback`, `promote`, `compact`
run from a second machine) write without taking the branch lease; lease
holder identity is hostname+pid, which containers can collide; and lease
expiry is judged against the claimant's wall clock, so large clock skew
between machines could steal a live lease. Each is fixable — the fencing
core is designed for this — and multi-daemon safety is the named first
step of any future fleet work ([non-goals](../ROADMAP.md#non-goals-v1)).

**Instead:** shard by store, not by daemon: give each host its own store
(the eval-harness per-worker pattern), and move state between them with
`offshoot export` / `create --from`, or checkpoint + re-checkout.

## No merge

**What:** no row-level or three-way merge between branches, and it's not
on the roadmap ([non-goals](../ROADMAP.md#non-goals-v1)).

**Why:** the target workload is fork-many-keep-one — attempts are
disposable, so there's nothing to merge back. Real merge would also
forfeit the single-fenced-writer invariant above, and would mean solving
schema and semantic conflicts with no general solution
([the full reasoning](faq.md#can-i-merge-two-branches)).

**Instead:** `promote` the winner whole; for reconciliation, materialize
both branches and use [`offshoot diff`](diff.md) (or `sqldiff` and your
own logic) outside offshoot. If you genuinely need merge as a first-class
operation, [Dolt is built for that](faq.md#why-not-dolt).

## Pre-1.0: the format may change — never silently

**What:** offshoot is 0.2.x. The on-disk/on-bucket storage format may
change in a backward-incompatible way in a **minor** release (0.2 → 0.3);
patch releases don't break. 1.0 is reserved for the point the format
freezes.

**The bound on it:** any format break ships **in the same release** with
either an in-place migration or a documented `export` → `create --from`
path — both halves of that path are shipped, tested code today. Every
store records a layout version, and a binary that doesn't understand a
store's layout refuses the whole store, loudly, rather than guessing.
Your data is never trapped either way: every checkout and every `export`
is a stock SQLite file. The full promise, mechanism, and proposed v1.0
criteria: [the stability contract](stability.md).

**Do:** pin exact versions, read release notes before upgrading a binary
that shares a store with others, and never point an older binary at a
store a newer one has written (the manifest gate will stop it, but it's
politer not to need it).

## Platforms: Linux and macOS only

**What:** no native Windows, for both the CLI and the daemon.

**Why:** the WAL capture path and lock/SHM coherence probing are
POSIX-specific — a real engineering gap, stated as a
[non-goal](../ROADMAP.md#non-goals-v1) rather than glossed over
([details](faq.md#why-no-windows-support)).

**Instead:** WSL2 works as-is (Linux binaries, Docker image, or build
from source).

## SQLite and filesystem requirements

**What:** checkouts are materialized in WAL mode and must stay that way,
and the agent and daemon **must share a kernel and a local POSIX
filesystem**: containers with bind mounts work; virtiofs, NFS, and
gVisor-style microVM boundaries don't
([the connection contract](architecture.md#wal-capture-and-the-connection-contract)).

**Why:** live capture works against connections offshoot doesn't own only
under that contract; POSIX lock semantics are what make it sound.

**Honest edge:** both clauses are trusted assumptions today, not live
enforcement — nothing polls `PRAGMA journal_mode` or watches for a
rollback-journal file mid-session. What IS detected, and tested: on
daemon restart/resume, a WAL-emptiness + main-file-hash continuity check
refuses to pretend continuity after any divergence — the checkout goes
dirty and its tail becomes an orphan fork instead
([how resume decides](architecture.md#wal-capture-and-the-connection-contract)).

**Instead:** across a VM or network-filesystem boundary, run offshoot
*inside* the guest — it's one binary.

## "S3-compatible" means conditional writes

**What:** offshoot requires the store to enforce conditional writes
(compare-and-swap). Every attach probes for this and **refuses to run**
against a store that can't provide it — fail-closed, on every CLI
invocation.

**Verified providers, honestly labeled:** MinIO (conformance suite runs
against real MinIO in CI on every PR) and AWS S3 (probe + conformance +
multipart against a real us-east-1 bucket, 2026-08-13). **Google Cloud
Storage is unsupported** — its S3-interop API has no conditional writes,
so the probe refuses it outright
([why](faq.md#why-no-google-cloud-storage)). Other S3-compatible
endpoints may work but aren't claimed until the conformance suite passes
against them for real.

## Durability advances on flush, and the window is explicit

**What:** between flushes, a daemon session's writes are committed to
SQLite but not yet durable in the store. Durability advances on explicit
`session flush`, and — by default — on the daemon's background timer:
`serve -flush-every`, default **`30s`** (`0` disables). Worst case, a
daemon that dies loses at most one interval's worth of
committed-but-unflushed writes.

**Why:** "durable" is a reported fact here, never an assumption —
`session status` shows the exact txid each session is durable through,
and hiding the window would be pretending
([why it's explicit](faq.md#why-is-durability-explicit-instead-of-automatic)).

**Do:** call `flush` explicitly wherever your durability requirement is
tighter than the cadence, or lower `-flush-every`.

## Crash behavior: what's proven, and what isn't

**What's proven:** the kill -9 torture harness runs a stock `sqlite3`
writer and `SIGKILL`s it mid-write on roughly half of every round, while
bouncing the capture engine mid-traffic every 10th round; the replica
must converge to byte-identical dump output after every round. A real
300-second run is ~3,500 rounds with zero divergence, and it runs in CI
on a nightly cadence ([the harness in full](testing.md#the-kill--9-torture-harness)).
On daemon restart, a WAL/txid mismatch (e.g. the agent's autocheckpoint
ran while the daemon was down) marks the checkout dirty and preserves the
tail as an orphan fork rather than pretending continuity.

**What isn't:** the harness bounces the capture engine through its
graceful-shutdown path — it does not `SIGKILL` the capturer process
itself. That case is argued safe in the code's shutdown/resume comments
but is not exercised by the harness, and this page says so because the
testing page does.

## Lease expiry is advisory; the fence is the guarantee

**What:** leases expire on a wall clock (default 30s, renewed
continuously by a live session), but clock expiry is not what protects
you. Acquiring or reclaiming a branch bumps its **epoch**, and every
object write lands under the epoch current at write time — a writer that
pauses, loses its lease, and resumes writes into a dead epoch prefix no
ref points at: garbage, never corruption. A fenced session stops rather
than write under a dead epoch.

**Consequence:** a wedged holder can hold a branch until its lease times
out (or an operator breaks it with `offshoot lease acquire`, which bumps
the epoch and fences the old holder out) — but it can never corrupt the
branch ([fencing in two paragraphs](testing.md#fencing-and-cas-in-two-paragraphs)).

## GC is deliberately slow to delete

**What:** garbage collection is two-phase per object: unreachable objects
are tombstoned first, then actually deleted only once the tombstone is
older than the grace period *and* still unreachable at sweep time
(`offshoot gc --grace`, default `1h`; the daemon janitor's `-gc-grace`,
default `15m`). An object re-referenced during grace — a fork racing GC —
is left alone; a fork that finds its fork point tombstoned fails with a
retryable error rather than proceeding. GC fails closed: an incomplete
mark deletes nothing.

**And under copy-on-write, "destroyed" ≠ "reclaimed":** destroying a
branch removes its ref instantly, but a destroyed parent's bytes linger
for as long as any surviving child's chain still reads through them —
reclaimed only once the last sharing child is destroyed or compacted
(`offshoot compact` is the manual release valve). Expect storage refunds
to lag destroys; that's the design, not a leak
([the ledger](faq.md#storage-cost-honestly)).

## The performance envelope, from measured numbers

All from [benchmarks](benchmarks.md) (darwin/arm64, local store; method
and caveats there — the byte accounting transfers to S3, the milliseconds
don't):

- **Fork at a named checkpoint is near-constant:** ~9–12 ms from 12 MB to
  1 GB, adding [377 bytes](benchmarks.md#added-object-store-bytes-per-fork-100-mb-database)
  for a 100 MB database, flat from 1 to 100 forks.
- **Fork at head is O(size)** in wall clock — not from the share, but
  from a safety check that SHA-256-hashes the whole checkout (~2.6 GB/s
  on the benchmark machine) to warn about un-checkpointed changes: 1 GB
  forks at head in ~418 ms. Fork a named checkpoint when latency matters.
- **`promote`, `rollback`, and `compact` each materialize a full copy**
  (~G bytes for a G-byte database). Fork is free; picking a winner isn't.
- **A diverging shared child pays only for changed pages** (~776 B per
  single-row transaction against a 100 MB database) — but every 16th
  flush (`-snapshot-every`, default 16) writes a full self-snapshot to
  keep read chains bounded.
- **At-rest `checkpoint` always writes a full snapshot** — without a
  daemon there's no record of which pages changed. If you checkpoint
  large databases in a loop, run a daemon.
- **A session whose checkout had to be (re)materialized pays one settling
  full-snapshot flush** after open — O(size), once per session; reopening
  a clean, current checkout uploads nothing.

## Smaller edges worth knowing

- **`offshoot status` can be expensive on large checked-out branches:**
  deciding `dirty` requires a WAL checkpoint plus a full SHA-256 of each
  checked-out, unleased branch's content on every call — there is no
  cheap short-circuit ([details](reference.md#branch-states)).
- **The read-only checkout cache can serve stale content** in one narrow
  case: a branch destroyed and recreated with a same-named checkpoint;
  `--force` or deleting `checkouts-ro` clears it
  ([details](reference.md#read-only-historical-checkout---at-checkpoint---read-only---force)).
- **Daemon tuning flags are never persisted** — a restarted daemon needs
  its flags passed again, and an at-rest CLI fork can't learn a daemon's
  `-snapshot-every` (it uses the library default of 16; materialization
  stays bounded either way) ([tuning flags](operations.md#tuning-flags)).
- **The opt-in HTTP listener has no TLS** — loopback-by-default with
  bearer-token auth; a non-loopback bind requires explicit
  acknowledgment plus an explicit token, and belongs behind a trusted
  network boundary ([threat model](operations.md#httpauth-threat-model)).
- **No multi-node orchestration** — nodes sharing a bucket won't corrupt
  each other (fencing guarantees that), but there's no placement,
  failover, or routing, deliberately
  ([non-goals](../ROADMAP.md#non-goals-v1)).
- **No page-level dedupe** — copy-on-write shares whole objects between a
  fork and its own ancestors only, never across unrelated databases
  ([non-goals](../ROADMAP.md#non-goals-v1)).
