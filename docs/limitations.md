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

**Two at-rest `offshoot checkpoint` commands on one branch: the second is
refused.** An at-rest checkpoint takes the branch lease before it writes
anything and releases it in the same ref write that advances the head, so
it is a leased, epoch-fenced writer like a daemon session. While one runs,
a second checkpoint on the branch fails at once with `has a live lease
held by "checkpoint:<host>/<pid>/<nonce>" ... (another checkpoint is in
progress)`; retry once the first finishes. The acquire bumps the epoch, so
the checkpoint's object key is its own: no other writer can overwrite it,
and an object a crashed or fenced writer left at the same txid sits under
an older epoch, which chain resolution never picks and GC reclaims.
`--force` does not take over a live lease, a session's or a checkpoint's.

Two checks remain for what a lease cannot fence. After its ref write the
checkpoint `HEAD`s its object and compares the etag with the one its
upload returned (on a mismatch it reads the trailer checksum), which
catches something outside offshoot replacing the object; and it compares
the checkout's fingerprint from right after quiesce with the one at stamp
time, which catches a write to the checkout between the encode and the
stamp. When either finds the store's head may differ from the checkout,
it stamps no checksum and a hash no file can match: the checkout reads as
having un-checkpointed changes, so `fork` warns and `checkout` says it is
overwriting them, the shadow is dropped, the next checkpoint writes a full
snapshot, and `offshoot_checkpoint_overwrite_detected_total` counts it.

## One daemon per store

**What:** the supported topology today is exactly one daemon (and its
CLI) per store. Pointing daemons on two machines — or two daemons on one
machine — at the same bucket or directory is not supported and has not
been tested, even though each branch has only one leased writer.

**Why:** the epoch-fencing scheme protects the live-session write path
and, since an at-rest checkpoint takes the branch lease, `checkpoint`
too, but three pieces of groundwork for shared-store fleets haven't
landed: the at-rest repoints (`rollback`, `promote`, `compact`) still
write without taking the branch lease — they refuse a live one instead
(below), which is a same-host courtesy, not a cross-host protocol; lease
holder identity is hostname+pid, which containers can collide; and lease
expiry is judged against the claimant's wall clock, so large clock skew
between machines could steal a live lease. Each is fixable — the fencing
core is designed for this — and multi-daemon safety is the named first
step of any future fleet work ([non-goals](../ROADMAP.md#non-goals-v1)).

**The same-host rule (v0.2.14):** on the one supported host, the at-rest
verbs `checkpoint`, `rollback`, `promote --onto`, and `compact` refuse a
branch that has a live lease — an open daemon session, `offshoot lease
acquire`, or an at-rest checkpoint in progress — and refuse a branch that
is mid-`destroy` or mid-reap outright (no flag overrides that). A lease on
`promote`'s *source* never blocks; only the target's does. For the three
repoints, `--force` overrides the lease: the repoint clears it, so a
session's next flush fails rather than writing under a dead lineage, and
whatever it had committed since its last flush — up to one `-flush-every`
interval, default 30 s — never reaches the store; a checkpoint in
progress fails without committing. `checkpoint --force` never overrides a
lease: `checkpoint` takes the lease itself, so it waits its turn behind a
session or another checkpoint; a lease whose holder is gone (a killed
daemon, a forgotten `lease acquire`) is freed with `offshoot lease
release`. `checkpoint` also refuses a *detached*
checkout (one whose sidecar lineage no longer matches the ref, because the
branch was repointed after it was materialized) unless `--force`, since
checkpointing it would silently revert the repoint. `destroy` already
applied the same live-lease rule.

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

**Verified providers, honestly labeled:** RustFS (conformance suite runs against real RustFS in CI on every PR; MinIO was verified through v0.2.9 and is no longer re-verified since its images were withdrawn) and AWS S3 (probe + conformance +
multipart against a real us-east-1 bucket, nightly in CI since
2026-09-25). **Google Cloud Storage is unsupported** — its S3-interop API has no conditional writes,
so the probe refuses it outright
([why](faq.md#why-no-google-cloud-storage)). Other S3-compatible
endpoints may work but aren't claimed until the conformance suite passes
against them for real.

**Assumed of the object store, not probed:** strong read-after-write and
`LIST` consistency — offshoot reads a ref back after a compare-and-swap
and lists a lineage prefix to resolve a chain, and an eventually
consistent store could show a stale ref or an incomplete chain. AWS S3
(since December 2020), RustFS and MinIO all provide this. The conditional
multipart path (objects over 5 GiB) runs against the in-process fake S3 on
every `go test` and against a real provider only on RustFS (every PR) and
AWS (nightly).

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

**What's proven:** the torture harness runs a stock `sqlite3` writer and
`SIGKILL`s it mid-write in roughly half of every round, while bouncing
the capture engine through its graceful shutdown every 10th round; the
replica must converge to byte-identical dump output after every round. A
300-second run is ~3,500 rounds with zero divergence, and it runs in CI
on a nightly cadence ([the harness in full](testing.md#the-kill--9-torture-harness)).
On daemon restart, a WAL/txid mismatch (e.g. the agent's autocheckpoint
ran while the daemon was down) marks the checkout dirty and preserves the
tail as an orphan fork rather than pretending continuity. "Crash-tested"
in these docs means the *writer's* crash.

**The capturer's own crash is tested too** (since v0.2.15's harness,
`TestTortureCapturerKill`): the capture engine runs in a child process
that is `SIGKILL`ed mid-traffic every round and restarted on the same
state directory and replica; the replica must match the source after a
graceful drain. Every restart after a kill takes the rebase path (a kill
leaves no verified-clean checkpoint to resume from), and none has
diverged. **What isn't:** power loss — no harness cuts power, so the
kernel's write-back of what the killed process issued is assumed. A
daemon that dies, however it dies, loses up to one
`-flush-every` interval (default 30 s) of committed-but-unflushed writes
([the window](#durability-advances-on-flush-and-the-window-is-explicit)).
No power-loss test exists either: since v0.2.14 every rename into place
(local store objects and refs, materialized checkouts, capture state) is
followed by a directory `fsync(2)` (the plain call, not macOS's
`F_FULLFSYNC` disk-cache flush, which costs tens of milliseconds per
rename and is not needed to order a name behind an already-synced file),
and `.sum` sidecars are written atomically but not synced — they are a
cache that a reader re-derives by hashing — so a local store is
*designed* to keep what was flushed across power loss, but that design is
not torture-tested.

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
([the ledger](faq.md#storage-cost-honestly)). Locally, too, `destroy`
removes the branch's checkout, sidecar and shadow but **not** the
`checkouts-ro/<db>/~by-chain/` entries it was cloned from — another branch
may share them. They age out under the default LRU bound (64 entries per
database), or with `-ro-cache-budget`, or an `rm -rf` of `checkouts-ro`
([operations](operations.md#budgets)).

## The performance envelope, from measured numbers

All from [benchmarks](benchmarks.md) (darwin/arm64, Apple M5, local APFS
store; method and caveats there — the byte accounting transfers to S3, the
milliseconds don't):

- **Fork is near-constant, at a named checkpoint and — for a quiet
  checkout — at head:** ~9–10 ms from 12 MB to 1 GB, adding
  [377 bytes](benchmarks.md#added-object-store-bytes-per-fork-100-mb-database)
  for a 100 MB database, flat from 1 to 100 forks; 1 GB forks at head in
  9.5 ms. *Caveat:* the head check trusts the checkout's size/mtime
  fingerprint only when its last write is more than 1 s older than its
  sidecar stamp; inside that second it SHA-256-hashes the whole file as
  before (100 MB: ~44 ms).
- **A checkout of a state already on local disk is a clone:** 8.8 ms for a
  100 MB database (262.8 ms before v0.2.12), and a clean checkout is proven
  clean in ~0.3 ms at 64 MB and 512 MB alike. *Caveats:* the **first**
  materialization of a seed or a new chain still decodes it from the
  store, O(size); and
  on a filesystem that cannot clone (ext4, tmpfs, most network
  filesystems) there is no by-chain cache, so every checkout decodes, as
  before.
- **`promote` and `rollback` share by default** — a base pointer, like
  fork — so picking a winner no longer copies it. *Caveats:* `compact`
  still re-encodes the whole database (~G bytes for a G-byte database), as
  do `--materialize` and a promote or rollback at the fork-time depth
  floor;
  and a shared result keeps the lineage it points into live until it
  diverges past it or is compacted.
- **A diverging shared child pays only for changed pages** (~761 B per
  single-row transaction against a 100 MB database) — but every 16th
  flush (`-snapshot-every`, default 16) writes a full self-snapshot to
  keep read chains bounded.
- **At-rest `checkpoint` writes a segment of the changed pages when it
  can**, diffed against a reflinked shadow of the checkout. *Caveats:*
  the diff still **reads the whole checkout and the whole shadow**, so its
  local I/O is O(size) even when the upload is a few pages; half or more
  of the pages changing, a chain at the snapshot cadence, or `--snapshot`
  writes a full snapshot; and on a filesystem that cannot clone there is
  no shadow, so every at-rest checkpoint is a full snapshot — there, if
  you checkpoint large databases in a loop, run a daemon.
- **A fresh checkout's first query reads from disk, not the page
  cache.** A checkout is now a clone, and a clone starts with a cold page
  cache even when its source is warm: on the reference machine BranchBench's
  eval query (a scan of `order_line` in a 17 MiB database) took 50.6-54.9 ms
  on a fresh clone and 6.3-6.4 ms on a freshly written copy. BranchBench's
  eval p50 rose accordingly (`simulation` 11.5 → 56.4 ms) while its
  checkout p50 fell (314.0 → 43.0 ms); the step as a whole got faster
  ([the numbers, and the "Diagnostic instrumentation" tables](benchmarks.md#branchbench-topologies-v0212)).
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
- **No multi-node orchestration** — and no multi-node anything: two
  daemons on one store are unsupported
  ([one daemon per store](#one-daemon-per-store)); placement, failover,
  and routing are deliberately out of scope
  ([non-goals](../ROADMAP.md#non-goals-v1)).
- **The local store's per-key lock is time-broken.** A local store
  serializes writes to one key on a `.lock` file, and a lock older than
  30 s is broken on the assumption its holder died; since v0.2.14 the lock
  is held only around the compare and the rename (the object is streamed
  to a temp file first), so a legitimate hold is milliseconds — but a
  process paused for over 30 s inside that window can have its lock
  broken.
- **Never `export --force` over a database another process has open.**
  `export` writes a temp file and renames it over the destination; a
  process with the old file open keeps writing to the replaced inode, and
  its uncheckpointed WAL writes are orphaned with it.
- **No page-level dedupe** — copy-on-write shares whole objects between a
  fork and its own ancestors only, never across unrelated databases
  ([non-goals](../ROADMAP.md#non-goals-v1)).
