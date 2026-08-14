# Core concepts

The vocabulary offshoot's commands, docs, and error messages all share —
each term defined once, with the model it belongs to. The deeper design
document behind this page is [Architecture](architecture.md); the exact
flag-by-flag behavior of every command named here is the
[CLI reference](reference.md).

## One picture

```
              checkpoints
demo@main     ○ init ──── ○ seeded ──── ○ v2 ─────────▶ head    (lineage A)
                          │
                          │  fork: child lineage B writes a base pointer
                          │  naming A and the fork-point txid — no data
                          │  copy; reads below the fork point resolve
                          │  through A's own objects
demo@attempt-1            ╰──── ○ fork ──── ○ passed ─▶ head    (lineage B: shared)

promote attempt-1 onto main:
demo@main     ○ promote ──────────────────────────────▶ head    (lineage C: materialized)
              main is repointed at a NEW lineage seeded from B's head —
              a full copy (fork shares; promote materializes). Lineage A
              is orphaned: tombstoned, held for a grace period, then GC'd.
              attempt-1 survives unchanged — typically forked with --ttl,
              so the janitor reaps it after expiry, and a destroyed or
              reaped branch's shared bytes are reclaimed once no surviving
              child still reads through them.
```

## The naming model

### Store

The place everything durable lives: a local directory or an S3-compatible
bucket (`-store ./.offshoot`, `-store s3://bucket/prefix`). A store holds
refs, snapshots, and segments; checkouts are always real local SQLite
files even when the store is a bucket. Every attach probes the store for
conditional-write (compare-and-swap) support and refuses to run without
it — see [Installation](installation.md#initialize-a-store) and the
[storage layout](architecture.md#storage-layout-epoch-fenced-versioned).

### Database

A named entity in a store (`invoices`). Names are unique per store,
charset `[a-z0-9-_.]`, max 128 characters. `offshoot create invoices`
makes an empty one; `offshoot create invoices --from existing.db` imports
an existing SQLite file (the source is never modified).

### Branch

A named ref within a database, addressed `db@branch`
(`invoices@attempt-1`); every database has a default branch `main`, and
`db` alone means `db@main`. The ref records the branch's lineage id,
epoch, head transaction id, checkpoint map, TTL, and protected flag — and
every mutation to it is a compare-and-swapped conditional write
([invariant 2](architecture.md#invariants)).

### Checkpoint

A name within a branch mapping to a transaction id — `offshoot checkpoint
app v1` — unique per branch. It's the unit you fork from (`fork --at v1`),
roll back to (`rollback --to v1`), export, or diff against. Children never
inherit a parent's checkpoints: a fork's own history begins at its fork
point with an auto-created `fork` checkpoint, so pre-fork states are
resolved on the parent instead.

### Checkout (the working copy)

The materialized local SQLite file for a branch — what `offshoot checkout
app@attempt-1` prints, at the fixed path
`<store>/checkouts/<db>/<branch>.db` for local stores. You open it with
any stock SQLite client at native speed; offshoot never proxies SQL. The
path is derived from `db@branch`, never used as an identifier, and a live
connection is never rewritten out from under itself
([invariant 5](architecture.md#invariants)). Read-only historical
checkouts of a named checkpoint live in a separate `checkouts-ro` tree
(`checkout --at v1 --read-only`).

## The storage model

### Lineage

An append-only sequence of transaction segments in the store — the
storage history behind a branch. The core invariant: **a lineage is only
ever written by one branch, for its entire life**
([invariant 1](architecture.md#invariants)). Branches acquire *new*
lineages via fork, rollback, and promote; they never share or adopt
another branch's live lineage. Segments use LTX, Litestream v0.5's
Apache-2.0 transaction-aware format
([credit](faq.md#why-not-litestream)).

### Snapshot

A full encoding of the database at one transaction id — the self-contained
kind of object in a lineage. Every at-rest `offshoot checkpoint` writes
one; a daemon session also writes one every Nth flush
(`-snapshot-every`, default 16) to keep replay bounded.

### Segment

An incremental object: only the pages changed since the previous flush,
at a specific transaction id. Only a daemon session writes segments
(continuous capture knows what changed); measured cost is
[~776 B per single-row transaction](benchmarks.md#divergence-cost-what-a-shared-child-pays-as-it-writes)
against a 100 MB database.

### Chain

The resolved read path for a branch at some transaction id: one snapshot
plus the segments after it, in order — never more than one snapshot plus
`N-1` segments at the default cadence, so materializing state never
replays an unbounded history. Chain resolution follows base pointers
across lineages but never merges members across them
([fork mechanics](architecture.md#fork-mechanics-and-cost)).

### Base pointer (copy-on-write)

What makes fork near-free. A forked child gets its own lineage for
everything it writes *after* the fork, plus one durable base pointer
(`data/{lineage}/base.json`) naming the parent lineage and the fork-point
txid. Reads on the child resolve through the parent's objects below the
fork point and the child's own above it — so a fork of a 100 MB database
adds [377 bytes](benchmarks.md#added-object-store-bytes-per-fork-100-mb-database)
to the store, flat from 1 to 100 forks. Two automatic snapshot floors
keep chains bounded on deep fork-of-fork spines
([details](architecture.md#fork-mechanics-and-cost)).

### Storage class: shared vs materialized

`offshoot status` labels every branch `storage=shared` (a base-pointer
fork: near-free to hold, but it pins whatever ancestor storage its chain
still reads through) or `storage=materialized` (a fully self-contained
lineage: created roots, and the results of promote/rollback/compact). The
asymmetry to internalize: **fork shares (near-free); promote, rollback,
and compact each materialize a full copy** — deliberate, because those
operations abandon their old lineage, and base-pointing into a lineage
that is meant to die would pin it forever
([the storage-cost ledger](faq.md#storage-cost-honestly)).

### Promote, rollback, and compact: the materializing operations

All three are the fork machinery pointed at a new, self-contained lineage.
**Rollback** (`rollback app@b --to v1`) repoints the branch at a lineage
seeded from a checkpoint, keeping checkpoints at or before the target.
**Promote** (`promote app@attempt-1 --onto main --force`) repoints the
target at a lineage seeded from the source's head; the source survives
unchanged, and the target's checkpoint map resets to just `promote`.
**Compact** (`compact app@b`) turns a shared fork into a self-contained
branch — the manual lever for releasing a destroyed ancestor's storage —
resetting its checkpoints to `compact`. Each pays a full copy
(~G bytes for a G-byte database).

## The concurrency model

### Session

A live, daemon-held writing context on one branch: `offshoot session open
app` (against a running `offshoot serve`) acquires the branch's lease,
materializes its checkout, and captures every committed WAL transaction
continuously while your process keeps writing. `session flush` (and the
daemon's background timer, default every 30s) makes captured writes
durable in the store; `session close` releases the lease. Without a
daemon, every command runs *at rest* — open, do the work, exit.

### Lease

The claim that makes a branch's single writer explicit: at most one
holder per branch, renewed continuously by a daemon session, inspectable
and breakable via `offshoot lease list/acquire/release`. Expiry is
wall-clock and advisory — the actual guarantee against a stale writer
comes from the epoch fence and ref CAS, not the clock
([fencing in two paragraphs](testing.md#fencing-and-cas-in-two-paragraphs)).

### Epoch

The fencing counter in the ref: acquiring or reclaiming a branch bumps
it, and every object write lands under the epoch current at write time as
a create-only put. A writer that pauses, loses its lease, and later
resumes writes into a dead epoch prefix no ref points at — garbage,
collected later, never corruption of the live chain
([invariant 3](architecture.md#invariants)).

### TTL

A branch's self-destruct timer, set at fork time (`fork app attempt-1
--ttl 2h`) or later (`touch app@attempt-1 --ttl 30m`; `--ttl none` clears
it). Measured from the last durable write or lease renewal, whichever is
later; `touch` resets the clock. A branch with an active lease is never
reaped, protected branches are never reaped, and branches without a TTL
live until destroyed. Reaping is the janitor's job — `offshoot serve`'s
timer or an on-demand `offshoot gc`.

### Protected branch

A per-branch flag, on by default for `main`: `destroy` and `promote
--onto` refuse without `--force`, uniformly across the CLI, the daemon,
and the MCP server. This is what lets an agent fork and experiment freely
without being able to vaporize `main` in a single unforced call
([invariant 7](architecture.md#invariants)).

### GC

The two-step cleaner behind fork-spam being the *expected* workload.
**Reap** destroys branches whose TTL expired; **collect** finds storage
objects no live branch's chain can reach (following base pointers, so a
destroyed parent's bytes stay live while a shared child still reads
through them), tombstones them, and deletes them once a grace period
passes (`gc --grace`, default 1h; the daemon janitor's `-gc-grace`,
default 15m). Destroy is instant; the storage refund waits for the last
sharing child ([the full semantics](reference.md#offshoot-destroy-dbbranch---force)).

## Where next

- **I want to run the loop these words describe:**
  [Quickstart](quickstart.md).
- **I want the invariants and failure modes:**
  [Architecture](architecture.md) — especially the
  [invariants list](architecture.md#invariants).
- **I need exact flags and defaults:** [CLI reference](reference.md).
