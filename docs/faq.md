# FAQ: why not X?

offshoot occupies a specific spot: a self-hosted, single-binary tool that
branches *stock SQLite files* over *your own* object storage, for the
server-side, fork-heavy workloads agent platforms and eval harnesses
generate. Most of the "why not X" questions below have a good answer that
isn't "X is bad" — it's usually "X solves a different problem, or requires
adopting something bigger than a binary."

## Why not Litestream?

Litestream replicates one SQLite database continuously to object storage for
disaster recovery and point-in-time restore. It's built for **one lineage**:
a single database, one logical timeline, restored to a moment in time on the
same machine or a standby. It does not branch.

offshoot actually builds on the same ecosystem: it stores data as **LTX**
(Litestream v0.5's transaction-aware segment format, Apache-2.0), and its WAL
capture path is adapted from Litestream's sync engine — the lock dance for
holding a read transaction against a connection you don't own is genuinely
hard to get right, and there was no reason to reinvent it. Credit where due:
without Litestream's format and capture design, offshoot wouldn't exist in
this shape.

What offshoot adds on top: many independent lineages (branches) per
database — forked copy-on-write, so N forks cost near-zero added storage —
with fork/rollback/promote/compact as first-class operations, TTL-based
cleanup, and leases with epoch fencing so concurrent writers can't corrupt
each other. If you need durable backup and PITR for a single database, run
Litestream. If you need N disposable forks of a database for parallel agent
attempts or test runs, that's the gap offshoot fills.

## Why not LiteFS?

LiteFS replicates SQLite across nodes using FUSE, giving you a read-replica
mesh with automatic failover — it solves multi-node availability for a
single logical database. It's also a heavier integration: a FUSE filesystem
in the write path, not a binary you shell out to.

offshoot doesn't do multi-node replication or failover at all (see the
[non-goals](../ROADMAP.md#non-goals-v1) — that's explicitly out of scope for
v1, and we don't use the word "cluster"). It solves a different problem:
branching, not availability. Fly.io sunset LiteFS Cloud, which is a useful
data point about how quickly a managed layer on top of one of these tools can
disappear out from under you — part of why offshoot's answer is "your bucket,
Apache-2.0, one binary," not a hosted service.

## Why not Turso?

Turso Cloud has native database branching and it's a genuinely good product
for the "give me a managed serverless SQLite branch" use case — it's managed,
proprietary infrastructure with instant branching baked in.

offshoot is the self-hosted alternative to that specific feature: a single
binary over a bucket you already control (S3, MinIO, or just a
local directory), producing stock SQLite files you can open with any SQLite
client, no proprietary edge network required. The trade is real — you run and
operate it yourself, and it doesn't give you Turso's global read replicas —
but you also can't be deprecated out from under you the way LiteFS Cloud
customers were, or the way any managed-database customer eventually risks
being. If you want branching as a managed platform feature, Turso is a
reasonable choice. If you want it as infrastructure you own, that's the
wedge here.

## Why not Dolt?

Dolt is a fully versioned, MySQL-compatible database with git-style branch,
diff, and merge built into the query engine itself — a genuinely different
piece of engineering, and the closest thing to "git for a database" that
exists at the SQL-semantics level.

offshoot takes the opposite bet: keep **stock SQLite**, unmodified, and put
the version control at the storage layer instead of the query engine. That
means offshoot can't diff two branches row-by-row the way Dolt can (the
escape hatch is `sqldiff` over two materialized checkouts — see "why no
merge" below), but it also means every offshoot checkout is a plain `.db`
file any SQLite tool already understands, with SQLite's ecosystem, ORMs, and
tooling working unmodified. If you need real row-level version control
semantics in the query language, Dolt is built for that. If you need SQLite
specifically, branchable, offshoot is the fit.

## Why not Neon-style branching?

Neon (and similar Postgres platforms) do for Postgres roughly what offshoot
does for SQLite: instant, copy-on-write branches of a managed database. It's
a strong model, and agent workloads are a meaningful part of why Neon built
it (their own numbers: a large share of their databases and branches are
created by agents).

Two differences: Neon is Postgres, and Neon is managed. offshoot targets
SQLite specifically — the engine that's trivial to embed next to an agent
process or a test runner, not one that needs a server to talk to — and it's
self-hosted: your storage, your binary, no service to depend on staying up
or staying priced the way it is today.

## Why not just `cp`?

For a laptop, `cp app.db app-backup.db` (or a filesystem/VM snapshot) is
completely fine. This isn't a subtle point — offshoot's own design spec names
the individual laptop user as explicitly **not** the target persona. If
that's you, stop reading and go use `cp`.

The pitch is for the case `cp` doesn't cover well: an engineer running N
parallel agent attempts or test runs *server-side*, against real databases,
who needs more than a copy of the bytes:

- **Lineage** — a fork records where it came from and when, not just a blob.
- **Content-addressed refs, CAS'd** — every branch pointer update is a
  conditional write, so concurrent forks/checkpoints/promotes on the same
  store don't race each other into a corrupt state the way parallel `cp`
  invocations against a shared destination could.
- **Leases and epoch fencing** — a writer that pauses and resumes doesn't
  silently clobber newer state.
- **`promote`** — pick a winning attempt and make it the branch of record,
  atomically, instead of manually renaming files around a live path.
- **TTL + garbage collection** — attempt branches clean themselves up instead
  of accumulating as an ever-growing pile of `app-attempt-47-final-v2.db`
  files nobody remembers to delete.
- **Agent integration** — MCP tools, SDKs, and a LangGraph companion so the
  agent (or the framework) drives forking directly, rather than a human
  running `cp` in a loop.

If you don't need those, you don't need offshoot. `cp` is not a strawman
here — it's genuinely the right tool below a certain scale.

## Why not git-annex / DVC-style data versioning?

git-annex and DVC version large files and datasets alongside git — they're
built for tracking how a dataset or model artifact changed over time,
typically for read-mostly, batch workflows (train a model, commit the
weights, diff a dataset revision against another).

offshoot versions **live, writable SQLite databases** that an agent or test
process opens and writes to with a stock SQL client while the daemon captures
transactions continuously in the background. It's built for is fork-and-write
workloads, not fork-and-diff-a-static-blob workloads. If what you're
versioning is a static dataset someone occasionally updates, DVC-style tools
are a better fit; if it's a database an agent is actively mutating right now,
offshoot's daemon and lease model are built for exactly that.

## Why one writer per branch?

Every offshoot lineage — the append-only sequence of storage objects behind
a branch — is written by exactly one writer at a time, enforced by a lease
and an epoch fence (every object write happens under the epoch current when
it was written; a writer that loses its lease and later resumes writes into
a dead epoch prefix that no ref points at — garbage, never corruption). This
isn't a missing feature; it's the invariant the whole safety story rests on.
Concurrent writers to the *same* lineage would mean reconciling interleaved
SQLite WAL frames from two processes — a much harder and much riskier
problem than the one offshoot actually solves. If you want two agents
writing "at once," give them two branches (fork), let them write
independently, and `promote` the one that wins. See
[docs/architecture.md](architecture.md) for the fencing scheme in full.

## Can I merge two branches?

No — offshoot has no row-level merge, deliberately, and it's not on the
roadmap (see [non-goals](../ROADMAP.md#non-goals-v1)). The workload
offshoot is built for — eval harnesses and agent attempts — is
**fork-many-keep-one**: try N approaches on N branches, evaluate them,
`promote` the winner whole, and let the losing attempts TTL away. Attempts
are disposable by design; there is nothing to merge back because the whole
point of an attempt branch is that it either becomes the branch of record
or it dies.

The reason merge is out isn't that it's hard to build (it is), it's that
it would forfeit the guarantees everything else here rests on: offshoot's
safety story comes from immutable, append-only lineages with a single
fenced writer each — three-way row merge means synthesizing new state from
two lineages that diverged under independent writers, exactly the
situation the single-writer model exists to make impossible to get wrong.
Real merge would also mean solving schema conflicts, constraint
violations, and semantic conflicts (two branches both "correctly" updating
the same row differently) with no general solution.

If you actually need row-level merge across concurrent writers — long-lived
branches, human collaborators, reconciliation as a first-class operation —
you want [Dolt](#why-not-dolt): that's the problem it's genuinely built
for, at the cost of adopting a whole versioned database engine instead of
stock SQLite files. The escape hatch inside offshoot is application-level:
materialize both branches (`offshoot checkout`) and reconcile with
`sqldiff` or your own logic outside offshoot. offshoot gives you the two
checkouts to work from; it doesn't pretend to resolve the conflict for
you.

## Why is durability explicit instead of automatic?

It's both, and both are reported rather than assumed. A daemon session's
writes are captured continuously into SQLite's WAL; they become durable in
the store on an explicit `flush` (or CLI `checkpoint`), **and** — by
default — on the daemon's background flush timer (`serve -flush-every`,
default `30s`; `0` disables it). `session status` reports exactly which
transaction each session is durable through, so "durable" is a number you
can check, not a guess.

The trade-off, stated plainly: a daemon that dies between flushes loses
the writes after the last one — acknowledged to SQLite, never mirrored to
the bucket. The default timer bounds that exposure to at most one
`-flush-every` interval; call `flush` explicitly whenever your durability
requirements are tighter than the cadence.

## Why no Windows support?

The WAL capture path and the lock/SHM coherence probing it depends on are
POSIX-specific — they lean on file-locking semantics Windows doesn't provide
the same way. This is a real engineering gap, not a policy choice, and it's
named plainly as a non-goal rather than glossed over. Containers with bind
mounts work fine (Linux/macOS host); virtiofs, NFS, and gVisor-style microVM
boundaries don't, for the same reason — the daemon and the writer need to
share a real POSIX filesystem and kernel.

## Why no Google Cloud Storage?

GCS's S3-interoperability API doesn't support conditional writes
(compare-and-swap), and CAS on every ref update is the mechanism the whole
single-writer/no-corruption story is built on. offshoot's store-attach probe
checks for this and refuses to run against a store that can't provide it,
rather than silently degrading to a weaker guarantee. If GCS ever exposes
CAS semantics through an API offshoot can use, this is revisitable; today
it's a hard "unsupported," stated as such rather than left to a confusing
runtime failure.

## Storage cost, honestly

Since v0.2.0, forks are **copy-on-write**: a fork writes a base pointer
into its parent's already-durable chain and adds objects only as it
diverges, so N forks of a G-byte database cost near-zero added bytes in
your bucket — not the N×G the original design paid. The fork-heavy attempt
workload is the one this was built for, and it's now near-free at the
storage layer.

What still costs real bytes, stated plainly:

- **`promote`, `rollback`, and `compact` each materialize a full
  independent copy** (~G for a G-byte database). Fork is free; picking a
  winner isn't. This asymmetry is deliberate — those operations abandon or
  replace a lineage, and base-pointing into a lineage that is meant to die
  would pin it forever.
- **A destroyed parent's bytes linger while shared children survive.**
  Destroy is instant, but GC reclaims a shared ancestor's storage only
  once no surviving child's chain still reads through it — destroy or
  `compact` the children to get the refund.
- **Deep fork spines occasionally pay a snapshot floor**: a fork whose
  resolved chain is already at the depth bound materializes one fresh
  snapshot instead of sharing, keeping reads bounded.

TTLs remain the cleanup mechanism for attempt branches
(`offshoot fork app attempt-1 --ttl 2h`); `offshoot status` reports every
branch's cost class (`storage=shared` vs `storage=materialized`) so the
bill is visible per branch. Page-level content-addressed dedupe — sharing
across *unrelated* databases, or sub-object pages — remains a non-goal
(see the roadmap's [non-goals](../ROADMAP.md#non-goals-v1)): copy-on-write
shares whole objects between a fork and its own ancestors only.
