DRAFT — not posted. Copy for the maintainer to review, edit, and post by hand when the ROADMAP launch gate (one external person completes install → fork → promote without help) is met.

# How offshoot is tested: kill -9, CAS everywhere, and a benchmark nobody ran on SQLite

## HN text post

offshoot branches SQLite files the way git branches code — fork-per-attempt
databases for AI agents and eval harnesses, copy-on-write over a local
directory or an S3-compatible bucket. Before pitching it, we wanted to show
the testing, not just claim it.

The load-bearing test: a stock `sqlite3` CLI writer runs real transactions
against a WAL-mode database while offshoot's capture engine follows the WAL
live. We `SIGKILL` the writer mid-write on roughly half of every round and
bounce the capture engine mid-traffic every 10th round. A 300-second run
drives about 3,500 rounds, kills the writer with `SIGKILL` roughly 1,700
times, and the replica converges to byte-identical `.dump` output after
every single round — zero divergence. It runs nightly on Linux and weekly
on macOS.

We also ran BranchBench (arXiv:2604.17180), a benchmark for database
branching under agentic workloads that hosted Postgres-family systems have
struggled with. Our deep-narrow "mcts" topology — 10 workers x 100 steps,
depth 25 — finished all 1,000 worker-steps in 65.1s wall time, with
eval-query p50 latency at 9.1ms at depth 1 and 10.4ms at depth 25: reads
don't get slower as branches deepen. Per-test isolation: a bare `fork`
stays flat around 9-11ms regardless of database size; open a session and
materialize the checkout and it's 421ms on a 100MB database.

Every writer is CAS-fenced by a lease epoch, one writer per lineage,
always. And since v0.2.11, every tagged release is signed keylessly and
carries SLSA provenance and an SBOM — verify what you actually downloaded.

Full methodology and the honest caveats (what this does *not* prove
included): https://sricola.github.io/offshoot/docs/testing/

## Show HN one-liner

Show HN: offshoot — branch SQLite like git, with a kill -9 torture harness
and a real BranchBench run to back it (recording inside)

## Predictable questions

**Why not Dolt or Neon?** Dolt is a fully versioned, MySQL-compatible
database with branch/diff/merge built into the query engine — genuinely
"git for a database" at the SQL-semantics level. offshoot takes the
opposite bet: keep stock SQLite, unmodified, and put version control at
the storage layer instead, so every checkout is a plain `.db` file any
SQLite tool already understands. Neon does for Postgres roughly what
offshoot does for SQLite — instant copy-on-write branches — but it's
Postgres and it's managed; offshoot targets SQLite specifically (the
engine that embeds next to an agent process with no server to talk to)
and is self-hosted on your own bucket and binary. If you need real
row-level merge or you're already on Postgres, those are the right tools;
if you need SQLite, branchable, offshoot is the fit.

**Why no merge?** offshoot's workload is fork-many-keep-one: try N
approaches on N branches, evaluate them, `promote` the winner whole, and
let the losers TTL away. There's nothing to merge back because an attempt
branch either becomes the branch of record or it dies. This isn't a
missing feature so much as a traded-off one: offshoot's safety story comes
from immutable, append-only lineages with a single fenced writer each, and
row-level merge means synthesizing state from two lineages that diverged
under independent writers — exactly what the single-writer model exists to
make impossible to get wrong. The escape hatch is application-level:
materialize both branches with `offshoot checkout` and reconcile with
`sqldiff` or your own logic outside offshoot.

**Why SQLite only?** SQLite is the database that's trivial to embed
directly next to an agent process or a test runner — no server, no
connection pool, just a file — which is exactly the shape an eval harness
or a fork-per-attempt agent loop wants. Putting the version control at the
storage layer instead of the query engine means we don't have to fork
SQLite itself or give up any of its ecosystem (ORMs, ad hoc `sqlite3`
CLI use, existing tooling) to get branching; every checkout stays a stock
file. That specificity is also the limit: if your database is Postgres
or MySQL, offshoot doesn't help you today.

**Is S3 really safe?** Every lineage has exactly one writer at a time,
enforced by a lease with an epoch that bumps on every acquisition; a
writer that loses its lease and keeps writing lands in a dead epoch prefix
no ref will ever point at, not corruption of the live chain. Every branch
pointer update (fork, checkpoint, promote, rollback, destroy) is a
compare-and-swap naming the exact prior state it replaces — a losing
writer gets a clean error, never a silently dropped update — and offshoot
refuses to attach to any store that can't prove it supports conditional
writes (a CAS probe runs on every attach; that's why we don't support
Google Cloud Storage's S3-interop API). This is the same fencing model
across backends: local filesystem, a fake S3 on every test run, real
RustFS on every PR, and real AWS S3 nightly. Honest caveat: only RustFS
and AWS are independently CI-verified against live storage; other
S3-compatible providers are same-code-path, not separately proven.

**Will the format break?** Before 1.0, yes, the on-disk/bucket storage
format can change in a backward-incompatible way between minor versions —
that's stated plainly in the CHANGELOG. What won't happen silently: any
format break ships in the same release with either an in-place migration
or a documented `export` → `create --from` path, and both halves of that
path are shipped, tested code today, round-tripping a database out of one
store and into a fresh one with identical `.dump` output. Every store also
records a layout version, and a binary that doesn't understand a store's
layout refuses it outright rather than guessing — that gate has already
fired for real once, when v0.2.0's copy-on-write forks introduced layout
version 2. 1.0 is reserved for the point the storage format actually
freezes; see [docs/stability.md](../../stability.md) for the full
contract and proposed v1.0 criteria.
