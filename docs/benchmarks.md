# Benchmarks

## Copy-on-write fork cost (v0.2.x)

Measured numbers for the claim the copy-on-write fork actually makes:
**forking is storage-O(1) — a shared fork writes two tiny objects (the
child lineage's `base.json` + the branch ref), regardless of database
size — and a child pays only for what it changes.** Everything in this
section was measured on this machine, at v0.2.7 + this harness
(2026-08-12); every other section of this document predates copy-on-write
(v0.1.x measurements) and describes the **materialize** path — see the
version note below it.

**Machine:** darwin/arm64, Apple M4 (10 cores), 16 GiB RAM, macOS 27.0
(build 26A5388g), local APFS disk, Go 1.26.5. Local directory store
backend (the default `make bench` target's backend). No other load.

**Reproduce:** `make bench-cow` (benchmarks live in
`internal/ops/cow_bench_test.go`; the target's invocations are exactly the
ones below). Latency numbers are the **median of 5 samples**
(`-count=5 -benchtime=3x`, each sample the mean of 3 iterations); byte
numbers are exact object-store accounting (sum of stored object sizes,
diffed before/after — what an S3 backend would bill), identical across
repeat runs.

### Shared fork latency vs database size

| DB size | `Fork` at head (default), median | `Fork` at named checkpoint, median |
|---|---|---|
| 12 MB | 17.0 ms | 11.7 ms |
| 100 MB | 50.3 ms | 11.3 ms |
| 1 GB | 418.0 ms | **9.3 ms** |

The share path itself (`at=checkpoint`) is **near-constant, ~9–12 ms from
12 MB to 1 GB** — it writes two small objects and never touches the
database's bytes. The honest flag on the default call shape: `Fork` at
head (`at=""`) **still grows with size** — not from the share, but from
the pre-existing safety check that SHA-256-hashes the whole checkout file
to warn about uncheckpointed changes (~2.6 GB/s on this machine; the same
O(size) check documented in "What's still O(size) after Task 6a" below).
So "O(1) fork" is true of the storage work and of forking a named
checkpoint, and NOT yet true of a default at-head fork's wall-clock
latency. Raw samples:

```
BenchmarkCoWSharedFork/size=12MB/at=head-10          3  13807180 ns/op ... (5 samples: 13.8/17.1/21.3/17.0/13.6 ms)
BenchmarkCoWSharedFork/size=12MB/at=checkpoint-10    3  10676945 ns/op ... (10.7/9.3/12.2/11.7/12.7 ms)
BenchmarkCoWSharedFork/size=100MB/at=head-10         3  47628208 ns/op ... (47.6/53.3/50.3/51.0/48.6 ms)
BenchmarkCoWSharedFork/size=100MB/at=checkpoint-10   3   8351125 ns/op ... (8.4/11.3/12.8/10.6/13.2 ms)
BenchmarkCoWSharedFork/size=1GB/at=head-10           3 418239347 ns/op ... (418.2/409.3/410.6/418.4/418.0 ms)
BenchmarkCoWSharedFork/size=1GB/at=checkpoint-10     3   8028959 ns/op ... (8.0/8.9/9.3/13.0/13.0 ms)
```

### Added object-store bytes per fork (100 MB database)

| Fork kind | N forks | Added bytes per fork | Added objects per fork |
|---|---|---|---|
| Shared (CoW, the default) | 1 | 377 B | 2 |
| Shared (CoW, the default) | 10 | 377 B | 2 |
| Shared (CoW, the default) | 100 | 376.9 B | 2 |
| Materialized (forced via test hook — the pre-CoW behavior / fork-time floor) | 1 and 10 | 105,807,581 B (~105.8 MB, one full snapshot) | 2 |
| Plain `cp` of the SQLite file (no offshoot) | — | the full file (~105.8 MB) | — |

A shared fork of a 100 MB database adds **377 bytes** — one `base.json`
(the durable base pointer) plus one branch ref — about **280,000× less**
than a materialized fork or a `cp`, and flat from N=1 to N=100. For
latency context, plain `cp` of the same 105.8 MB file took a median
**43.5 ms** (2.4 GB/s full byte copy) vs the share path's ~11 ms.

```
BenchmarkCoWSharedForkAddedBytes-10         100  47355148 ns/op  376.9 addedBytes/fork  2.000 addedObjects/fork
BenchmarkCoWMaterializedForkAddedBytes-10    10  47614808 ns/op  105807581 addedBytes/fork  2.000 addedObjects/fork
BenchmarkCoWCpBaseline-10                     3  42279986 ns/op  2483.08 MB/s   (5 samples: 42.3/44.5/43.5/57.7/40.2 ms)
```

### Divergence cost: what a shared child pays as it writes

Fork a 100 MB database (share), pre-materialize the child's checkout (so
the settling-flush suppression arms and flushes are segments — the
steady-state, see `cow_divergence_test.go`), then write k single-row
transactions with one `Flush` each at the default snapshot cadence
(`SnapshotEvery=16`). Identical across 3 repeat runs:

| k (transactions) | Added bytes total | Added bytes per transaction |
|---|---|---|
| 8 (stays under the cadence) | 6,209 B | **776 B** |
| 20 (crosses the cadence once) | 105,822,553 B | 5.29 MB amortized |

Under the cadence, divergence is **O(changed pages)**: ~776 B per
single-row transaction against a 100 MB database — segments, the ref
update, nothing size-proportional. (The 6,209 B numerator also includes
the fork's own one-time 377 B of `base.json` + ref; the pure per-segment
cost is ~729 B/txn — 776 is the conservative all-in figure.) The honest
second row: every `SnapshotEvery`-th flush (16th, by default) writes a
**full self-snapshot** (the divergence floor that keeps read chains
bounded), which is O(DB) — 105.8 MB here. Its latency also shows up in
the loop timings below: k=20 runs ~0.70 s longer than k=8, of which the
12 extra segment flushes (~14 ms each) explain only ~0.17 s — the
remaining ~0.5 s is the snapshot flush. Steady-state cost is therefore
~729 B/txn plus one full snapshot per 16 flushes.

```
BenchmarkCoWDivergenceAddedBytes/k=8-10    1   726884166 ns/op       6209 addedBytes/child     776.1 addedBytes/txn
BenchmarkCoWDivergenceAddedBytes/k=20-10   1  1428649333 ns/op  105822553 addedBytes/child   5291128 addedBytes/txn
```

### Read-path sanity: checking out a shared fork

Full checkout materialization from scratch (checkout file + sidecar
deleted each iteration; 100 MB database; median of 5):

| Branch | Checkout (full materialize), median |
|---|---|
| Parent (own snapshot) | 610.0 ms |
| Shared child (resolves through `base.json` into the parent's chain) | 357.7 ms |

The shared child's checkout is in the same regime as the parent's — the
bounded-replay claim holds at this shape (the child's resolved chain is
the parent's snapshot plus zero own segments, one extra `base.json` read).
Run-to-run spread is wide (parent samples 421–689 ms; child 336–539 ms),
so read the table as "no read penalty for a shared fork beyond noise,"
not as "children are faster." The child-lower medians are likely an
ordering artifact — the parent sub-benchmark always runs first, so the
child materializes the same snapshot object with the page cache already
warm; the spread, not the ordering, is the result.

### Caveats (read before quoting these numbers)

- **Local backend, APFS, one machine.** S3 adds per-request network
  latency to every number here, but the **object math is identical by
  construction**: a shared fork issues the same two small writes
  (`base.json` + ref) against S3 — no snapshot copy, no data-plane
  traffic — so the "377 B / 2 objects per fork" accounting carries over
  even though the milliseconds do not.
- Added bytes are **logical stored-object bytes** (what S3 would bill).
  On APFS the local backend's materialize path clones (`clonefile`), so
  the materialized fork's *physical* local disk usage is far below its
  105.8 MB logical footprint — the logical number is the one that
  transfers to a real object store.
- Default at-head `Fork` latency is O(size) (the uncheckpointed-changes
  hash), not O(1) — see the latency table's note.
- A shared child's write path snapshots in full every `SnapshotEvery`
  (default 16) flushes — divergence is O(changed pages) *between* those
  floors, not unconditionally.
- Seeded databases are freshly checkpointed single-snapshot chains — the
  share path's common case. Forking a branch whose resolved chain has
  reached the fork-time floor (`ForkShareMaxDepth`, or the configured
  session cadence) materializes instead, at the "Materialized" row's cost.

---

Everything below this line was measured **before** copy-on-write existed
and is preserved as published; the next section's version note scopes it.

Measured baselines for `ops.Workspace.Fork`, `Checkout`'s clean-skip fast
path (Task 1 of Milestone 2), and `session.Open` — **before** and **after**
Task 6a's fast-path fork. The point of this document is to make the
before/after comparison honest: these are real numbers from real runs, not
estimates. The "before" column is exactly what was measured and published
prior to Task 6a; it is kept, not replaced, so the comparison is checkable.

> **Version note — every number below was measured on a pre-copy-on-write
> build (the v0.1.x line, 2026-08-06), and is kept as measured rather than
> re-invented.** Since v0.2.0 the default `fork` **shares** (a base
> pointer, no object copy at all), so the `ForkAtHead` numbers below no
> longer describe the common-case fork's storage work — they describe the
> **materialize path**: a fork that trips the fork-time snapshot floor,
> and every `promote`/`rollback`/`compact`, all of which still use exactly
> the snapshot-copy machinery measured here. The O(size)
> uncheckpointed-changes check these numbers are dominated by (see
> "Isolating the fast path itself") still runs on a shared fork at head
> too. The `CheckoutCleanSkip` and `SessionOpen` numbers are unaffected by
> the copy-on-write change. Also superseded since measurement: S3
> `CopyObject`'s 5 GiB ceiling is now a strategy boundary, not a limit —
> v0.2.4 added multipart `UploadPartCopy` for sources up to S3's 5 TiB
> per-object ceiling, so the ">5GB falls back to materialize" behavior
> described below no longer applies (only >5 TiB falls back).

Benchmarks live in `internal/ops/fork_bench_test.go`. Run them with
`make bench` (local store) or `make bench-s3` (real MinIO in Docker). The
benchmark code itself is unchanged by Task 6a — same subtest names, same
seeding, same `at=""` (fork at branch head, which runs `Fork`'s normal
uncheckpointed-changes check; see "What's still O(size) after Task 6a"
below) — so before/after numbers are a like-for-like comparison of the same
call, not two different things being measured.

## Per-test isolation primitives (v0.2.11)

An eval harness needs a fresh, isolated database per test (see
[docs/eval-harness.md](eval-harness.md)). This section puts offshoot's own
fork side by side with the plain-file and Postgres alternatives a harness
author might reach for instead, measured on this machine with
`scripts/bench-isolation.py` (`make bench-isolation`; stdlib + `sdk/python`
on `sys.path`, `docker` CLI only for the Postgres rows — skipped cleanly,
with a note, when Docker or a local `postgres:16` image isn't available;
`--dry-run` validates tooling and prints the plan without doing any timed
work).

**What each row is:**

- **`offshoot fork` + `open` + `close` (SDK):** the full per-test cost via
  the Python SDK, against an `eval-bench` database seeded once via
  `Client.create(db, from_path=...)` (Task 1's SQLite import) — fork a
  fresh branch, open a live session on it, close it. This is what a test
  pays if it opens a session at all (e.g. via `offshoot_fork()` / the
  pytest fixture), even if it never writes.
- **`offshoot fork` alone:** the same fork, no session opened — isolates
  the pure branch-creation cost from what `open` adds on top.
- **`sqlite3.Connection.backup()`:** Python's built-in online-backup API,
  copying the seed file into a fresh file.
- **`shutil.copyfile`:** a plain byte-for-byte copy of the seed file.
- **Postgres `CREATE DATABASE trial TEMPLATE seed`:** clones a database
  from a template inside a local `docker run -d postgres:16` container
  (`DROP DATABASE` between iterations); the seed is built with
  `generate_series` to approximately the stated size, verified via
  `pg_total_relation_size`.
- **Postgres cold container start:** `docker run -d postgres:16` until
  `pg_isready`, then `docker rm -f` — the cost of *not* keeping a
  container warm between tests. Measured separately, only 3 iterations
  (it's slow, and independent of seed size).

**Machine:** darwin/arm64, Apple M5, macOS 27.0 (build 26A428), Docker
29.8.0 (Postgres runs inside Docker Desktop's Linux VM here, not
natively), Go 1.27.1, local-directory store backend, no other load, no
network. Measured 2026-09-26. Raw output of `make bench-isolation`
(`--sizes 10,100 --iters 20`), pasted verbatim:

| Primitive | 10 MB | 100 MB |
|---|---|---|
| `offshoot fork` + `open` + `close` (SDK) | 68.34 / 81.67 | 420.87 / 435.87 |
| `offshoot fork` alone (no session) | 9.63 / 11.67 | 9.41 / 10.76 |
| `sqlite3.Connection.backup()` | 9.39 / 11.58 | 93.06 / 102.28 |
| `shutil.copyfile` | 0.91 / 0.99 | 10.93 / 90.85 |
| Postgres `CREATE DATABASE trial TEMPLATE seed` | 57.28 / 60.84 | 119.91 / 127.51 |

Postgres cold container start (`docker run -d postgres:16` until `pg_isready`; N=3, size-independent): **355.88 / 367.67 ms** (median / p95)

docker exec no-op (SELECT 1) overhead (same `docker exec ... psql` round trip the template-clone row above pays; N=20, size-independent): **37.04 / 41.73 ms** (median / p90)

`pg_isready` -> first successful `SELECT 1` gap (measured inside the cold-container start above; N=3, size-independent): **250.79 / 255.32 ms** (median / p90)

Values are `median / p95` milliseconds over 20 iterations (3 for the cold-container row), except the docker-exec-no-op and pg_isready-gap lines above, which report `median / p90`.
Postgres seed sizes (`pg_total_relation_size`): 10 MB target -> 10.6 MiB actual, 100 MB target -> 105.2 MiB actual.
SQLite seed sizes (actual file size): 10 MB target -> 10.0 MiB actual, 100 MB target -> 100.1 MiB actual.

**Caveats:**
- Single host, single run, not a fleet average: all rows were measured back-to-back on the same machine (see the machine line above this table in the docs) with no other load; run-to-run variance on a laptop is real.
- Apple Silicon macOS: the offshoot daemon and SQLite rows run natively, but Postgres only runs in Docker Desktop's Linux VM here — its numbers include a virtualization/VM-boundary tax a native Linux host would not pay.
- offshoot uses its local-directory store backend (not S3) for this run.
- No network access is used or required by this script; the postgres:16 image must already be present locally or the Postgres rows are skipped.

**Why the SQLite rows are near-constant in size and the Postgres row is
not:** `offshoot fork` alone stays flat (~9 ms either way) because a
shared CoW fork writes two small objects — a base pointer and a branch
ref — regardless of database size (see "Copy-on-write fork cost" above);
nothing about the fork touches the database's bytes. `sqlite3.Connection.
backup()` and `shutil.copyfile` scale close to linearly with size, as
expected, because both physically copy every byte (`backup()`: 9.39 ms ->
93.06 ms, roughly the 10x size ratio; `copyfile` is under a millisecond at
10 MB, so page-cache noise dominates its p95 there). The two rows are not
an apples-to-apples I/O comparison, though: `backup()` opens its
destination connection with SQLite's default `synchronous=FULL`, so every
`.backup()` call fsyncs the destination as part of committing — it asks
the OS for durability. `shutil.copyfile` never fsyncs, so it asks for
none. That asymmetry accounts for part of the gap between the two rows,
on top of the difference in what each one physically does. Postgres's
`CREATE DATABASE ... TEMPLATE` also physically copies the template's
files — PostgreSQL 16's default `STRATEGY = WAL_LOG` copies the template
block by block through WAL regardless of filesystem, rather than taking a
filesystem-level reflink/CoW shortcut — but the row's *absolute* numbers
at these sizes are dominated by the fixed cost of the `docker exec ... psql` round trip
itself: a no-op `SELECT 1` through the same path, measured directly
above, costs **37.04 ms** median (p90 41.73 ms) alone on this host. Only
the roughly 63 ms difference between the two sizes' medians (57.28 ->
119.91 ms) is the actual template-clone work; that delta does scale with
size, it's just swamped by per-command overhead at 10 MB.

**The honest fork-versus-open statement:** `offshoot fork` by itself is
the flat, near-constant row above (~9 ms). The `fork` + `open` + `close`
row is roughly 7.1x slower at 10 MB and roughly 44.7x slower at 100 MB —
not because forking got more expensive, but because of what `open` (and
`close`) does that `fork` doesn't: a real daemon round trip, and — since a
freshly forked branch has no checkout materialized locally yet (the fork
itself only wrote a base pointer + ref) — `Open`'s first `CheckoutProven`
call has to materialize the branch's actual database bytes to local disk
before handing back a live session, on top of the settling-flush checksum
machinery described in "Settling-flush cost" below. That materialization
is real per-byte I/O, which is why the `fork` + `open` + `close` row,
unlike bare `fork`, is NOT size-independent (68 ms at 10 MB vs. 421 ms at
100 MB). `Session.Close` is not free either — it drains the capture
engine and, on a provably clean close, re-stamps the checkout's sidecar
(see `docs/status.md`'s sidecar-refresh row) — so this row's overhead is
not solely `Open`'s materialization cost, though materialization dominates
it at these sizes. A test that only forks and never opens a session pays
the flat ~9 ms row instead of this one.

**A note on the cold-container number:** the Postgres image's entrypoint
briefly runs a temporary, setup-only server on the same socket before the
real server starts, and `pg_isready` (used here, per this row's own
definition) can report ready during that window. A follow-up check this
script uses elsewhere, before it runs any real SQL against a container it
just started (an actual `SELECT 1`, retried until it succeeds), measured
directly above as its own line, took another **250.79 ms** median (p90
255.32 ms) past `pg_isready`'s signal on this host — note that this figure
is measured via `_wait_pg_queryable`, which re-checks `pg_isready` (already
satisfied at that point) before it starts polling `SELECT 1`, so the
measured gap includes one redundant `pg_isready` docker-exec round trip and
slightly overstates the pure readiness-to-queryable gap. Both numbers are
still comfortably sub-second here — this container was already locally
cached (no image pull) and mounts no volume; a colder path (a network
pull, a mounted volume, a busier host) would cost meaningfully more.

## BranchBench topologies (v0.2.11)

BranchBench ("Aligning Database Branching with Agentic Demands" — Elaine Ang,
In Keun Kim, Sam Weldon, Kevin Durand, Kostis Kaffes and Eugene Wu, of
Columbia University's DAPLab, [arXiv:2604.17180](https://arxiv.org/abs/2604.17180))
is a benchmark for database *branching* under agentic workloads: five
macrobenchmark workflows, each a parameter tuple over one tree-building loop —
fork a branch, mutate it, evaluate it, sometimes prune it — run against hosted
branchable databases. Its harness is at
[github.com/ElaineAng/db-fork](https://github.com/ElaineAng/db-fork).

**What we ran.** `cmd/branchbench` (`make bench-branchbench`) re-runs those
five parameter tuples against a local offshoot store. The harness repository
carries no license file, so none of it is vendored here: we took the *numbers*
below — BranchBench's own notation: T workers, S steps per worker, F_r root
fanout, F_i inner fanout, D max depth, C cross-branch queries for the run, γ
prune probability, M_s schema changes per step, M_d data mutations per step,
Q_v eval queries per step — and wrote our own generator, schema and SQL around
them.

| Workflow | T workers | S steps/worker | F_r | F_i | D | C cross-branch | γ prune | M_s schema/step | M_d mutations/step | Q_v eval/step |
|---|---|---|---|---|---|---|---|---|---|---|
| `simulation` (flat star) | 1000 | 1 | 1000 | – | 1 | 1 | 1.0 | 0 | 50 | 1 |
| `data_cleaning` (wide shallow) | 10 | 20 | 10 | 3 | 3 | 2 | 0.0 | 1 | 1 | 1 |
| `software_dev` (bushy) | 5 | 20 | 5 | 3 | 4 | 1 | 0.1 | 1 | 1 | 2 |
| `mcts` (deep narrow) | 10 | 100 | 10 | 10 | 25 | 0 | 0.1 | 0 | 1 | 1 |
| `failure_repro` (flat, 1 worker) | 1 | 10 | 10 | – | 1 | 0 | 1.0 | 5 | 45 | 1 |

That is 2,310 forks in all (one per worker-step). The whole table takes **under
3 minutes** on the machine below and needs **about 30 GB of free space in
`$TMPDIR`** — see the per-workflow stores below. `go run ./cmd/branchbench
-quick` is a seconds-scale smoke run of the same five topologies (it is what
`go test ./cmd/branchbench` runs).

- **Every workflow gets its own store.** Each one runs against a fresh store
  directory with its own `ops.Init` and its own seed, and that directory is
  removed before the next workflow starts (`-keep` keeps them; an interrupted
  run removes them on the way out). The rows are therefore independent: no
  workflow's latencies or bytes are measured against the branches an earlier
  one left lying around. It also bounds the disk needed to the largest single
  workflow — `mcts` peaks at 26.5 GiB, hence the ~30 GB above, against well
  over 45 GB if the five shared one store.
- **The seed** is our own CH-benCHmark-*shaped* generator
  (`cmd/branchbench/seed.go`), not BranchBench's SQL dump: TPC-C's
  transactional tables (`warehouse`, `district`, `customer`, `item`, `stock`,
  `orders`, `order_line`, `new_order`, `history`) plus TPC-H's
  `region`/`nation`/`supplier` dimension tables, at the scale BranchBench's
  configs describe (10 warehouses, 1K items, 100 customers per district —
  10,000 customers, 10,000 orders, ~100,000 order lines). It is deterministic
  (`math/rand` seeded 1), which is what makes the per-workflow stores
  comparable — every workflow starts from a byte-identical **17 MiB** database
  — and the program fails loudly if two seeds ever come out different sizes.
  Within a workflow, the root of the tree is `chbench@main`, whose head never
  moves after the seed checkpoint.
- **No daemon, no network.** Each step is `Fork` → `Checkout` → plain
  `database/sql` work on the checkout file → `Checkpoint`, straight against
  `internal/ops` (the in-process API the CLI uses). The checkpoint is what
  makes the loop a *tree*: a child forks from its parent's published state, so
  the parent's writes have to be snapshotted into the store before any child
  can fork from them. A node becomes eligible as a parent only once its own
  step has finished. Workers are goroutines through a semaphore
  (`-concurrency`, default 8); one mutex guards the tree.
- **Parent selection, and what happens when a tuple outgrows its tree.**
  BranchBench's rule is: the worker's current node if it still has depth and
  fanout left, else a uniformly random node that does, else the root. Two of
  these tuples ask for more branches than their own fanouts and depth allow —
  `data_cleaning` runs T×S = 200 steps against a tree that F_r=10, F_i=3, D=3
  caps at 130 nodes — so the rule's last clause fires by construction and the
  surplus forks off the root. That is the specified behaviour, not a fallback
  of ours, but it does mean such a workflow ends with more children at depth 1
  than F_r and more live branches than its fanouts alone would allow; the
  per-workflow lines print how many forks took that path.
- **Cross-branch queries are aggregated in the driver** — for each live
  branch, check it out, sum `ol_amount` read-only, total it up in Go — which
  is what BranchBench's harness does too. [DoltHub's critique of the
  benchmark](https://www.dolthub.com/blog/2026-06-03-branch-bench-database-benchmarking-for-agentic-workflows/)
  makes the point that this "live[s] as global knowledge in the driver rather
  than leveraging Dolt's native SQL-layer cross-branch support"; offshoot has
  no cross-branch SQL to undersell, so the driver-side shape is a fair match
  here — on a system that has one, it would not be. This pass is timed
  separately (the per-workflow lines) and counts in wall time, but not as
  branch management.
- **Branch management** means `fork` + `checkout` + `checkpoint` + `destroy`.
  The "Branch overhead" column is that time summed across workers over the
  run's worker-time budget: wall × *effective* concurrency, where effective
  means `min(-concurrency, T)` — a workflow with fewer workers than
  `-concurrency` can never use the whole semaphore, and dividing by the flag
  would understate its overhead badly (`failure_repro` has one worker). At
  concurrency 1 this is exactly BranchBench's "fraction of wall-clock time
  spent on branch management". `synchronous=OFF` is set on the step
  connections, which makes the *productive* SQL cheaper and so the overhead
  ratio larger: the conservative direction for us.
- **Latencies** are p50/p99 per operation at depth 1 and at the deepest depth
  the workflow reached, so a depth penalty would be visible. With these
  tuples most (operation, depth) cells hold well under 100 samples, and there
  a p99 is simply the slowest sample — the counts are on the per-workflow
  lines, and the note under the table repeats the caveat.
- **`Store peak`** is the largest the workflow's store directory got, sampled
  while it ran and once at the end. For a local store the materialized
  checkouts live inside that directory, so it counts both store objects and
  one checkout file per live branch. **`Peak live`** counts live *forked*
  branches, excluding the root; for the γ=1.0 workflows, which prune every
  branch immediately after evaluating it, that number measures in-flight
  concurrency rather than accumulation — worth keeping in mind next to Neon's
  20-live-branch ceiling quoted below.

**Machine:** as the header line below reports — darwin/arm64, Apple M5, macOS
27.0, local-directory store backend, no other load, no network, measured
2026-09-26. Raw stdout of `make bench-branchbench` with its defaults (all five
workflows, concurrency 8, 10 warehouses, 2h per-workflow cap), pasted
verbatim, minus make's own echoed `go run` line:

darwin/arm64, Apple M5, 10 cores, Go 1.27.1, offshoot dev-329fc05-54-g1057bd1, measured 2026-09-26, seed 17 MiB, concurrency 8

| Workflow | Steps | Wall | Branch overhead | Fork p50/p99 (d=1 → d=max) | Checkout p50/p99 (d=1 → d=max) | Checkpoint p50/p99 (d=1 → d=max) | Eval p50/p99 (d=1 → d=max) | Peak live | Store peak |
|---|---|---|---|---|---|---|---|---|---|
| simulation | 1000/1000 | 73.9 s | 88% | 73.2/136.7 → (d=1 is max) | 316.4/381.4 → (d=1 is max) | 119.5/162.0 → (d=1 is max) | 11.4/17.5 → (d=1 is max) | 8 | 12.0 GiB |
| data_cleaning | 200/200 | 17.8 s | 71% | 46.2/58.9 → 39.4/76.2 | 335.7/365.6 → 347.4/416.6 | 136.0/151.2 → 132.1/161.0 | 8.3/14.4 → 7.1/21.7 | 200 | 6.1 GiB |
| software_dev | 100/100 | 7.8 s | 81% | 35.8/39.9 → 29.5/46.3 | 178.0/181.7 → 180.4/211.8 | 102.9/109.9 → 105.8/130.9 | 12.6/16.6 → 12.5/17.9 | 84 | 2.8 GiB |
| mcts | 1000/1000 | 65.1 s | 96% | 52.8/59.9 → 77.0/141.0 | 331.0/377.2 → 312.2/379.6 | 138.3/150.8 → 111.8/146.2 | 9.1/9.7 → 10.4/18.6 | 890 | 26.5 GiB |
| failure_repro | 10/10 | 1.9 s | 73% | 15.3/15.7 → (d=1 is max) | 49.2/60.9 → (d=1 is max) | 70.3/72.4 → (d=1 is max) | 5.1/5.1 → (d=1 is max) | 1 | 167 MiB |

Latencies are milliseconds. p99 is the maximum sample wherever a cell has fewer than 100 samples at that depth, which is most of them — the per-workflow lines below give the counts. Branch overhead is fork+checkout+checkpoint+destroy time summed over workers, over wall x effective concurrency (min(-concurrency, T workers)). Peak live counts live forked branches, excluding the root.

- `simulation` (flat star; T=1000, S=1, F_r=1000, F_i=0, D=1, C=1, γ=1.0, M_s=0, M_d=50, Q_v=1): max depth reached 1; 1 cross-branch query over 1 live branch (the root included) in 9 ms; all 1000 steps landed at d=1; store ended at 12.0 GiB of which 17 MiB is the seed; branch-management time 522.7 s summed over workers (7.1x wall at effective concurrency 8 of 8 requested); 0 CAS retries; store sizes approximate: 4 entries vanished or were unreadable during the size walks
- `data_cleaning` (wide shallow; T=10, S=20, F_r=10, F_i=3, D=3, C=2, γ=0.0, M_s=1, M_d=1, Q_v=1): max depth reached 3; 2 cross-branch queries over 201 live branches (the root included) in 4.1 s; 20 steps landed at d=1 and 129 at d=3 (the sample counts behind those two p50/p99 pairs); store ended at 6.1 GiB of which 17 MiB is the seed; 10 forks fell back to the root after the tree filled; branch-management time 101.3 s summed over workers (5.7x wall at effective concurrency 8 of 8 requested); 0 CAS retries
- `software_dev` (bushy; T=5, S=20, F_r=5, F_i=3, D=4, C=1, γ=0.1, M_s=1, M_d=1, Q_v=2): max depth reached 4; 1 cross-branch query over 84 live branches (the root included) in 862 ms; 5 steps landed at d=1 and 50 at d=4 (the sample counts behind those two p50/p99 pairs); store ended at 2.8 GiB of which 17 MiB is the seed; branch-management time 31.4 s summed over workers (4.0x wall at effective concurrency 5 of 8 requested); 0 CAS retries
- `mcts` (deep narrow; T=10, S=100, F_r=10, F_i=10, D=25, C=0, γ=0.1, M_s=0, M_d=1, Q_v=1): max depth reached 25; no cross-branch queries (C=0); 10 steps landed at d=1 and 78 at d=25 (the sample counts behind those two p50/p99 pairs); store ended at 26.5 GiB of which 17 MiB is the seed; branch-management time 500.5 s summed over workers (7.7x wall at effective concurrency 8 of 8 requested); 0 CAS retries
- `failure_repro` (flat, 1 worker; T=1, S=10, F_r=10, F_i=0, D=1, C=0, γ=1.0, M_s=5, M_d=45, Q_v=1): max depth reached 1; no cross-branch queries (C=0); all 10 steps landed at d=1; store ended at 167 MiB of which 17 MiB is the seed; branch-management time 1.4 s summed over workers (0.7x wall at effective concurrency 1 of 8 requested); 0 CAS retries

Every workflow completed every step: 2,310/2,310 worker-steps, 166.5 s of
workflow wall time in total (2 m 50 s for the whole `make` invocation,
including a seed build per workflow), no CAS retries, nothing aborted or timed
out.

**What BranchBench found on hosted systems.** For context — these are the
authors' numbers on hosted Postgres-family systems, not ours:

- From the abstract: "systems optimized for fast branching suffer up to
  5-4000x slower reads as branches deepen, while systems optimized for fast
  data operations incur 25-1500x higher branch creation and switching
  latency." The paper states those ranges across systems and topologies; it
  does not attribute either end of either range to a particular topology (the
  two figures below do name theirs).
- Dolt, on the deep-narrow MCTS topology (10 workers × 100 steps), completed
  **170/1000** worker-steps before the harness's 2-hour cap — its published
  `run_stats_final` records `timed_out: true` at `elapsed_sec: 7581.85`.
- Neon's cloud service "does not support more than 20 concurrent 'live'
  branches", which is what ends its MCTS run rather than the clock:
  [DAPLab's write-up](https://daplab.cs.columbia.edu/general/2026/05/26/branchable-databases-arent-ready-for-agentic-workloads.html)
  reports **33/1000** worker-steps. (The harness's raw `neon_full` MCTS stats
  file sums its per-worker `completed_steps` to 23, not 33. We cite the
  published figure and flag the discrepancy rather than reconcile it.)
- And the headline: "no system was able to fully complete the five agentic
  applications within the 2 hours."

**Not apples to apples.** Those are the authors' runs on hosted, network-
attached, multi-tenant Postgres-family services. Branch "creation" there
includes control-plane work and compute provisioning, and every measurement
carries WAN round trips, TLS, connection pooling and provider-side rate
limiting — BranchBench's own "mini" configs exist to stay inside Neon's
20-branch and API-rate ceilings. Ours is a local file store: no network, no
compute to provision, no other tenants, a different seed, a different
concurrency model (goroutines over one process's checkout files, not clients
over a server), and a storage engine whose byte accounting is not on the same
curve as either reference system's. Our numbers sit next to theirs because the
*shape* of the workload is the same, not because the systems are comparable.
Finishing all five in minutes is not evidence that offshoot "beats" Dolt or
Neon at anything they were measured on; it is what these five topologies cost
on a local copy-on-write SQLite store.

**What the numbers do and do not show.**

- **Reads do not get slower with depth.** That is the axis BranchBench's
  "5-4000x slower reads as branches deepen" finding lives on, and it is why
  the table reports every latency at depth 1 *and* at the deepest depth
  reached. `mcts` eval p50 is 9.1 ms at depth 1 and 10.4 ms at depth 25;
  `data_cleaning` 8.3 → 7.1 ms, `software_dev` 12.6 → 12.5 ms. A branch's
  checkout is a plain SQLite file, so once it is materialized the store is not
  in the query path at all and depth cannot enter the read cost. The eval p99s
  do drift up at depth (`mcts` 9.7 → 18.6 ms, `data_cleaning` 14.4 → 21.7 ms),
  but each of those is the slowest of a few dozen samples, not a tail over
  thousands.
- **Checkout and checkpoint are flat in depth; fork is the one operation with
  a depth signal.** A checkout at depth 25 costs what one at depth 1 costs
  (`mcts` 331.0 → 312.2 ms p50; `data_cleaning` 335.7 → 347.4; `software_dev`
  178.0 → 180.4), and so does a checkpoint (`mcts` 138.3 → 111.8 ms). Fork is
  flat in the shallow topologies (`data_cleaning` 46.2 → 39.4 ms,
  `software_dev` 35.8 → 29.5) but in `mcts`, the only workflow that goes deep,
  it grows about 1.5x: 52.8 ms p50 at depth 1 against 77.0 ms at depth 25 (p99
  59.9 → 141.0 ms). That is the expected shape, not a surprise: offshoot forks
  by sharing the parent's chain (two small objects, no data copy) until the
  fully-resolved chain reaches 16 members, at which point the next fork
  materializes a fresh floor snapshot instead (`ops.ForkShareMaxDepth`;
  `offshoot compact` does the same on demand). A deep spine crosses that floor
  repeatedly and pays an O(size) copy each time — the same O(size) cost the
  "Per-test isolation primitives" section above documents for materializing a
  checkout. What depth does *not* do is compound: the floor resets the spine
  rather than letting the chain grow with it, which is why 25 levels cost
  about 1.5x depth 1 and not 25x.
- **These are latencies under 8-way concurrency, not per-op costs in
  isolation.** A checkout of a 17 MiB database is 0.18-0.35 s here, while the
  isolation section's *uncontended* `fork` + `open` + `close` of a 100 MB
  database is 421 ms. The difference is queueing: eight workers deep on one
  laptop's disk and page cache. One hypothesis this run does not test is that
  `Fork` at head also quiesces the *parent's* checkout, which would serialize
  concurrent forks that share a parent — plausible given the fanouts here, but
  unmeasured; 8-way I/O contention and the floor materialization above are the
  other candidates. Read these columns as what a branch-heavy agentic workload
  costs end to end, not as primitive latencies; the two sections above are
  where the primitives are measured.
- **Branch management dominates every one of these topologies.** With the
  denominator built from the concurrency each workflow can actually use, the
  spread is 71% (`data_cleaning`) to 96% (`mcts`) — `simulation` 88%,
  `software_dev` 81%, `failure_repro` 73%. There is no "cheap" topology here:
  even `failure_repro`, whose step does 5 schema changes and 45 mutations,
  spends nearly three quarters of its single worker's time forking, checking
  out, checkpointing and destroying, because that cycle moves a 17 MiB
  database while the SQL touches a few hundred rows. `data_cleaning` is lowest
  partly for a bookkeeping reason: its two cross-branch passes (4.1 s of its
  17.8 s wall) count in wall time but not as branch management. The honest
  reading is that BranchBench's tuples are deliberately branch-heavy — the
  ratio is a statement about the workload's mix at least as much as about
  offshoot, and the way to move it is to do more work per branch.
- **`Store peak` is "what the run held", not a steady state.** It counts the
  materialized checkouts as well as the store objects (200 live branches × a
  17 MiB seed is ~3.3 GiB of `data_cleaning`'s 6.1 GiB), and `destroy` is a
  metadata operation — it tombstones, it does not reclaim, because reclaiming
  is `offshoot gc`'s job and nothing runs `gc` during the benchmark. That is
  why `simulation` peaks at 12.0 GiB after pruning all 1,000 of its branches
  (γ=1.0). BranchBench measures reclaim as its own metric; this table does not
  measure it at all.
- **Rows are independent, and that mattered.** Each workflow gets its own
  fresh store (see "What we ran"). An earlier version of this benchmark reused
  one store across all five: the same topologies reported substantially higher
  fork latencies there, growing with how much the earlier workflows had
  already written. Anyone porting this methodology should isolate the stores
  rather than assume the store's size is irrelevant.
- **One host, one run.** These are quantiles within a single run, not a
  median-of-N across runs, on a laptop. Repeat runs of the whole table have
  reproduced the completion counts exactly and the wall times within a few
  percent, but run-to-run variance on a laptop is real and no fleet average is
  implied.

## Method

- Each subtest seeds a fresh SQLite database of the target size (bulk INSERT
  of blob rows into one table, single transaction, `PRAGMA synchronous=OFF`)
  and checkpoints it once — outside the timed loop, once per size, per the
  brief's "build once per size, fork/checkout/open b.N times" shape. Content
  is reused pseudo-random bytes (not unique per row): nothing in the write
  path compresses page content, so uniqueness has no bearing on the
  measurement and generating fresh randomness at the 4GB size would dominate
  seed time for no benefit.
- `BenchmarkForkAtHead` forks to a fresh, uniquely-named branch on every
  iteration and destroys it (`b.StopTimer`'d) before the next, so peak extra
  storage during a run stays close to one seeded database's worth rather
  than growing with `b.N`. Every seeded database checkpoints cleanly, so its
  chain is always exactly one snapshot — the fast path's precondition (see
  "Task 6a: what changed" below) holds for every iteration in this suite.
- `BenchmarkCheckoutCleanSkip` calls `Checkout` repeatedly against a checkout
  that never dirties between calls (the clean fast path performs no write),
  so every iteration measures the same "already correct, don't rebuild"
  path Task 1 added. Unaffected by Task 6a; re-measured anyway since
  `make bench` runs the whole suite together.
- `BenchmarkSessionOpen` measures `session.Open`'s latency (`AcquireLease` +
  `Checkout` + starting the capture engine and waiting for its
  resume-or-rebase verdict to settle — not the full async startup rebase,
  which does not gate `Open`'s return). It also forces one settling flush
  per size (not per iteration) and reports the stored snapshot's byte size
  as a custom `settleSnapshotBytes` metric — see "Settling-flush cost"
  below. Also unaffected by Task 6a (`session.Open` never calls `Fork`).
- Every subtest calls `b.SetBytes(dbSize)` with the *actual* on-disk
  checkout size (not the nominal seed target — SQLite page/header overhead
  makes them close but not identical), so both `ns/op` and the reported
  `MB/s` are meaningful.
- `make bench` runs `-count=3 -short`. `-short` excludes the `size=4GB` Fork
  case from the routine sweep; see "The 4GB case" below for why and how it
  was measured instead.

## Machine

- **Host:** darwin/arm64, Apple M4, 10 cores, 16 GiB RAM, macOS 26.0/27.0
  (build 26A5388g), local disk (APFS, ~48 GiB free at measurement time),
  Go 1.26.5. Same physical machine for both the "before" and "after"
  numbers below.
- **Linux container:** `golang:1.24-bookworm` under Docker Desktop on the
  same host. Docker Desktop's Linux VM on Apple Silicon runs **linux/arm64**
  containers, not linux/amd64 — these numbers show the container/cgroup
  overhead and a different libc/allocator, not a different CPU architecture
  from the host. sqlite3 3.40.1 + gcc 12.2.0 installed in-container via
  `apt-get` before running `go test`. The container's writable layer (where
  `t.TempDir()`/`b.TempDir()` land, since no `GOTMPDIR` is set) is backed by
  Docker Desktop's Linux VM disk — confirmed via
  `go test ./internal/ops/reflink -run TestCopyFileClonedFlagOnClonableFS -v`
  inside the container, which skips loudly ("temp filesystem does not
  support reflink/clonefile"). This is the "ext4-without-reflink" case
  referenced throughout: Task 6a's fast path still fires (one object copy
  instead of decode+re-encode), it just can't clone, so it falls back to
  `reflink.CopyFile`'s plain-byte-copy path — see "Task 6a: what changed"
  below for what that's still worth.
- **S3 path:** `minio/minio:latest` in Docker on the same host (`make
  bench-s3` now runs RustFS instead, since MinIO's images were withdrawn;
  the numbers below were measured against MinIO and not re-measured), reached over
  loopback (`127.0.0.1`, host-mapped port). This is a real S3-API round trip
  (HTTP, request signing, `PutIf`-style conditional writes) but **not** a
  real network path — no WAN latency, no TLS. Treat it as "S3 API overhead
  measured locally," not "S3 performance from a real client location."
  `store.S3.CopyObject` returned the `ErrCopyUnsupported` sentinel
  unconditionally in Task 6a; Task 6b (this update) replaces that with a
  real server-side `CopyObject` call (gated to objects at or under S3's
  5GB single-request `CopyObject` limit — see "Task 6b: what changed"
  below). All MinIO-local numbers throughout this section are exactly
  that: MinIO-local. They say nothing about a real AWS S3 endpoint's
  network latency, throughput, or server-side copy performance at scale —
  do not read them as an AWS claim.
- "Before" (Task 6a's numbers) measured 2026-08-06 (pre-Task-6a, `main`);
  "after Task 6a" measured 2026-08-06 (post-Task-6a, this branch), same
  day, same machine. "After Task 6b" (S3 server-side copy) measured
  2026-08-06, same machine, separate section below.

## Task 6a: what changed

`ops.Workspace.copySnapshotToNewLineage` (the primitive behind `Fork`,
`Rollback`, and `Promote`) now checks, before doing anything else, whether
the source checkpoint's chain (`store.Chain`) resolves to **exactly one
member, and that member is a snapshot**. When it does — the common case: a
freshly checkpointed branch, or any at-rest fork of a branch that has never
been flushed through a live session's segment cadence — the child's seed is
a direct backend-level object copy (`store.Backend.CopyObject`) of the
source snapshot object, verified by resolving the child's own chain
afterward, rather than a materialize-to-temp-file-then-`ltxio.EncodeSnapshot`
round trip. That post-copy verification (`tryFastForkCopy`'s second call to
`store.Chain`, which is itself one `Backend.List` call) is a real extra
round trip the fast path pays that the slow path doesn't need — it's what's
included in every `ForkAtHead` number in this document (the numbers were
never measured with it stripped out), and on the S3 backend it's a real
extra HTTP request per fork alongside the `HEAD`/`CopyObject`/`PutRef`
sequence described in "Results: S3 path" below. `internal/ops/reflink` backs
the local implementation: a
filesystem clone (`clonefile(2)` on darwin, the `FICLONE` ioctl on Linux)
when the filesystem supports it, silently falling back to a plain byte copy
otherwise. S3 returns `store.ErrCopyUnsupported` unconditionally in this
slice (Task 6b adds S3 server-side `CopyObject`, gated to objects ≤5GB — since v0.2.4 larger objects copy server-side too, via multipart `UploadPartCopy`), so
`Fork` against S3 is unaffected — see "Results: S3 path" below.

The precondition does NOT hold once a daemon session has flushed even one
segment past the branch's last snapshot (`SnapshotEvery` cadence): a
multi-member chain has no single source object to copy, so `Fork` falls
through to exactly the pre-6a materialize-and-re-encode path, unchanged
(`internal/ops/gc_chain_test.go`'s `TestForkFastPathSkipsMultiMemberChains`
covers this).

## Task 6b: what changed

`store.S3.CopyObject` (`internal/store/s3.go`) now issues a real
server-side copy — S3's `CopyObject` API (a `PUT` carrying an
`X-Amz-Copy-Source` header, no request body, no download to this process
and no re-upload) — instead of returning `ErrCopyUnsupported`
unconditionally. `ops.Fork`'s fast path (Task 6a, unchanged by this slice)
now actually fires against an S3 backend for any single-snapshot-chain
checkpoint, the same way it already did against the local backend.

**The 5GB guard** *(as measured then; superseded in v0.2.4 — see the
version note at the top: `CopyObject` now multipart-copies sources up to
S3's 5 TiB per-object ceiling, and only >5 TiB falls back)*: S3's
`CopyObject` supports source objects up to 5 GiB in a single request;
anything larger needed the multipart `UploadPartCopy` API, which this
backend did not implement at measurement time (out of scope for that
slice — see `copyObjectMaxBytes` in `s3.go`). `CopyObject` HEADs the source first — one request, not a
download — reads `ContentLength`, and returns the `ErrCopyUnsupported`
sentinel for anything over 5 GiB *before* ever attempting the copy. That
sentinel is the exact signal `ops.Fork`'s fast path (Task 6a) was already
wired to treat as "fall back to the slow, materialize-and-re-encode path,"
so an oversized checkpoint still forks correctly, just without the
server-side-copy win — pinned directly at the `store.S3.CopyObject`
boundary by `TestS3CopyObjectOverSizeLimitFallsBack`
(`internal/store/s3_test.go`), using a fake-S3 hook
(`FakeS3.SetSizeOverride`) to make a small stored object report a >5GB
size on `HEAD` without actually allocating or uploading one.

The shared `storetest.RunConformance` `CopyObject` subtest (added in Task
6a, at the one place backend conformance tests live) now **runs for real**
against S3 — both the in-process fake (`TestS3Conformance/CopyObject`) and
the MinIO-gated real-provider suite (`TestS3RealProvider/Conformance/
CopyObject`, `make test-s3`) — instead of skipping on the sentinel, exactly
as the brief's placement of that subtest was meant to let 6b inherit for
free. The in-process fake (`internal/store/storetest/fakes3.go`) gained
`CopyObject`-request handling (an `X-Amz-Copy-Source`-carrying `PUT`) to
make that possible — without it, the fake would have silently overwritten
the destination with an empty body instead of a copy, since a `CopyObject`
request has no body of its own.

## Results: local store (host, `make bench`, `-count=3`)

| Benchmark | Size | ns/op, before Task 6a | ns/op, after Task 6a | MB/s, before | MB/s, after |
|---|---|---|---|---|---|
| ForkAtHead | 64MB | 353.5ms | **30.3ms** | 190 | 2217 |
| ForkAtHead | 512MB | 2.87s | **198.2ms** | 188 | 2712 |
| CheckoutCleanSkip | 64MB | 24.3ms | 24.5ms | 2768 | 2747 |
| CheckoutCleanSkip | 512MB | 193.3ms | 192.3ms | 2782 | 2795 |
| SessionOpen | 64MB | 29.6ms | 29.4ms | 2271 | 2287 |
| SessionOpen | 512MB | 195.4ms | 197.3ms | 2751 | 2724 |

`ForkAtHead` is ~11.7x faster at 64MB and ~14.5x faster at 512MB. The other
two benchmarks are within run-to-run noise of their pre-6a numbers, as
expected — Task 6a touches only `Fork`'s (and `Rollback`'s/`Promote`'s)
object-copy path.

Raw "after" output:

```
BenchmarkForkAtHead/size=64MB-10             40    30251214 ns/op   2221.09 MB/s
BenchmarkForkAtHead/size=64MB-10             40    30296356 ns/op   2217.78 MB/s
BenchmarkForkAtHead/size=64MB-10             39    30357906 ns/op   2213.29 MB/s
BenchmarkForkAtHead/size=512MB-10             6   198433674 ns/op   2708.56 MB/s
BenchmarkForkAtHead/size=512MB-10             6   197286291 ns/op   2724.31 MB/s
BenchmarkForkAtHead/size=512MB-10             6   198909368 ns/op   2702.08 MB/s
BenchmarkCheckoutCleanSkip/size=64MB-10      48    24529970 ns/op   2739.13 MB/s
BenchmarkCheckoutCleanSkip/size=512MB-10      6   191314104 ns/op   2809.35 MB/s
BenchmarkSessionOpen/size=64MB-10            39    29178191 ns/op   2302.77 MB/s   settleSnapshotBytes=67710606
BenchmarkSessionOpen/size=512MB-10            6   197517292 ns/op   2721.12 MB/s   settleSnapshotBytes=541863202
```

(Original "before" raw output is preserved in this file's git history —
commit that added "Task 6a" to the log, or `git show 2ba8fdb:docs/benchmarks.md`.)

### Isolating the fast path itself

`ForkAtHead`'s `at=""` call still pays for `Fork`'s pre-existing
uncheckpointed-changes check (`warnIfUncheckpointed` → `checkoutState` →
a full SHA-256 hash of the checkout file, Task 1 machinery, unrelated to
Task 6a) on every call. That check is itself O(size) — at ~2.7 GB/s on this
machine, it accounts for essentially all of the 198.2ms measured at 512MB
above (compare `CheckoutCleanSkip`'s ~192ms, which is dominated by the same
hash). To see the fast path's own cost in isolation, forking at a **named**
checkpoint (`at="seed"`) skips that check entirely:

```
go test ./internal/ops -bench 'ForkAtHead' -benchmem -run '^$' -short
# (fork_bench_test.go temporarily edited: w.Fork(db, "main", branch, "seed", 0))
BenchmarkForkAtHead/size=64MB-10            146     9290401 ns/op    7232.28 MB/s
BenchmarkForkAtHead/size=512MB-10           148     9254606 ns/op   58075.83 MB/s
```

Each line above is a **single run** (`go test`'s default `-count=1`), not
this document's usual `-count=3`-and-average the tables further up use —
labeled that way here so it isn't mistaken for the same kind of measurement.
It is a spot-check, taken once per size with the temporary edit noted above,
not a repeated-and-averaged number.

**~9.3ms regardless of size** — the 64MB and 512MB single-run figures land
within a few percent of each other. This is the number that matters for the
design spec's "~40ms" figure: it's in the same regime, on this machine, in
this fast path's applicability window. It is not the number the committed
benchmark suite reports (that suite deliberately measures `Fork`'s actual
default behavior, `at=""`, which still includes Task 1's separate O(size) check),
and it required a one-line, temporary, uncommitted edit to produce — it's
reported here for honesty about what's actually constant-time and what
isn't, not as a replacement for the table above.

## Results: Linux container (`golang:1.24-bookworm`, linux/arm64, `-count=3`)

| Benchmark | Size | ns/op, before Task 6a | ns/op, after Task 6a | MB/s, before | MB/s, after |
|---|---|---|---|---|---|
| ForkAtHead | 64MB | 364.8ms | **47.5ms** | 186 | 1418 |
| ForkAtHead | 512MB | 3.02s | **512.6ms** | 178 | 1065 |
| CheckoutCleanSkip | 64MB | 23.0ms | 22.9ms | 2924 | 2931 |
| CheckoutCleanSkip | 512MB | 179.1ms | 177.2ms | 3000 | 3033 |
| SessionOpen | 64MB | 24.2ms | 23.6ms | 2775 | 2844 |
| SessionOpen | 512MB | 182.4ms | 182.7ms | 2947 | 2943 |

This container's writable layer does **not** support `FICLONE` (verified —
see "Machine" above), so every one of these `ForkAtHead` numbers is the
**plain-copy fallback**, not a real clone. It is still 7.7x faster at 64MB
and 5.9x faster at 512MB than before Task 6a: skipping the
decode-into-SQLite-pages-then-re-encode-into-LTX round trip and doing one
`io.Copy` instead is a real win on its own, exactly as expected for
"ext4-without-reflink" — a plain copy is still O(size), just a much smaller
constant than decode+re-encode, and the fallback triggers silently with no
special-casing needed anywhere in `ops`.

Raw "after" output:

```
BenchmarkForkAtHead/size=64MB-10             21    50917202 ns/op   1319.61 MB/s
BenchmarkForkAtHead/size=64MB-10             25    45323748 ns/op   1482.46 MB/s
BenchmarkForkAtHead/size=64MB-10             26    46263989 ns/op   1452.33 MB/s
BenchmarkForkAtHead/size=512MB-10             3   495316153 ns/op   1085.10 MB/s
BenchmarkForkAtHead/size=512MB-10             2   598705230 ns/op    897.72 MB/s
BenchmarkForkAtHead/size=512MB-10             3   443778612 ns/op   1211.12 MB/s
BenchmarkCheckoutCleanSkip/size=64MB-10      45    23315294 ns/op   2881.83 MB/s
BenchmarkCheckoutCleanSkip/size=512MB-10      6   177494146 ns/op   3028.09 MB/s
BenchmarkSessionOpen/size=64MB-10            44    24421828 ns/op   2751.26 MB/s   settleSnapshotBytes=67710606
BenchmarkSessionOpen/size=512MB-10            6   187563653 ns/op   2865.53 MB/s   settleSnapshotBytes=541863202
```

Within noise of the host numbers for the two unaffected benchmarks —
unsurprising, since both run on the same physical machine (the container is
not a different CPU or a network hop). The value of this table remains
process isolation and confirming the code builds and runs unmodified under
a plain `golang:*-bookworm` image, plus — new for Task 6a — direct evidence
that the fast path's fallback behaves correctly and still helps on a
filesystem with no clone support.

## Results: S3 path (`make bench-s3`, MinIO in Docker, `-count=1`)

| Benchmark | Size | ns/op, before Task 6a | ns/op, after Task 6a | MB/s, before | MB/s, after |
|---|---|---|---|---|---|
| ForkAtHead | 64MB | 538ms | 536.9ms | 125 | 125 |
| ForkAtHead | 512MB | 4.07s | 4.57s | 132 | 118 |
| CheckoutCleanSkip | 64MB | 27.2ms | 27.5ms | 2470 | 2442 |
| CheckoutCleanSkip | 512MB | 194.3ms | 198.2ms | 2766 | 2712 |
| SessionOpen | 64MB | 31.9ms | 31.8ms | 2109 | 2115 |
| SessionOpen | 512MB | 214.4ms | 203.4ms | 2506 | 2642 |

This table is Task 6a's measurement, kept as published: at that point
`store.S3.CopyObject` still returned `ErrCopyUnsupported` immediately, with
no HTTP call, before `copySnapshotToNewLineage` fell through to the
unchanged materialize-and-re-encode path. The 64MB numbers landed within 1%
of the pre-6a run; 512MB was ~12% slower, which read as single-sample
Docker/MinIO variance (`-count=1`, no repeats to average, a different
container/network warm-up state than the original run) rather than a
regression — there was no code-path difference on this backend for Task 6a
to have introduced. `CheckoutCleanSkip` and `SessionOpen` are, as before,
close to the local numbers because their dominant cost never touches the
backend at all — Task 6b doesn't change that either.

### Task 6b: S3 server-side CopyObject (`make bench-s3`, MinIO in Docker, `-count=1`)

| Benchmark | Size | ns/op, before Task 6b (=after 6a) | ns/op, after Task 6b | MB/s, before | MB/s, after |
|---|---|---|---|---|---|
| ForkAtHead | 64MB | 536.9ms | **153.0ms** | 125 | 439 |
| ForkAtHead | 512MB | 4.57s | **1.03s** | 118 | 522 |
| CheckoutCleanSkip | 64MB | 27.5ms | 27.2ms | 2442 | 2466 |
| CheckoutCleanSkip | 512MB | 198.2ms | 194.4ms | 2712 | 2764 |
| SessionOpen | 64MB | 31.8ms | 30.7ms | 2115 | 2191 |
| SessionOpen | 512MB | 203.4ms | 202.7ms | 2642 | 2651 |

`ForkAtHead` is ~3.5x faster at 64MB and ~4.4x faster at 512MB: the
server-side copy means this process never downloads the source snapshot
object, never re-encodes it, and never re-uploads it — MinIO copies the
object on its own side of the wire, so the round trips this process pays
for are the `HEAD` (size gate), the `CopyObject` request itself, one `List`
call to verify the child's chain resolves after the copy (see "Task 6a: what
changed" above — this is extra work the fast path pays that the slow path
doesn't, not free), and the same `PutRef`/`GetRef` calls every fork makes
regardless of path. It is
NOT as fast as the local numbers (1.03s vs. ~198ms at 512MB): `Fork`'s
uncheckpointed-changes SHA-256 check (see "Isolating the fast path itself"
above) still runs against the LOCAL checkout file either way
(`OFFSHOOT_CHECKOUTS` is local disk regardless of backend), so that ~192ms
cost is present in both the local and S3 numbers; the remaining gap is
real HTTP round trips (`HEAD`, `CopyObject`, `PutRef`) plus MinIO's own
server-side copy time, which is real disk I/O on MinIO's side even though
it never crosses the network back to this process. `CheckoutCleanSkip` and
`SessionOpen` are unaffected, as expected (neither calls `CopyObject`).

Raw "after Task 6b" output:

```
BenchmarkForkAtHead/size=64MB-10              7   152972149 ns/op    439.24 MB/s
BenchmarkForkAtHead/size=512MB-10             1  1030185084 ns/op    521.72 MB/s
BenchmarkCheckoutCleanSkip/size=64MB-10      39    27249625 ns/op   2465.75 MB/s
BenchmarkCheckoutCleanSkip/size=512MB-10      6   194418798 ns/op   2764.49 MB/s
BenchmarkSessionOpen/size=64MB-10            34    30661629 ns/op   2191.36 MB/s   settleSnapshotBytes=67710606
BenchmarkSessionOpen/size=512MB-10            5   202717683 ns/op   2651.32 MB/s   settleSnapshotBytes=541863202
```

Same caveat as every other S3 number in this document: MinIO-local,
`-count=1`, no WAN latency, no TLS — this is "S3 API overhead measured
locally," not an AWS performance claim, and a real network hop to a real
AWS region would add real latency Task 6b's design does nothing to hide
(one `HEAD` + one `CopyObject` round trip per fork, same as any other S3
API call this codebase makes).

## The 4GB case

`BenchmarkForkAtHead/size=4GB` exists in the source but is skipped under
`-short` (`make bench`'s default) — the PM's amendment for this task
timeboxed a 4GB run to a single attempt, not a routine `-count=3` sweep
costing several GB of RAM and disk on every `make bench` invocation. Before
and after Task 6a, run once, directly, host-only:

```
go test ./internal/ops -bench 'ForkAtHead/size=4GB' -benchmem -run '^$' -benchtime=1x -timeout 20m

# before Task 6a
BenchmarkForkAtHead/size=4GB-10   1   83385791875 ns/op   51.56 MB/s   18172542944 B/op   15762922 allocs/op

# after Task 6a
BenchmarkForkAtHead/size=4GB-10   1    3409868667 ns/op 1260.96 MB/s      89224 B/op        489 allocs/op
```

**83.4s → 3.41s, a ~24.5x speedup**, and the allocation profile tells the
real story: before Task 6a, `Fork`'s slow path buffered the entire
re-encoded snapshot in memory (`bytes.Buffer` in `ltxio.EncodeSnapshot`)
before writing it out, so a 4GB database drove ~18GB of allocation over the
call (~4.5x the database size) — GC pressure at that scale, not disk or
network, was the likely reason it didn't scale linearly from the smaller
sizes. After Task 6a, that same fork allocates 489 times and 89KB total: the
fast path never materializes the content in Go memory at all, it's a
backend-level object copy end to end. The 4GB fork is still slower than the
512MB fork (3.41s vs. 198ms is more than the 8x size ratio would predict) —
consistent with the "Isolating the fast path itself" finding above: the
remaining time is dominated by `Fork`'s separate O(size)
uncheckpointed-changes SHA-256 check, which does scale with size, not by
the object copy itself.

## What's still O(size) after Task 6a

- **`Fork`'s uncheckpointed-changes check** (`warnIfUncheckpointed` →
  `checkoutState`, Task 1 machinery, not touched by Task 6a): every default
  `Fork(..., at="", ...)` call quiesces and SHA-256-hashes the whole
  checkout file to decide whether to print a "forking last committed state"
  warning. This is now the dominant cost in every `ForkAtHead` number above
  — see "Isolating the fast path itself".
- **`Checkout`'s clean-skip path** (Task 1): avoids the rebuild, but
  `checkoutState` still quiesces the WAL and computes a fresh SHA-256 over
  the whole checkout file on every call to confirm it's actually still
  clean. Cheaper than a rebuild (no temp file, no rename, no LTX encode) —
  the ~2.7-3.0 GB/s throughput above is close to this machine's raw SHA-256
  rate — but not free, and still O(size). Unchanged by Task 6a.
- **`session.Open`**: dominated by the same `Checkout` clean-skip cost,
  since `Open` calls `Checkout` internally. Unchanged by Task 6a.
- **`Fork` itself, when the fast-path precondition doesn't hold**: a
  multi-member chain (post-segment-flush branch) still takes the full
  materialize-and-re-encode path, unchanged from before Task 6a. At
  measurement time a single-snapshot checkpoint whose object was over S3's
  5GB single-request `CopyObject` limit also fell back this way; since
  v0.2.4 that case is instead a multipart server-side copy (see the
  version note at the top). Before Task 6b, EVERY S3 fork took the slow
  path regardless of chain shape or size (`store.S3.CopyObject` returned
  `ErrCopyUnsupported` unconditionally) — see
  `TestForkFastPathSkipsMultiMemberChains`.

**Task 6a's fast path applies only when the checkpoint being forked
resolves to a chain of exactly one member, and that member is itself a
snapshot** (`store.Chain` returns a single snapshot entry, no segments
layered after it). That's the common case for the benchmarks above — a
freshly seeded-and-checkpointed database — and for any at-rest fork of a
branch that has never been flushed through a live session with segment
cadence (`SnapshotEvery > 1`). It stops applying the moment a daemon
session has flushed even one segment past the branch's last snapshot:
forking from a multi-member chain still requires replaying segments to
materialize before anything can be re-encoded, so the pre-6a numbers in
this document remain the correct comparison baseline for that case. The
design spec's "forking a 10GB database took ~40ms" figure describes the
reflink/clonefile mechanism this task implements, in exactly this
single-snapshot-chain window, on a clone-capable local filesystem — see
"Isolating the fast path itself" for the closest this document gets to
checking that figure directly (~9.3ms, in the same regime); it is cited
here only as the target this benchmark suite exists to check against, never
as a claim about `ForkAtHead`'s own reported numbers, which also include
Task 1's separate O(size) check.

## Settling-flush cost (Task 2 controller decision)

> **Update (Milestone 2 follow-up, shipped):** the measurements below still
> describe the upload's *size* accurately for the case where it happens, but
> it is no longer unconditional. `rebaseline` now skips this flush entirely
> when BOTH the checkout `Open` received was already proven byte-identical
> to the branch's head at open time AND the checksum recorded in that
> checkout's own `.sum` sidecar exactly matches what the checkout actually
> contains once the session's real startup rebase finishes — see
> [docs/status.md](status.md)'s "Settling-flush checksum-compare
> suppression" row and `internal/session/session.go`'s `rebaseline` doc
> comment for exactly why both conditions matter. A read-only agent
> reopening an unmodified checkout uploads nothing at all — AND, just as
> important, the checksum comparison itself costs no store read: it's read
> straight out of the local sidecar `Checkout`/`Checkpoint`/`Rollback`/
> `Promote` already stamp, never fetched fresh from the store. An earlier,
> since-reverted version of this fix DID fetch it fresh on every `Open`,
> which meant downloading the entire head object whenever it happened to be
> a full snapshot — the exact case a permanently-idle read-only session
> always hits, since it never advances past its first (snapshot) head.
> `internal/session/flush_test.go`'s
> `TestReadOnlySessionWithCleanCheckoutMakesNoStoreWrites` asserts this at
> the backend-call level (exactly 2 `Get`s during `Open` — both tiny ref
> reads, zero for the head object — and zero of anything at all across the
> idle settle window). `BenchmarkSessionOpen`, re-run against the final
> code:
>
> | size | ns/op | throughput | B/op | allocs/op |
> |---|---|---|---|---|
> | 64MB | 29,996,972 | 2239.92 MB/s | 62,634 | 394 |
> | 512MB | 199,496,139 | 2694.13 MB/s | 62,906 | 393 |
>
> (`-benchtime=3x`, local filesystem backend — the default this benchmark
> falls back to without `OFFSHOOT_S3_TEST_BUCKET` set; `B/op`/`allocs/op` stay flat across
> a 8x size increase, consistent with no per-byte store traffic on this
> path — the unit test above is the byte-level proof, this is its latency
> consequence.) A session whose checkout had to be (re)materialized first
> (first-ever open, a dirty/stale checkout, or one whose sidecar predates
> checksum-recording — see status.md's row for the exact fail-toward-settling
> case) still pays the settling-flush cost measured below, once.

Every daemon session's first auto-flush tick after `Open` uploads a **full
snapshot** — the `forceSnapshot` path — even for a session that never
writes anything (a read-only agent that only queries). This was a ledgered
tradeoff from Task 2: closing a startup-rebase race required forcing that
first flush to be a full re-baseline rather than a segment.

`BenchmarkSessionOpen` measures this directly rather than only describing
it: `settleSnapshotBytes` above is the actual stored object size of that
forced snapshot, read back from the store after triggering it. It tracks
the database size almost exactly — 67,710,606 bytes for the 64MB seed
(a ~67MB source database) and 541,863,202 bytes for the 512MB seed — i.e.
the settling flush is **O(size)**, not a fixed background cost. A daemon
serving many idle read-only sessions against large databases pays one
full-snapshot upload per session, once, ~30s after each `Open` (the
daemon's default `-flush-every`), not per query and not repeatedly. This
call never goes through `Fork`'s fast path (it's a session flush, not a
fork) and is unaffected by Task 6a.
