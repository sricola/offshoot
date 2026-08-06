# Benchmarks

Measured baselines for `ops.Workspace.Fork`'s current implementation, plus
`Checkout`'s clean-skip fast path (Task 1 of Milestone 2) and
`session.Open`, taken **before** Task 6's fast-path fork lands. The point of
this document is to make the before/after comparison honest: these are real
numbers from real runs, not estimates, and they are re-measured (not
replaced) once the fast path exists.

Benchmarks live in `internal/ops/fork_bench_test.go`. Run them with
`make bench` (local store) or `make bench-s3` (real MinIO in Docker).

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
  than growing with `b.N`.
- `BenchmarkCheckoutCleanSkip` calls `Checkout` repeatedly against a checkout
  that never dirties between calls (the clean fast path performs no write),
  so every iteration measures the same "already correct, don't rebuild"
  path Task 1 added.
- `BenchmarkSessionOpen` measures `session.Open`'s latency (`AcquireLease` +
  `Checkout` + starting the capture engine and waiting for its
  resume-or-rebase verdict to settle — not the full async startup rebase,
  which does not gate `Open`'s return). It also forces one settling flush
  per size (not per iteration) and reports the stored snapshot's byte size
  as a custom `settleSnapshotBytes` metric — see "Settling-flush cost"
  below.
- Every subtest calls `b.SetBytes(dbSize)` with the *actual* on-disk
  checkout size (not the nominal seed target — SQLite page/header overhead
  makes them close but not identical), so both `ns/op` and the reported
  `MB/s` are meaningful.
- `make bench` runs `-count=3 -short`. `-short` excludes the `size=4GB` Fork
  case from the routine sweep; see "The 4GB case" below for why and how it
  was measured instead.

## Machine

- **Host:** darwin/arm64, Apple M4, 10 cores, 16 GiB RAM, macOS 26.0
  (build 26A5388g), local disk (APFS, ~48 GiB free at measurement time),
  Go 1.26.5.
- **Linux container:** `golang:1.24-bookworm` under Docker Desktop on the
  same host. Docker Desktop's Linux VM on Apple Silicon runs **linux/arm64**
  containers, not linux/amd64 — these numbers show the container/cgroup
  overhead and a different libc/allocator, not a different CPU architecture
  from the host. sqlite3 3.40.1 + gcc 12.2.0 installed in-container via
  `apt-get` before running `go test`.
- **S3 path:** `minio/minio:latest` in Docker on the same host, reached over
  loopback (`127.0.0.1`, host-mapped port). This is a real S3-API round trip
  (HTTP, request signing, `PutIf`-style conditional writes) but **not** a
  real network path — no WAN latency, no TLS. Treat it as "S3 API overhead
  measured locally," not "S3 performance from a real client location."
- Measured 2026-08-06.

## Results: local store (host, `make bench`, `-count=3`)

| Benchmark | Size | ns/op (avg of 3) | MB/s (avg of 3) |
|---|---|---|---|
| ForkAtHead | 64MB | 353.5ms | 190 |
| ForkAtHead | 512MB | 2.87s | 188 |
| CheckoutCleanSkip | 64MB | 24.3ms | 2768 |
| CheckoutCleanSkip | 512MB | 193.3ms | 2782 |
| SessionOpen | 64MB | 29.6ms | 2271 |
| SessionOpen | 512MB | 195.4ms | 2751 |

Raw output: `internal/ops/fork_bench_test.go` benchmark names above; a
representative run:

```
BenchmarkForkAtHead/size=64MB-10             3   351673000 ns/op   191.06 MB/s
BenchmarkForkAtHead/size=512MB-10            1  2920021042 ns/op   184.06 MB/s
BenchmarkCheckoutCleanSkip/size=64MB-10     48    24260647 ns/op  2769.54 MB/s
BenchmarkCheckoutCleanSkip/size=512MB-10     6   190528271 ns/op  2820.94 MB/s
BenchmarkSessionOpen/size=64MB-10           40    29681427 ns/op  2263.73 MB/s  settleSnapshotBytes=67710606
BenchmarkSessionOpen/size=512MB-10           6   196077889 ns/op  2741.10 MB/s  settleSnapshotBytes=541863202
```

## Results: Linux container (`golang:1.24-bookworm`, linux/arm64, `-count=3`)

| Benchmark | Size | ns/op (avg of 3) | MB/s (avg of 3) |
|---|---|---|---|
| ForkAtHead | 64MB | 364.8ms | 186 |
| ForkAtHead | 512MB | 3.02s | 178 |
| CheckoutCleanSkip | 64MB | 23.0ms | 2924 |
| CheckoutCleanSkip | 512MB | 179.1ms | 3000 |
| SessionOpen | 64MB | 24.2ms | 2775 |
| SessionOpen | 512MB | 182.4ms | 2947 |

Within noise of the host numbers — unsurprising, since both run on the same
physical machine (the container is not a different CPU or a network hop).
The value of this table is process isolation (no host Go toolchain/module
cache reuse) and confirming the code builds and runs unmodified under a
plain `golang:*-bookworm` image with `sqlite3`/`gcc` installed, not a
different performance regime.

## Results: S3 path (`make bench-s3`, MinIO in Docker, `-count=1`)

| Benchmark | Size | ns/op | MB/s |
|---|---|---|---|
| ForkAtHead | 64MB | 538ms | 125 |
| ForkAtHead | 512MB | 4.07s | 132 |
| CheckoutCleanSkip | 64MB | 27.2ms | 2470 |
| CheckoutCleanSkip | 512MB | 194.3ms | 2766 |
| SessionOpen | 64MB | 31.9ms | 2109 |
| SessionOpen | 512MB | 214.4ms | 2506 |

`ForkAtHead` is the path most affected by S3: roughly 1.4-1.5x slower than
the local store, from the extra `PutIf`/`GetRef` HTTP round trips
`Fork`/`Checkpoint` make even though the seeded checkout itself is a local
file either way (`OFFSHOOT_CHECKOUTS` is still local disk — only the
*store* objects move to S3). `CheckoutCleanSkip` and `SessionOpen` are
close to the local numbers because their dominant cost (see below) never
touches the backend at all.

## The 4GB case

`BenchmarkForkAtHead/size=4GB` exists in the source but is skipped under
`-short` (`make bench`'s default) — the PM's amendment for this task
timeboxed a 4GB run to a single attempt, not a routine `-count=3` sweep
costing several GB of RAM and disk on every `make bench` invocation. It was
run once, directly, host-only:

```
go test ./internal/ops -bench 'ForkAtHead/size=4GB' -benchmem -run '^$' -benchtime=1x -timeout 20m
BenchmarkForkAtHead/size=4GB-10   1   83385791875 ns/op   51.56 MB/s   18172542944 B/op   15762922 allocs/op
```

83.4s, well inside the 20-minute timebox, and worth reporting as-is rather
than smoothing over: throughput at 4GB (51.6 MB/s) is roughly a third of
the 512MB number (188 MB/s), not the same constant. `Fork`'s slow path
buffers the entire re-encoded snapshot in memory (`bytes.Buffer` in
`ltxio.EncodeSnapshot`) before writing it out, so a 4GB database drives
~18GB of allocation over the call (~4.5x the database size) — GC pressure
at that scale, not disk or network, is the likely reason it doesn't scale
linearly from the smaller sizes. This is itself a data point for Task 6:
the fast path's object-copy design sidesteps this entirely by never
materializing the content in Go memory at all.

## What's O(size) today, and what Task 6 targets

Every number above traces back to real per-byte work, not a fixed cost:

- **`Fork`** (`ops.Workspace.copySnapshotToNewLineage`): materializes the
  source checkpoint to a temporary SQLite file, then `ltxio.EncodeSnapshot`
  walks every page of that file to re-encode a fresh snapshot object for
  the child lineage. Full read + decode + re-encode + write, O(size), for
  every fork regardless of how large or small the actual change from parent
  to child is (there usually isn't one — a fork's whole point is "same
  content, new lineage").
- **`Checkout`'s clean-skip path** (Task 1): avoids the rebuild, but
  `checkoutState` still quiesces the WAL and computes a fresh SHA-256 over
  the whole checkout file on every call to confirm it's actually still
  clean. Cheaper than a rebuild (no temp file, no rename, no LTX encode) —
  the ~2.3-3.0 GB/s throughput above is close to this machine's raw SHA-256
  rate — but not free, and still O(size). This was called out as a known
  minor when Task 1 shipped and remains true here.
- **`session.Open`**: dominated by the same `Checkout` clean-skip cost,
  since `Open` calls `Checkout` internally. Its numbers track
  `BenchmarkCheckoutCleanSkip`'s closely for exactly that reason.

**Task 6's fast path targets exactly one case: when the checkpoint being
forked resolves to a chain of exactly one member, and that member is
itself a snapshot** (`store.Chain` returns a single snapshot entry, no
segments layered after it). That's the common case for the benchmarks
above — a freshly seeded-and-checkpointed database — and for any at-rest
fork of a branch that has never been flushed through a live session with
segment cadence (`SnapshotEvery > 1`). It stops being true the moment a
daemon session has flushed even one segment past the branch's last
snapshot: forking from a multi-member chain still requires replaying
segments to materialize before anything can be re-encoded, so the slow
path in this document remains the correct comparison baseline for that
case even after Task 6 ships. The design spec's "forking a 10GB database
took ~40ms" figure describes the reflink/clonefile mechanism Task 6 is
expected to use locally, in its single-snapshot-chain window — it is cited
here only as the target this benchmark suite exists to check against, not
as a claim about the current implementation measured above.

## Settling-flush cost (Task 2 controller decision)

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
daemon's default `-flush-every`), not per query and not repeatedly.
