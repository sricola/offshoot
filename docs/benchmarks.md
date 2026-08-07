# Benchmarks

Measured baselines for `ops.Workspace.Fork`, `Checkout`'s clean-skip fast
path (Task 1 of Milestone 2), and `session.Open` — **before** and **after**
Task 6a's fast-path fork. The point of this document is to make the
before/after comparison honest: these are real numbers from real runs, not
estimates. The "before" column is exactly what was measured and published
prior to Task 6a; it is kept, not replaced, so the comparison is checkable.

Benchmarks live in `internal/ops/fork_bench_test.go`. Run them with
`make bench` (local store) or `make bench-s3` (real MinIO in Docker). The
benchmark code itself is unchanged by Task 6a — same subtest names, same
seeding, same `at=""` (fork at branch head, which runs `Fork`'s normal
uncheckpointed-changes check; see "What's still O(size) after Task 6a"
below) — so before/after numbers are a like-for-like comparison of the same
call, not two different things being measured.

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
- **S3 path:** `minio/minio:latest` in Docker on the same host, reached over
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
slice (Task 6b adds S3 server-side `CopyObject`, gated to objects ≤5GB), so
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

**The 5GB guard:** S3's `CopyObject` supports source objects up to 5 GiB in
a single request; anything larger needs the multipart `UploadPartCopy` API,
which this backend does not implement (out of scope for this slice, same as
it was for Task 6a's local reflink path — see `copyObjectMaxBytes` in
`s3.go`). `CopyObject` HEADs the source first — one request, not a
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
commit that added "Task 6a" to the log, or `git show 539b832:docs/benchmarks.md`.)

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
  multi-member chain (post-segment-flush branch), or — since Task 6b — a
  single-snapshot checkpoint whose object is over S3's 5GB single-request
  `CopyObject` limit, still takes the full materialize-and-re-encode path,
  unchanged from before Task 6a. Before Task 6b, EVERY S3 fork took this
  path regardless of chain shape or size (`store.S3.CopyObject` returned
  `ErrCopyUnsupported` unconditionally) — see `TestForkFastPathSkipsMultiMemberChains`
  and, for the size gate specifically, `TestS3CopyObjectOverSizeLimitFallsBack`.

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
