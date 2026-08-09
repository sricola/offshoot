# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches 1.0. Before 1.0, minor versions may include breaking changes.

**Pre-release status:** offshoot is pre-1.0. The on-disk/on-bucket storage
format (refs, checkpoints, segments, leases) may change in a
backward-incompatible way before 1.0 without a migration path. Pin an exact
version if you depend on format stability.

## [Unreleased]

### Added

- **S3 multipart uploads now upload parts concurrently.** `store.S3`'s
  `putMultipart` (backing `PutReader`/`PutReaderIf` above
  `multipartThreshold`) previously uploaded parts strictly sequentially — a
  5 GiB snapshot at the 64 MiB default part size paid ~80 sequential
  `UploadPart` round trips. Parts now upload in parallel, bounded to
  `multipartConcurrency` (default 4) simultaneous workers, but ONLY on the
  `io.ReaderAt` path (the production `*os.File` case): `io.ReaderAt` is
  documented safe for parallel `ReadAt` calls on the same source, so each
  worker can read its own part independently. The non-`io.ReaderAt`
  fallback — which reads every part sequentially into one reused buffer —
  is unaffected and stays strictly sequential; parallelizing it would
  either race the shared buffer or need a buffer per worker, multiplying
  memory for exactly the callers who couldn't afford one `partSize` buffer
  in the first place. Every part's result lands in a pre-sized slice at its
  own `PartNumber` index, so the final `CompletedPart` list stays correctly
  ordered regardless of completion order — no sort step needed. The
  existing abort-on-every-error-path guarantee, per-part checksum
  propagation, and first-error-wins-with-prompt-cancellation semantics all
  carry over unchanged under concurrency.

- **`store.S3.CopyObject` now server-side-copies objects over 5 GiB
  instead of declining.** Previously, any source over S3's 5 GiB
  single-request `CopyObject` limit returned `ErrCopyUnsupported`,
  forcing callers (`ops.Fork`'s fast fork path) to fall back to a full
  materialize-download-re-encode-upload of the whole database. `CopyObject`
  now performs a real multipart server-side copy for sources up to S3's
  actual 5 TiB per-object ceiling: `CreateMultipartUpload` + a sequence of
  `UploadPartCopy` calls (each with an inclusive `CopySourceRange` byte
  range, sized the same way multipart uploads size their parts) +
  `CompleteMultipartUpload`. `ErrCopyUnsupported` is now reserved for
  sources genuinely beyond any S3 mechanism's reach (over 5 TiB) — the 5
  GiB boundary is now a strategy choice (single-request vs. multipart), not
  a capability limit. Same abort-on-every-error-path discipline as the
  multipart upload path; `CompleteMultipartUpload` sets no conditions,
  matching `CopyObject`'s existing overwrite-on-existing-dst contract.

## [0.2.3] - 2026-08-09

### Added

- **S3 snapshot uploads over 5 GiB no longer fail.** `store.S3.PutReader`/
  `PutReaderIf` previously issued a single `PutObject`, which S3 caps at 5
  GiB — a snapshot of a larger database failed to upload. Both now switch to
  a real multipart upload (`CreateMultipartUpload` + sequential `UploadPart`
  + `CompleteMultipartUpload`) once size exceeds `multipartThreshold`
  (default: 5 GiB, matching the single-PUT ceiling it replaces). Part size
  is computed to respect S3's two hard limits — parts >= 5 MiB, at most
  10,000 parts — so any object up to S3's 5 TiB per-object limit stays
  within the part-count ceiling. `PutReaderIf`'s CAS condition
  (`IfNoneMatch: "*"`/`IfMatch`) is placed on `CompleteMultipartUpload`, not
  on `Create`/`UploadPart`, and a precondition rejection there maps to
  `store.ErrCAS` exactly as the single-PUT path does — CAS semantics are
  indistinguishable to callers regardless of which path an object's size
  took. Below the threshold, behavior is byte-for-byte unchanged: the
  original single-`PutObject` path, untouched.

  Cost-critical: an abandoned multipart upload leaves its uploaded parts
  billed on S3 indefinitely, so every error exit after a successful
  `CreateMultipartUpload` — a part-upload failure, a part-read failure, a
  `Complete` failure including a CAS rejection — runs `AbortMultipartUpload`
  via a `defer`; only a successful `Complete` skips it. An abort failure is
  appended to (never masks) the original error.

  Part bodies avoid buffering the whole object: when the caller's reader is
  both an `io.ReaderAt` AND an `io.Seeker` (true of the `*os.File` the only
  production caller — `flush.go`'s snapshot upload — passes), each part
  streams directly from the file via `io.NewSectionReader`, zero buffering
  — the reader's current position (via `Seek(0, SeekCurrent)`) is captured
  as a base offset so a reader the caller already seeked into uploads the
  same bytes the single-`PutObject` path would have (that path reads from
  `r`'s current position via `Body: r`); `io.ReaderAt` alone would silently
  read from absolute offset 0 instead. A reader missing either falls back
  to reading one part at a time into a reused, part-sized buffer.
  `store.S3.CopyObject`'s separate 5 GiB server-side-copy limit is
  untouched — that's a documented, unrelated fallback (to the slow
  materialize-and-re-encode path), not a failure.

  Both `CreateMultipartUpload` and every `UploadPart` call now declare
  `ChecksumAlgorithm: CRC32` explicitly, and each part's returned checksum
  is carried from `UploadPartOutput` into its `CompletedPart` entry.
  Declaring it on both calls (not just Create) matters regardless of the
  SDK's `RequestChecksumCalculation` setting: left to its default
  (`WhenSupported`) the SDK attaches a CRC32 checksum to every `UploadPart`
  on its own, but under `when_required` (the common workaround for
  third-party S3-compatible stores after the Jan-2025 default-checksum
  change — exactly the audience `S3Config` names via R2/Tigris/MinIO) it
  would not, leaving `CreateMultipartUpload`'s declared algorithm
  unmatched by any part's supplied checksum. Either way, a real S3
  `CompleteMultipartUpload` rejects a mismatch with `InvalidRequest` — a
  case the in-process fake cannot reproduce (plain HTTP takes a different
  SDK code path than the `aws-chunked` framing production traffic uses
  over TLS, and the fake ignores checksum headers entirely).
  `CompleteMultipartUploadInput` also now sets `MpuObjectSize` to the
  expected total, so a length mismatch from this backend's own offset
  arithmetic is rejected by S3 with a 400 instead of silently storing a
  wrong-length object.

  Part size defaults to 64 MiB (`defaultPartSize`), not a smaller
  "AWS-CLI-like" default — parts upload strictly sequentially (no
  concurrency yet), so a smaller default part size only means more
  sequential round trips for no benefit. Both `multipartThreshold` and
  `defaultPartSize` are overridable in tests only
  (`export_test.go`'s `SetMultipartThresholdForTest` /
  `SetPartSizeForTest`) since a real >5 GiB upload, or one with a
  production-sized 64 MiB part, can't be exercised in a test;
  `storetest.FakeS3` now models the full multipart API
  (Create/UploadPart/Complete/Abort) to make the path genuinely testable,
  not just compiled. `TestS3RealProvider` (env-gated on
  `OFFSHOOT_S3_TEST_BUCKET`) gained a multipart subtest — a real provider's
  `CompleteMultipartUpload` precondition/checksum handling is the only
  evidence the in-process fake can never substitute for.

### Changed

- **Doc note on `store.BatchDeleter`/`ReaderGetter`/`ReaderPutter`**: these
  optional capabilities are discovered by type assertion, so a `store.Backend`
  wrapper that embeds another Backend (e.g. for instrumentation/caching)
  silently hides them — callers fall back to the buffered/per-key path,
  which stays correct but loses the batching/streaming benefit. No code
  change: `ops.markCache` (the one in-repo wrapper today) implements
  `Backend` directly rather than embedding, and needs none of these three,
  so nothing is affected today.

## [0.2.2] - 2026-08-08

### Added

- `make lint`: gofmt -l (fails on any unformatted file) + `go vet` +
  best-effort staticcheck. ci.yml and ci-local run the same gofmt/vet gate
  on every PR, so an unformatted file can no longer ship silently.

### Changed

- **Materializing a chain (checkout/fork/rollback/promote/compact's read
  path) no longer holds every resolved object in memory at once** (perf
  audit H3, read-side). `ops.materializeMembersAt` now type-asserts the
  backend for a new optional `store.ReaderGetter` capability
  (`GetReader(key) (io.ReadCloser, etag, error)`, implemented by both `S3`
  — the raw `GetObject` response body, no `io.ReadAll` — and `Local` — a
  plain `os.Open`) and, when present, applies the chain through lazily-
  opened, self-closing streams: each member's stream opens on first Read
  and closes on EOF, so at most one snapshot/segment is resident at a time
  instead of the whole chain. Safe because `ltxio.MaterializeChain` already
  consumes its inputs strictly sequentially (snapshot fully, then each
  segment fully, in order). A backend without `ReaderGetter` falls back to
  the prior fully-buffered path unchanged. Materialized output, the
  returned checksum, and every stream's close-on-every-path (success and
  mid-apply error) are unaffected — pinned by a counting-backend test
  asserting max concurrently-open streams <= 1 and open count == close
  count. Read-side only; flush/capture (H3's write side) is unchanged.

- **A snapshot flush no longer buffers the whole encoded snapshot in memory
  before uploading it** (perf audit H3, write-side companion to the
  read-side entry above). `Session.flush`'s snapshot branch now
  `ltxio.EncodeSnapshot`s to a second per-flush scratch FILE (alongside
  task 8's replica-clone scratch) instead of a `bytes.Buffer`, then streams
  that file to the backend via a new optional `store.ReaderPutter`
  capability (`PutReaderIf`/`PutReader`, implemented by both `S3` — a plain
  `PutObject` with `Body: r`/`ContentLength: size`, no `io.ReadAll` — and
  `Local` — write-to-temp-then-rename streamed via `io.Copy`, computing the
  sha256 etag incrementally alongside the write) instead of buffering the
  object as a `[]byte` for `PutIf`/`Put`. `PutReaderIf`/`PutReader` mirror
  `PutIf`/`Put`'s exact CAS/create-only/overwrite contract, including the
  orphan-overwrite retry (a fresh reader reopened from offset 0, since the
  first attempt's reader is exhausted after `PutReaderIf`). The segment
  branch (already O(changed pages)) and any backend without `ReaderPutter`
  are entirely unchanged — still buffered, byte-for-byte the prior
  behavior. Upload→ref-CAS ordering, the durable checksum, and every other
  flush invariant (task 8's clone-then-release-`replicaMu` discipline,
  `forceSnapshot`/pageSet drain semantics) are unaffected — pinned by a
  counting-backend test asserting the streaming methods are used and the
  buffered ones never are, that both scratch files are cleaned up on every
  path, and that streamed output materializes byte-identically to a direct
  encode; a separate test pins the no-`ReaderPutter` fallback unchanged.
  S3's `PutReaderIf`/`PutReader` inherit `PutObject`'s single-request 5 GiB
  ceiling (same limit `CopyObject` already documents); multipart upload to
  lift it is a noted follow-up, not implemented here. `make test-torture`:
  3168 rounds, 316 kill -9 bounces, 316/316 resumed cleanly, zero
  corruption.

- **Snapshot flushes no longer hold the replica lock across the full-file
  encode** (perf audit M2). `Session.flush`'s snapshot branch now clones the
  replica to a per-flush scratch file under `replicaMu` (a copy-on-write
  reflink — near-O(1) — on APFS/btrfs/xfs, plain byte copy elsewhere),
  releases the lock, and runs `EncodeSnapshot` over the scratch outside it,
  so the capture engine's `Apply`/`Rebase` no longer stall for the O(DB
  size) encode+checksum on every snapshot flush (multi-GB replicas: seconds
  of `Lag()` spike per snapshot, gone). The clone is taken while `replicaMu`
  freezes the replica at the flush's transaction boundary, so the uploaded
  bytes and checksum are exactly what encoding the live replica under the
  lock produced before; upload/ref-CAS ordering, `forceSnapshot`/pageSet
  drain semantics, and the segment path (already O(changed pages), still
  encoded under the lock) are unchanged. The scratch is removed on every
  exit path.

- **Capture no longer fsyncs its resume-offset state file per transaction**
  (perf audit M3). `drainStep` now records the applied position in memory
  and the engine persists it once per drain burst (plus at shutdown's final
  drain), lifting the ~200 txn/s capture ceiling the per-commit ~5ms fsync
  imposed. Safe because the persisted `Off`/`Salt1`/`Salt2` are operator
  observability only — resume eligibility rests solely on the verified-clean
  marker (`Clean` + empty WAL + `MainHash`), which is unchanged — and the
  persisted offset can now lag the replica by up to one in-flight burst but
  never lead it. Clean shutdown/restart behavior (resume without rebase) is
  unchanged; a kill -9 mid-burst still safely rebases on the next start.

- **Chain resolution of a diverged shared fork now Lists the child's prefix
  once instead of twice** (perf audit H1). The seam path — a shared child
  with its own segments but no divergence-floor snapshot yet, the normal
  working state of an active fork — reuses one parsed member listing for
  both the snapshot-anchored walk and the seam-range walk. One less List
  RPC per affected lineage per resolution (Checkout, Fork, Compact, flush's
  divergence seed); resolution results are byte-identical.

- **A materializing (snapshot-floor) fork resolves the source chain once,
  not twice** (perf audit M1). Fork passes the chain it already resolved
  for the floor decision straight to the snapshot copy instead of letting
  the copy re-resolve the identical (lineage, txid) — the redundant
  resolution was most expensive exactly when the floor trips, i.e. on a
  long chain. Rollback and Promote are unchanged; fork results are
  byte-identical.

- **GC's phase-2 sweep batch-deletes on S3: 1000 objects per DeleteObjects
  RPC instead of one serial DeleteObject each** (perf audit H2). Destroying
  a month-old branch (~80k objects) now sweeps in ~80 round trips instead
  of ~80k (~an hour inside a single janitor tick). Implemented as an
  optional `store.BatchDeleter` capability — S3 chunks into 1000-key
  DeleteObjects requests; the local backend loops; a backend without the
  capability falls back to per-key deletes. The swept key set, tombstone
  pruning, and abort-on-error semantics are unchanged (a partial batch
  failure prunes exactly the succeeded keys' tombstones and keeps the
  rest for a later pass); only the RPC shape changed.

- **A sweeping GC pass no longer re-enumerates refs a third time** (perf
  audit M4). The compensating rule's live-head set is derived from the same
  ListRefs + GetRefs the phase-2 re-mark already performed, saving
  1 ListRefs + one GetRef per ref per sweeping pass — and guaranteeing the
  "never delete above a live head" rule sees exactly the refs the re-mark
  saw (one consistency window). GC results are identical.

- **Repeated shared forks pay the layout-v2 manifest check once per
  process, not once per fork** (perf audit L4). The layout version is
  monotonic, so the store memoizes the "manifest already >= v2"
  observation after the first EnsureLayoutV2; subsequent shared forks skip
  the manifest Get entirely — 1 RPC saved per fork, ~15% of a shared
  fork's total RPC bill.

- **A CI image that loses sqlite3/sqldiff/promtool now fails loudly instead
  of silently skipping the integration suite green.** The ~96 copy-pasted
  `exec.LookPath` skip guards across the test suite collapsed into a shared
  `internal/testutil` helper (`RequireSQLite3`/`RequireSQLdiff`/
  `RequirePromtool`): locally a missing binary still skips the test, but
  under CI (`CI` non-empty; `OFFSHOOT_REQUIRE_PROMTOOL` for promtool's
  dedicated metrics-lint job) it is a hard failure. Test/CI-only change.

- **Best-effort orphan cleanup is now loud.** The seven orphan-cleanup
  deletes in ops (lost ref-CAS paths in create, checkpoint, fork, rollback,
  promote, compact) and Destroy's checkout-file removals now log to stderr
  when the underlying delete/remove fails, instead of failing silently.
  Behavior is otherwise unchanged: cleanup remains best-effort and
  reachability GC still reclaims anything left behind.
- Named previously-hardcoded values: the session `DrainNow` budget (30s,
  shared by flush and close), the capture engine's checkpoint-takeover
  thresholds (64 frames / 5s idle), and the two deliberately different
  SQLite busy timeouts (ops quiesce 3000ms, capture engine 5000ms). Values
  unchanged.
- internal: split the checkout-fingerprint (.sum sidecar) subsystem out of
  `internal/ops/ops.go` into `internal/ops/sidecar.go` (no behavior change).
- internal: split the janitor (reap/stale-claim healing/GC/ro-cache
  eviction loop) out of `internal/daemon/server.go` into
  `internal/daemon/janitor.go` (no behavior change).
- Refreshed doc comments that still described pre-daemon/pre-CoW behavior
  (`Ref.Base`, `Ref.Epoch`, `EnsureLayoutV2`, the ops package doc, the
  capture `Sink` contract and `hashSrc` cost note).

### Removed

- `store.Store.DeleteRef` (unused in production; `Destroy` uses the
  claim-guarded `DeleteRefIf`). Internal API only.

### Security

- **`export` is no longer reachable over HTTP `/rpc`** (400, "not
  available over HTTP; use the local socket"). It is the one op that
  writes to an unconfined, client-chosen path on the daemon host's
  filesystem — safe under the unix socket's same-host/same-user trust
  model, but an arbitrary-file-write/exfiltration primitive for an
  authenticated network client on a non-loopback bind. Unix-socket
  export is unchanged.
- **The daemon's unix socket is created 0600 from the first instant**
  (a 0o077 umask around the listen), closing the window between
  `net.Listen` and the subsequent chmod in which — for a `-socket` path
  in a world-accessible directory — another local user could connect
  and get a full daemon client. The default socket path (0700 parent
  dir) was never exposed.

## [0.2.1] - 2026-08-08

### Added

- **`offshoot_fork_mode_total{mode="shared"|"materialized"}`** — a
  fork-time counter for the copy-on-write storage mode, completing the
  0.2.0 observability story: `status`/`branches` already report
  shared-vs-materialized per branch at read time; this records it as a
  rate at fork time. `shared` = a base pointer into the parent's chain,
  zero data objects copied; `materialized` = a full snapshot copy (the
  fork-time snapshot floor). Orthogonal to `offshoot_fork_total{path}`,
  which names the materialize path's copy strategy. Both label values
  pre-registered at `0`; documented in docs/operations.md.
- **`offshoot_gc_errors_total`** — counts janitor GC passes that returned
  an error. GC deliberately **fails closed** (an incomplete reachability
  mark deletes nothing), which previously left a stalled GC silent while
  the store bloated: the error went to stderr only, with no alertable
  signal. The stderr line still carries the cause; the counter is the
  signal to alert on.

### Fixed

- **Daemon `compact` now refuses while the branch has an open session on
  this daemon** ("close the session first"), matching `rollback` and
  `promote`: compact repoints the branch to a new self-contained lineage,
  so a live session's next flush would CAS-fail against the repointed
  ref. Previously the daemon flushed the session and compacted anyway,
  leaving the session fenced. With no session open the durable head is
  already current, so compact materializes exactly that head — behavior
  with no open session is unchanged.
- **The fork-time snapshot floor now tracks the configured snapshot
  cadence** instead of the hardcoded default (16). A daemon started with
  `-snapshot-every N` below 16 could previously mint shared forks whose
  resolved base chains reached up to 15 members — bounded, but looser
  than the session's own `len(chain) <= SnapshotEvery` cadence. The
  daemon now passes its cadence through to the fork floor
  (`Workspace.SnapshotEvery`), so the fork floor and the divergence floor
  agree. The at-rest CLI `fork` and any daemon without `-snapshot-every`
  keep the default bound of 16 — behavior unchanged.
- **`store.Chain` now detects base-pointer cycles** in a corrupt or
  hand-edited store and returns a clear `base pointer cycle detected`
  error instead of recursing until the stack overflows. Not reachable
  through this binary's writers (`WriteLineageBase` is create-only,
  rejects a self-base, and always points at an existing lineage) — this
  is crash-hardening for a corrupt store, mirroring `BaseSpine`'s
  existing defensive cycle guard.
- Docs: `offshoot compact`'s cost class in docs/reference.md no longer
  claims a single compact pays "the same N×G copy" — N×G is the
  aggregate across N materialized forks; one compact/promote/rollback
  pays ~G (one full copy). README's install line no longer says tagged
  binaries land "with the first 0.1.x release" (stale since 0.2.0
  shipped; binaries are published per tagged release). The original
  design spec's launch-demo bullet gained a superseded-note: under
  copy-on-write there is no async-upload/pending window for a shared
  fork to caveat.

## [0.2.0] - 2026-08-07

**Copy-on-write forks.** `offshoot fork` no longer materializes a full
copy of the source into the child's lineage. The common-case fork now
**shares**: it writes a durable base pointer into the parent's
already-durable chain — zero data objects of its own — so N forks of a
G-byte database cost near-zero added store bytes instead of up to N×G.
The rest of this release is what makes that safe: base-following chain
resolution under a strict never-merge-across-lineages rule, two automatic
snapshot floors that keep read materialization bounded on any fork spine,
an object-granular reachability GC, a new `offshoot compact` command to
cut a shared branch's cord, and honest per-branch cost reporting in
`status`/`branches`.

**Storage-format change — the reason this release is 0.2.0, not 0.1.4.**
The first shared fork CAS-bumps the store manifest from LayoutVersion 1
to 2, and every pre-0.2.0 binary then refuses the **entire store** up
front (`CheckManifest`). The lockout is deliberate: a 0.1.x binary's
lineage-granular GC cannot see base pointers and would sweep a shared
ancestor's objects out from under live children — silent data loss — so
old binaries are fenced out of the store rather than allowed to corrupt
it. There is no downgrade path once a base pointer exists; upgrade every
binary that touches a store before any of them forks. A 0.2.0 binary
reads and writes a v1 (no-base) store unchanged, and GC's reachability
handles a mixed store (old full-copy forks + new shared forks) uniformly.

**The two cost models, stated honestly:** fork shares (near-free);
`promote`, `rollback`, and `compact` each still materialize a full
independent copy — "fork at checkpoint X is free but rollback to X costs
a full copy" is a deliberate, documented asymmetry, not an oversight
(rollback/promote abandon their old lineage; base-pointing into a lineage
that is meant to die would pin it forever). Destroying a parent stays
instant and always allowed, but its **bytes** are now reclaimed only once
no surviving child's chain still reads through them — destroy lingers
until the last sharing child is destroyed or compacted.

### Changed

- **Fork is copy-on-write** (Tasks 1–4). The share path writes only a
  durable per-lineage base pointer (`data/<lineage>/base.json`, written
  create-only via `store.WriteLineageBase`) plus the child ref's new
  `Base *BasePointer` reporting mirror (`BasePointer{Lineage, TXID}`,
  deliberately separate from the human-facing `Parent` breadcrumb, and
  deliberately epoch-free — pinning an epoch could resurrect a fenced
  writer's orphan). Resolution: `store.Chain` follows base pointers
  transitively under the one hard rule that members are never merged
  across lineages — each lineage's half is resolved wholly within its own
  prefix and concatenated at a seam verified contiguous, so
  `keepHighestEpoch` can never let a higher-epoch parent object win a
  child's txid range. The base object (not the ref mirror) is the
  resolution source of truth, so a shared parent's ref can be destroyed
  while descendants still resolve through its lineage. Bounded
  materialization survives sharing via two automatic floors, both keyed
  on the existing snapshot cadence: a **fork-time floor** (a fork whose
  fully-resolved chain is already at `ops.ForkShareMaxDepth` materializes
  one fresh snapshot instead of sharing — the pre-0.2.0 fork path, now
  the fallback rather than the default) and a **divergence floor** (a
  session's snapshot counter seeds from the branch's durable divergence,
  so the `SnapshotEvery` bound is a property of the branch, not the
  process, and a shared child stops touching its parent once it has
  written its own snapshot). Tests cover divergence isolation both ways,
  fenced-orphan safety at the fork point, bounded replay across a
  40-level fork spine with content verification, and the
  cross-lineage-merge hazard (mutation-verified: a resolver that unions
  across lineages fails).
- **GC is an object-granular reachability collector** (Task 5), a
  rewrite forced by sharing: liveness can no longer be decided per
  lineage when one lineage can be live-below-a-fork-point for a
  descendant and dead-above-it for everyone. The mark phase computes the
  live set as the union, over every live branch, of its resolved chain at
  head AND at every checkpoint — using `store.Chain` itself, never a
  reimplementation, so mark == read-path resolution by construction
  (property-tested) — plus every `base.json` along each branch's base
  spine. Sweep follows at object granularity: `gc/tombstones` maps
  individual object keys (stale lineage-keyed entries are pruned
  harmlessly); the two-phase tombstone → grace → re-mark → delete shape,
  mint-this-run skip, and clobber-safe stone persist all survive at the
  new grain. `GC`'s `tombstoned`/`deleted` counts, the CLI `gc` summary
  line, and the `offshoot_gc_*` metrics now count **objects, not
  lineages**. A per-pass memoized resolver keeps a shared ancestor's
  prefix listed once per pass regardless of descendant count. Two
  review-driven hardenings: a tombstone rescued by the re-mark is pruned
  (a later legitimate death earns a fresh grace period), and the sweep
  never deletes above a live branch's head in its own lineage (a
  retrying flush can legitimately re-create exactly that key). Covered by
  reachability, mixed-store, checkpoint-survival, and
  ancestor-destroyed-mid-fork-of-fork fault-injection tests, each
  mutation-verified.
- **`offshoot status` output** (Task 7): every branch line now carries a
  `storage=shared|materialized` field between `state=` and `txid=` — the
  per-branch cost class, surfacing the fork-shares vs
  promote/rollback/compact-materialize asymmetry instead of hiding it.

### Added

- **`offshoot compact <db>[@branch]`** (Task 6) — the manual
  cord-cutter: re-encodes a shared branch's full base-following chain at
  head as ONE self-contained snapshot in a fresh lineage, CAS-repoints
  the ref with its base pointer cleared, and best-effort refreshes the
  checkout. The previously shared ancestor storage becomes unreferenced
  and is reclaimed by GC (compact itself deletes nothing). An already
  self-contained branch is a no-op returning the head txid, so scripted
  "compact everything" loops never fail. **Flagged tradeoff:** the
  checkpoint map resets to `{"compact": head}`, like promote's repoint
  (old checkpoints anchored on the shared ancestor would not resolve in
  the new lineage; preserving them rollback-style is a noted follow-up).
  A CAS loss to a concurrent flush deletes the orphan snapshot and
  returns a retry error. Ships as a daemon op (an open session is flushed
  first, like fork's source flush) and SDK `compact()` in Python and
  TypeScript. Review fix folded in: `Promote` and `Rollback` now clear
  the ref's `Base` mirror when they repoint a formerly-shared branch
  (previously a stale mirror survived, misreporting the branch as shared
  — and compact trusting it would needlessly re-materialize and wipe
  checkpoints Rollback had preserved); compact's no-op decision consults
  the durable base spine (`store.BaseSpine`), not the mirror, so the
  destructive path stays a no-op on a self-contained branch even against
  a stale mirror.
- **Per-branch cost reporting on the wire** (Task 7): `BranchInfo` (the
  daemon `branches` op) gains a wire-additive `shared` bool — true for a
  base-pointer fork (near-zero added storage), false for a materialized,
  self-contained branch — mirrored by `ops.BranchStatus.Shared` for the
  at-rest CLI `status`. No `omitempty`: false means "materialized," never
  "unknown"; old clients that don't read the key are unaffected.
- **Store plumbing for all of the above** (Tasks 1–2):
  `store.LayoutVersion` 2 (new stores are stamped v2 at init;
  `Store.EnsureLayoutV2` CAS-bumps an existing v1 manifest exactly once,
  idempotent under concurrent callers, called before the first base
  pointer is written), `store.BaseKey`/`WriteLineageBase` (which refuses
  a self-referential base) and `store.BaseSpine` (the durable
  base-ancestor walk), and `MaterializeChain`'s per-member
  `PreApplyChecksum` verification doing fail-closed duty on every
  resolved splice — a mis-resolved chain with divergent content is a loud
  materialization failure, never silent corruption.

## [0.1.3] - 2026-08-07

Milestone 4: operable at scale. A platform running hundreds of agent
sessions can now see (a hand-rolled, zero-dependency Prometheus `/metrics`
endpoint plus six computed branch states), bound (a `checkouts-ro` disk
budget with LRU eviction), and automate (an opt-in, token-authenticated
HTTP listener alongside the unix socket, plus an event bus drainable over
either transport) offshoot — without any of it requiring multi-node
anything; see [docs/operations.md](docs/operations.md)'s first paragraph
and [ROADMAP.md](ROADMAP.md#non-goals-v1) for that standing scope
boundary. **Metric names are locked as of this release: every
`offshoot_*` name below is now API — renaming one after this tag is a
breaking change** (this was the one free rename window; see PM Amendment
2/12 in the Milestone 4 plan). Operator-facing docs land in this release
too: [docs/operations.md](docs/operations.md) (the metrics/states/events/
budgets/HTTP-auth reference in one page) and
[docs/recipes/kubernetes.md](docs/recipes/kubernetes.md) (a real,
schema-validated sidecar manifest). See
[docs/status.md](docs/status.md)'s Standing nag section for what's still
user-gated (PyPI/npm publication, the `offshoot-db` org transfer, registry
listing submissions) — none of it is blocked on further engineering.

### Added

- **Branch state taxonomy** (Milestone 4 Task 1): `ops.BranchStateAt`
  (`internal/ops/status.go`) computes a branch's state — `active`, `dirty`,
  `detached`, `idle` — from its ref, lease liveness, and checkout `.sum`
  sidecar alone, no daemon dependency; a daemon layers two more states
  (`pending`, `error`) on top from its own in-memory session map
  (`internal/daemon/server.go`'s `Server.branchState`), since only a daemon
  knows which branches have a session reserved or errored. Precedence:
  `error` > `pending` > `active` > `dirty` > `detached` > `idle`. `idle` is
  a deliberate addition beyond the design spec's original taxonomy, which
  assumed a daemon was always present — see
  [docs/reference.md](docs/reference.md#branch-states) for the full table
  and rationale. Surfaced in `offshoot status` (`BranchStatus.State`), the
  daemon `branches` op (`BranchInfo.state`, always present — additive, not
  `omitempty`), and both SDKs' `Branch.state` (Python dataclass field,
  TypeScript interface field; both default to `""` against an older
  daemon that never sends it, so existing SDK code is unaffected).
  **Disclosed cost:** determining `dirty` for a checked-out, unleased
  branch whose sidecar identity already matches the ref requires a real
  WAL checkpoint (quiesce, up to a 3s busy timeout) followed by a full
  SHA-256 hash of the checkout — per branch, per `status`/`branches` call;
  a checkout that's busy during that checkpoint attempt is itself reported
  `dirty` rather than `idle` — see
  [docs/reference.md](docs/reference.md#branch-states)'s Cost/Known
  blind spot notes.

- **Metrics registry + instrumentation** (Milestone 4 Task 2):
  `internal/metrics` is a hand-rolled, zero-dependency, concurrent-safe
  Prometheus text-exposition registry — `Counter`/`Gauge`/`Histogram` (fixed
  buckets, cumulative `_bucket`/`+Inf`/`_sum`/`_count`), `CounterVec`/
  `GaugeVec` for bounded label sets, `Registry.WritePrometheus(io.Writer)`.
  Kept `internal` and zero-dep by design (one-sentence rationale on the
  package doc comment, per PM Amendment 7) so a later `client_golang` swap
  is a call-site-only change. `internal/daemon`'s `newMetrics` registers
  every metric on the plan's LOCKED name list: `offshoot_build_info{version}`,
  `offshoot_sessions_open`, `offshoot_capture_lag_bytes{db,branch}` /
  `offshoot_durable_age_seconds{db,branch}` (open sessions only, computed at
  SCRAPE TIME from the sessions map via a `Registry.Collect` callback, never
  continuously), `offshoot_flush_total{result,kind}` /
  `offshoot_flush_duration_seconds`, `offshoot_fork_total{path}` /
  `offshoot_fork_duration_seconds`, `offshoot_checkpoint_duration_seconds`,
  `offshoot_reap_total`, `offshoot_gc_tombstoned_total` /
  `offshoot_gc_deleted_total` / `offshoot_gc_backlog`,
  `offshoot_ro_cache_bytes` / `offshoot_ro_cache_evictions_total`
  (registered now at zero — Task 5 wires real numbers),
  `offshoot_janitor_runs_total{result}`. Instrumentation: `internal/ops`
  gained `ObserveFork`/`ObserveCheckpoint`, package-level nil-checked
  injected-hook vars (ops must not import `internal/metrics`) the daemon
  assigns once at `NewServer` construction; `internal/session` gained an
  `OnTransition` hook fired from the SAME `logTransition` call site
  Milestone 2 already used for its transition logs (not a new call site —
  Task 2's brief was explicit: hook the existing sites, don't restructure),
  which the daemon uses to drive flush counters/durations from the existing
  "flushed"/"flush-failed" events (and `flush()` now times itself and
  reports `duration_seconds` in that same kv). Janitor-loop metrics
  (reap/GC/backlog/`janitor_runs_total`) are updated from
  `Server.janitorTick`, split out of `StartJanitor`'s ticker body so a test
  can drive one tick deterministically. **PM Amendment 7 hard gate:**
  `WritePrometheus` output is golden-file-tested
  (`internal/metrics/testdata/golden.txt`) and validated against Prometheus's
  own `promtool check metrics` linter (`TestPromtoolCheckMetrics` in
  `internal/metrics`, `TestPromtoolCheckRealMetrics` in `internal/daemon`
  against the real, fully-wired registry) — both skip loudly (not silently)
  when `promtool` isn't on `PATH`; `.github/workflows/ci.yml` gained a new
  `metrics-lint` job that downloads a pinned `promtool` release and runs
  both. `internal/daemon/metrics_test.go`'s `TestMetricsSmokeOpenWriteFlushFork`
  opens a session, writes, flushes, and forks over the real unix-socket RPC
  path, then scrapes the registry directly (no HTTP yet — that's Task 3) and
  asserts the counters moved. Not yet exposed over HTTP (`GET /metrics`,
  Task 3) — see [docs/status.md](docs/status.md) for the exact split.

- **HTTP listener + token auth** (Milestone 4 Task 3): `offshoot serve
  -http ADDR` (off by default) starts an opt-in HTTP listener
  (`internal/daemon/http.go`) alongside the unix socket: `POST /rpc` (the
  same `Request`/`Response` JSON and `Server.dispatch` the socket uses —
  byte-identical semantics, `TestHTTPRPCParityWithSocket`; 1MiB body cap
  via `http.MaxBytesReader`, `413` on overflow with the connection still
  usable afterward; `Content-Type: application/json` required), `GET
  /metrics` (the T2 registry's Prometheus text exposition), `GET /healthz`
  (unauthenticated, `{"ok":true,"sessions":N}`), and `GET /debug/pprof/*`
  (PM Amendment 6: `net/http/pprof`'s standard handlers, same auth as
  everything else). Every route but `/healthz` requires `Authorization:
  Bearer <token>`, compared with `crypto/subtle.ConstantTimeCompare`
  (`checkAuth`) — never Go's built-in `==`. Token source: `-token` /
  `OFFSHOOT_TOKEN`, else `GenerateToken` (32 random bytes, hex-encoded)
  printed to stderr exactly once at startup (loopback binds only); ongoing
  output (including a later status/log line) shows only an 8-character
  `TokenFingerprint`, never the token again — `TestHTTPTokenRedaction`,
  `TestHTTPAutoGeneratedTokenPrintedOnceThenOnlyFingerprint`. A non-loopback
  bind additionally requires BOTH `-http-allow-non-loopback` (an explicit
  ack) AND an explicit token — two distinct startup errors if either is
  missing (`ValidateHTTPBind`, `TestValidateHTTPBindErrorsAreDistinct`),
  checked before any listener is created. `http.Server` runs explicit,
  justified timeouts (`ReadHeaderTimeout` 5s, `ReadTimeout` 30s,
  `WriteTimeout` 90s — sized to leave headroom for `/debug/pprof/profile`'s
  default 30s capture window, `IdleTimeout` 2m) instead of the stdlib's
  unbounded defaults, per PM Amendment 6.

  **Shutdown ordering:** the HTTP listener is closed (`http.Server.Close`,
  immediate — matching the unix socket's own force-close-live-connections
  philosophy, not a graceful drain) alongside the unix listener in
  `Server.Shutdown`. An HTTP `op=shutdown` request over `/rpc` gets the
  same respond-then-shutdown-trigger fix the unix socket's `handle` got in
  Milestone 3 (dispatch, encode the response, **flush it to the wire**,
  and only then trigger `Shutdown` in a fresh goroutine) —
  `TestHTTPShutdownRespondsBeforeClosingRequestingConn`, using the same
  delay-hook technique as the socket's own regression test, confirmed to
  fail reliably against a build with the ordering reversed. A generic
  hammer test (`TestHTTPShutdownWhileRequestsInFlightIsSafe`) proves no
  panic or hang under `-race` while `Shutdown` races real in-flight
  requests.

  **T2 review carried item, closed:** concurrent `GET /metrics` scrapes
  used to be able to tear the session-gauge collector's output — it
  `Reset`s then repopulates two `GaugeVec`s (`offshoot_capture_lag_bytes`/
  `offshoot_durable_age_seconds`) in two steps that were only atomic if no
  OTHER concurrent scrape's run of the same collector interleaved with it.
  Fixed by adding `scrapeMu` to `internal/metrics.Registry`, serializing
  `WritePrometheus` end to end (collector run AND render) — chosen over a
  Server-level scrape lock or making every collector independently
  concurrency-safe, since it's the single minimal choke point and
  concurrent scrapers simply queue at ordinary Prometheus scrape cadences.
  `internal/metrics/metrics_test.go`'s
  `TestWritePrometheusSerializesConcurrentCollectorRuns` (Registry-level,
  synthetic) and `internal/daemon/http_test.go`'s
  `TestConcurrentMetricsScrapesWithSessionChurn` (real HTTP, real session
  open/close churn across several branches, 8 scraper goroutines x 40
  scrapes) both assert the invariant that must hold in every single scrape
  (`offshoot_sessions_open`'s value equals the count of
  `offshoot_capture_lag_bytes` sample lines) and are both confirmed to
  fail reliably with `scrapeMu` removed.

  **Threat model** (PM Amendment 3, stated plainly in
  [docs/reference.md](docs/reference.md)): single-tenant,
  same-host-or-trusted-network auth — the token is a shared secret, not a
  multi-tenant isolation boundary; no TLS (the operator-facing
  `docs/operations.md` page itself is Task 8). `docs/status.md`'s HTTP row
  flips to shipped-and-tested; `docs/reference.md` gains the `-http`
  flag/route table and `OFFSHOOT_TOKEN`.

- **Eventing: bus + socket `subscribe` + SSE** (Milestone 4 Task 4a):
  `internal/daemon/events.go` adds an in-daemon event bus and two ways to
  drain it — the unix socket's new `subscribe` op and HTTP's `GET
  /events` (Server-Sent Events) — both streaming the exact same versioned
  JSON (PM Amendment 4: one schema, one encoder): `{v:1, ts, type, db,
  branch, detail{}}`, `type` one of `session_opened` / `flushed` /
  `flush_failed` / `fenced` / `session_closed` (all five fed from the
  SAME `internal/session` transition-callback call site Milestone 4 Task
  2 already hooked — `internal/session` itself is untouched, per this
  task's "ride the existing callback" brief) / `reaped` (fed from the
  janitor's `Reap` pass) / `evicted` (schema slot reserved now; nothing
  emits it yet — Milestone 4 Task 5's ro-cache LRU eviction path is the
  intended source and will publish to the bus at its own eviction call
  site once it exists).

  **Never blocks the daemon (Global Constraint):** `eventBus.publish` is a
  non-blocking fan-out — a subscriber whose bounded buffer (64 events) is
  already full is immediately dropped: removed from the subscriber set, sent
  a single terminal `{type:"dropped_slow_consumer"}` event, and has its
  channel closed, all without ever blocking the publisher (the session
  transition callback or the janitor). `TestSlowSubscriberDroppedSessionKeepsFlushing`
  proves this end to end — a subscriber that never reads its channel is
  dropped with the terminal event while a concurrently write-heavy session
  keeps flushing successfully throughout, unaffected.

  **Socket `subscribe`:** acks, then the connection PERMANENTLY LEAVES
  request/response mode and streams line-per-event JSON until the client
  disconnects — `handle()`'s per-connection loop special-cases this op
  exactly like it already special-cases `shutdown` (subscribing to the bus
  happens *before* the ack is sent, closing a race where a transition could
  fire in the gap between ack and subscription). **SDKs and any other
  caller MUST use a fresh, DEDICATED connection for `subscribe`** — it is
  unix-socket-only; a `subscribe` op sent over HTTP `POST /rpc` is refused
  with a message pointing at `GET /events` instead.

  **HTTP `GET /events`:** same Bearer auth as everything but `/healthz`;
  `data: <event JSON>\n\n` per event, plus a periodic `: ping` SSE comment
  (`sseKeepaliveInterval`, 15s default — PM Amendment 12: proxies/kubelets
  kill silent streams) to keep the connection alive across anything sitting
  in front of this daemon. **Re-arms its own per-write deadline** before
  every write (`http.NewResponseController(w).SetWriteDeadline(now +
  eventWriteDeadline)`, 45s default) rather than clearing it once,
  permanently — Task 3's `http.Server.WriteTimeout` (90s) covers a
  handler's *entire* wall-clock run with no concept of "still legitimately
  sending, just slowly," and would otherwise hard-cut every long-lived SSE
  stream at 90 seconds; a live stream re-arms this deadline forward on
  every successful write (and at least every keepalive tick), so it's
  never affected, while a subscriber that's still connected but has
  stopped reading entirely now gets its stream torn down within
  `eventWriteDeadline` instead of pinning the handler goroutine and its
  connection's file descriptor open forever. The unix socket's
  `streamEvents` gets the identical per-write re-arm before every
  `c.Write`, for the same reason. `TestSSEStreamSurvivesPastWriteTimeout`
  proves the live-stream side structurally and cheaply: `httpWriteTimeout`
  is a test-only var shrunk to 150ms, and the stream is proven alive well
  past that shrunk value via a real event delivered afterward — no test
  ever sleeps anywhere near the real 90s.

  Tested (`internal/daemon/events_test.go`, `-race`): a subscriber sees
  `session_opened` → `flushed` → `session_closed` for a real session
  lifecycle over the socket; the slow-subscriber-dropped-while-session-
  keeps-flushing proof above; SSE/socket parity (both transports
  subscribed before a real session lifecycle runs, asserted to observe the
  identical event sequence); the keepalive ping; the write-deadline proof;
  a genuinely STALLED (still-connected, never-reading) subscriber on
  either transport has its connection/handler torn down within
  `eventWriteDeadline`, forced through the real write path with an
  oversized event rather than merely asserted
  (`TestStalledSocketSubscriberConnectionIsClosedWithinWriteDeadline`,
  `TestStalledSSESubscriberConnectionIsClosedWithinWriteDeadline` — both
  confirmed to fail reliably against the pre-fix code); `/events` requires
  auth (401 without, plus a path-trick matrix extension); `subscribe` over
  `POST /rpc` is refused; reusing a subscribed connection for an ordinary
  op gets silence, never a Response.
  No `internal/session` changes — bus is fed purely from the daemon-side
  callback composition, so no torture run was required for this task.

- **Eventing: SDK stream helpers** (Milestone 4 Task 4b): both SDKs ship a
  thin `events()` helper over Task 4a's socket `subscribe` op — Python
  `Client.events()` (`sdk/python/offshoot/client.py`) is a generator
  yielding `Event(v, ts, type, db, branch, detail)` dataclass instances;
  TypeScript `Client.events()` (`sdk/typescript/src/client.ts`) is an
  `AsyncGenerator<OffshootEvent>` (a new versioned interface, mirroring the
  wire schema). Both **open their own fresh, dedicated socket connection**
  — never the caller's own `Client` connection, matching Task 4a's
  MANDATORY "subscribe permanently takes over its connection" contract
  (see both methods' doc comments and docs/reference.md's Eventing
  section) — send `subscribe`, read the ack, then yield one parsed event
  per line in publish order until the stream ends. Stdlib-only / zero-dep
  on both sides (Python: only `json`/`socket`, already-imported stdlib;
  TypeScript: only `node:net`, already-imported).

  **`dropped_slow_consumer` contract, decided and documented**: yielded
  like any other event, then the stream simply ends (Python: the generator
  returns, no `StopIteration`-wrapped error; TypeScript: the async
  iterator's `done: true`) — NOT raised/thrown as an `OffshootError`. A
  drop ends the stream the same way an ordinary disconnect does; a caller
  that cares whether it was dropped checks the last event's `type`. Only a
  genuine transport/protocol failure (a connect error, a malformed line, a
  daemon `ok:false` ack) raises/throws.

  **Closing early, no leaked fd**: Python — `break`/`generator.close()`
  delivers `GeneratorExit` at the suspended `yield`, caught by a `finally`
  that closes the buffered reader and the socket. TypeScript — `break`/
  `.return()` on the async iterator resumes the generator's suspended
  `yield` as an abrupt completion, running an identical `finally` that
  `sock.destroy()`s the dedicated connection. Both proved against a real
  daemon: a helper connection sees one real event, then closes mid-stream,
  and the daemon's open unix-domain-socket fd count (`lsof -p <pid>`
  filtered to `TYPE unix` — a raw total-fd comparison would be confounded
  by `internal/dbfile`'s deliberately-never-closed checkout descriptors)
  returns to its pre-subscribe baseline, and the daemon keeps answering
  ordinary ops on an unrelated connection throughout.

  **Real end-to-end lifecycle tests** (`sdk/python/tests/test_client.py`'s
  `test_events_sees_session_lifecycle_in_order`, `sdk/typescript/test/
  client.test.ts`'s matching test): the helper subscribes on its own
  connection; `session_opened` → `flushed` → `session_closed` is driven by
  real ops on a SEPARATE connection and observed in order with correct
  `db`/`branch`/`detail` fields. The `dropped_slow_consumer` path is hard
  to force deterministically from the SDK side (it needs the real bus's
  internal buffer-overflow timing — already proven server-side by Task
  4a's `TestEventBusDropsSlowSubscriberWithTerminalEvent`); both SDKs
  instead unit-test the helper's own decode/contract path end to end
  against a small scripted fake daemon speaking the identical
  line-per-event wire shape (`TestEventsDecodePath` / the `events():
  dropped_slow_consumer ...` `node:test` cases), covering the terminal
  event, a malformed line, and a not-ok ack.

  Both SDK suites green under `make test-sdks`; both SDKs confirmed
  stdlib-only/zero-dep (import inspection, no new dependency added to
  either `pyproject.toml` or `package.json`).
  `docs/status.md`'s eventing row flips to shipped-and-tested;
  `docs/reference.md` gains the `subscribe` op and `GET /events` route,
  with the dedicated-connection warning restated for operators.

- **Ro-cache disk budget + LRU eviction** (Milestone 4 Task 5): `offshoot
  serve -ro-cache-budget <bytes|0>` (default `0` = unlimited) bounds
  `checkouts-ro`'s total size — the read-only cache `CheckoutAt` (Milestone
  3 Task 2) materializes into and which, until now, grew without bound.
  The janitor's `Server.janitorTick` (`internal/daemon/server.go`) runs a
  new ro-cache pass on the same `-reap-every` cadence as reap/GC: it always
  computes and republishes current usage (`offshoot_ro_cache_bytes`, the
  T2-registered gauge, previously always 0) — a budget-less or
  already-under-budget pass still reports a real number, it just never
  evicts — and, once usage exceeds a configured budget, LRU-evicts entries
  (oldest first) until it's back under. A bare integer is bytes (the
  contract); `-ro-cache-budget` and `status`'s own display flag of the
  same name also accept a trailing power-of-1024 size suffix (`K`/`KB`,
  `M`/`MB`, `G`/`GB`, `T`/`TB`) as a convenience (`parseByteSize`,
  `cmd/offshoot/main.go`) — tested directly (`TestParseByteSize`) since
  the brief called out "if you add size parsing, test it."

  **LRU clock (PM Amendment 11, pre-decided): a `.last-used` touch-on-HIT
  marker file**, not the `.db` file's own mtime — `CheckoutAt`'s
  force=false cache-hit path (`internal/ops/export.go`) now calls
  `touchLastUsed`, which creates/`Chtimes`s a `<cachefile>.last-used`
  sidecar to the current time on every cache HIT. This is necessary, not
  cosmetic: `materializeAt`'s rename-into-place is the LAST thing that ever
  touches the `.db` file's own mtime (a cache hit is a pure read — it must
  never touch the `.db` file again), so without a separate marker "least
  recently used" would collapse to "least recently created," exactly
  backwards for a cache whose whole point is that a repeatedly-hit
  checkpoint should stay hot. `internal/ops/rocache.go`'s `lruClock` ranks
  by the marker's mtime when present, falling back to the `.db` file's own
  mtime as the documented floor for an entry materialized but never since
  hit. `TestEvictROCacheLastUsedTouchBeatsCreationOrder` and its daemon-level
  counterpart `TestJanitorTickEvictsLRUUnderBudgetAndFiresEvictedEvent`
  both prove this directly: an OLDER (by creation) entry that gets hit
  survives eviction while a NEWER, never-hit entry is evicted instead.

  **`checkouts/` (writable, leased) is never evicted — by construction, not
  a runtime check**: `ops.Workspace.EvictROCache` only ever walks and
  removes paths under the separate `checkouts-ro` tree (never joined with,
  or reachable from, `checkouts/`'s own path shape — see `CheckoutAtPath`'s
  existing doc comment on that separation); there is no code path here that
  can construct or be handed a `checkouts/` path at all.
  `TestEvictROCacheNeverTouchesWritableCheckout` (ops-level, budget=1 as
  aggressive as it gets) and `TestJanitorROCacheEvictionNeverTouchesLeasedWritableCheckout`
  (daemon-level: a real open, leased session survives an aggressive pass
  and can still flush afterward) both confirm this end to end, not just by
  code inspection.

  **Eviction is LOUD**: one `offshoot: janitor: ro-cache: evicted
  <db>@<branch>@<checkpoint> (<bytes> bytes)` stderr line per eviction,
  `offshoot_ro_cache_evictions_total` (the T2-registered counter,
  previously always 0) incremented, and Task 4a's reserved-but-unwired
  `evicted` event type now has its emitter — `janitorTick` publishes
  `{type:"evicted", db, branch, detail:{checkpoint, bytes}}` to the T4a bus
  at the eviction call site (`eventBus.publish` is non-blocking, so this
  can never stall the janitor loop). `offshoot status` gains an ro-cache
  usage summary line (`ROCacheUsage`, `internal/ops/rocache.go`) plus an
  optional `-ro-cache-budget` flag of its own — display-only, since (like
  every other `serve` tuning flag) the budget is never persisted, so this
  at-rest CLI command has no other way to show usage against it without a
  live daemon connection.

  Each eviction removes both the `.db` cache file and its `.last-used`
  marker together. Partial-progress-plus-first-error on a mid-pass
  `os.Remove` failure, matching `Reap`/`GC`'s own convention elsewhere in
  `internal/ops`.

  Tests: `go test ./internal/ops ./internal/daemon -count=1 -race` —
  `internal/ops/rocache_test.go` (usage accounting, budget=0 never evicts,
  already-under-budget never evicts, oldest-first eviction to under
  budget, both files removed together, the writable-checkout and
  touch-beats-creation-order proofs above) and
  `internal/daemon/rocache_test.go` (the same LRU/budget-zero/writable-
  checkout scenarios driven through a real `janitorTick` pass, plus the
  metrics move and the `evicted` event fires with `db`/`branch`/
  `checkpoint`/`bytes`). No `internal/session`/`internal/capture` changes
  — janitor/ops only — so no torture run was required for this task.
  `docs/status.md`'s Resource-behavior row flips to shipped-and-tested;
  `docs/reference.md` gains `-ro-cache-budget`, the LRU-clock rationale,
  and reiterates the writable-never-evicted guarantee; the `evicted` event
  row in the events table loses its "reserved" caveat.

- **`serve -snapshot-every N`** (Milestone 4 Task 6a): plumbs the
  embeddable session library's existing `Options.SnapshotEvery` cadence
  into every session `offshoot serve` opens, the same way `-flush-every`
  already plumbs `FlushEvery` — `Server.SetSnapshotEvery`
  (`internal/daemon/server.go`), read by `opOpen` under the same single-
  writer-before-`Serve` contract. Default `16`, unchanged if the flag is
  omitted (`SetSnapshotEvery` is simply never called, so `snapshotEvery`
  stays its zero value and `session.Open` applies its own default); rejects
  a value `< 1` at the flag-parsing layer — unlike `-flush-every 0`, there
  is no "disabled" sentinel, since every flush must eventually snapshot.
  `internal/session.DefaultSnapshotEvery` is now exported so
  `cmd/offshoot`'s error message references the same number rather than a
  second hardcoded copy of it.

  Tests: `internal/daemon/snapshot_every_test.go`
  (`TestServeSnapshotEveryWiringDrivesCadence`) drives 9 flushes over the
  socket with `SetSnapshotEvery(4)` and asserts a bounded `store.Chain`
  (≤4 members) plus more than one snapshot object across the lineage
  listing, proving the cadence recurs under daemon wiring and not just at
  session start; `cmd/offshoot/main_test.go`
  (`TestServeNonPositiveSnapshotEveryIsRejected`) pins `0`/negative/
  non-integer as usage errors. `go test ./internal/ops ./internal/store
  ./internal/daemon ./cmd/offshoot -count=1 -race` clean; no
  `internal/session`/`internal/capture` changes, so no torture run was
  required. `docs/status.md`'s bounded-replay and tuning-surface rows flip
  to shipped-and-tested; `docs/reference.md` gains `-snapshot-every` and
  documents the flush-cost trade-off (fewer segments per snapshot = more
  frequent full-database uploads but cheaper/bounded reads, and vice
  versa); README's "What a flush costs" section updated to match.

- **CAS-conditional ref delete** (Milestone 4 Task 6b): `ops.Workspace.Destroy`
  now CAS-writes a `Deleting` claim (`store.Ref.Deleting`/`DeletingAt`) on
  the ref before doing anything irreversible, closing the `GetRef` ->
  lease-check -> unconditional-`DeleteRef` TOCTOU an earlier design review
  documented for `Destroy` — a lease acquired in that window could
  previously have its branch deleted out from under it. `store.AcquireLease`
  refuses outright (`store.ErrDeleting`, retryable) once it sees the claim,
  the same way it already refuses `Reap`'s `Reaping` claim. `--force`
  bypasses the protected/live-lease pre-checks only, never the claim: a
  lease that wins the underlying CAS still survives a concurrent forced
  destroy.

  Scoped per the task's own one-review-cycle timebox (PM Amendment 9) as a
  **sibling claim field, not a unification with Reap's existing `Reaping`
  claim** — `Reaping`'s CAS mechanics are torture/race-tested and
  deliberately untouched (`internal/ops/reap.go`/its test files pass
  unmodified).

  Backend split (PM Amendment 5 — no pretending S3 `DeleteObject` has
  preconditions it doesn't): a new `store.ConditionalDeleter` optional
  capability interface (`DeleteIf(key, ifMatch) error`) backs
  `Store.DeleteRefIf`. Local implements it for real (`Local.DeleteIf`, the
  same per-key lock file `PutIf` already uses) — a true compare-and-delete,
  belt-and-suspenders on top of the claim. S3 does not implement it at all
  (`DeleteObject` ignores `If-Match`/`If-None-Match`; those headers are
  GET/PUT-only there), so `DeleteRefIf` falls back to an unconditional
  delete on S3 and the claim marker is the entire safety mechanism on that
  backend — documented as such on `S3.Delete`, not silently pretended away.

  A Destroy that crashes after claiming but before deleting leaves the
  claim stranded (branch untouched, no partial delete); `ops.Workspace
  .ClearStaleDeleteClaims` self-heals it by age (30s — a delete claim has no
  TTL/deadline concept the way Reap's `ReapDeadline` gives its own claim
  one), wired into both the daemon janitor (`janitorTick`) and the CLI
  `offshoot gc`, same "report and press on" convention as a reap failure.

  Tests: `internal/ops/destroy_claim_test.go`'s
  `TestConcurrentDestroyAndAcquireLeaseHaveExactlyOneWinner` (20-iteration
  race loop mirroring M2's `TestConcurrentTouchAndReapHaveExactlyOneWinner`:
  exactly one of Destroy/AcquireLease wins, the branch is never left
  half-deleted under a live lease), `TestForceDestroyStillClaimGuards` (same
  race with `force=true`), `TestDestroySelfHealsStaleDeletingClaim`;
  `internal/store/local_test.go`'s `TestLocalDeleteIfConditionalDelete`/
  `TestStoreDeleteRefIfUsesConditionalDeleteOnLocal`; `internal/daemon
  /destroy_claim_test.go`'s `TestJanitorTickClearsStaleDeleteClaim` through
  a real janitor pass. `go test ./internal/ops ./internal/store
  ./internal/daemon ./cmd/offshoot -count=1 -race` clean; no
  `internal/session`/`internal/capture` changes, so no torture run was
  required. `docs/status.md`'s TTL/GC/janitor table gains a
  shipped-and-tested row; `docs/reference.md`'s `offshoot destroy` section
  documents the claim-guard and the backend split.

- **SDK typing polish** (Milestone 4 Task 7, pre-publish list) — behavior-
  preserving, no public signature or wire changes.

  TypeScript (`sdk/typescript/src/client.ts`): the internal `_call`
  plumbing no longer returns `Promise<any>`. A module-private `RawResponse`
  interface (plus `RawBranchInfo`/`RawCheckpointInfo`/`RawSessionInfo`/
  `RawAck`/`RawEvent`) mirrors `internal/daemon/protocol.go`'s `Response`
  and its embedded structs exactly as JSON arrives off the wire — every
  `_call` consumer (`open`/`checkout`/`fork`/`rollback`/`promote`/
  `branches`/`dbs`/`checkoutAt`/`status`/`Session.flush`, plus `events`'s
  ack/event-line parsing) now reads a real, typed field instead of `any`,
  with the exact same `??`/non-null-assertion defaulting each call site
  already had (no behavior change — verified via a before/after `dist/
  client.js` diff showing only added comments). `_call` itself is tagged
  `@internal` and stripped from the published `.d.ts` via
  `tsconfig.build.json`'s new `stripInternal: true` — it remains a real,
  callable, fully-typed method at runtime and for this package's own test
  files (which compile against source, not the stripped declaration
  output), since `test/client.test.ts` overrides it directly as a test
  double. Verified additive: a `dist/client.d.ts` before/after diff shows
  exactly one removal (the now-`@internal` `_call` line) and zero changes
  to any other exported symbol; `dist/testkit.d.ts` is byte-identical.

  Python (`sdk/python`): added the PEP 561 `offshoot/py.typed` marker,
  shipped via a new `[tool.setuptools.package-data]` entry in
  `pyproject.toml` (verified present in the dry-run wheel via `unzip -l`).
  `mypy --strict offshoot` is clean across all four modules
  (`client.py`/`langgraph.py`/`pytest_plugin.py`/`__init__.py`) — fixed
  with annotations only: missing parameter/return types, `dict`/`Popen`
  generic type arguments, a module-private `_TTL`/`_Seed` type alias each
  in `client.py`/`pytest_plugin.py`, `typing.cast()` (a no-op at runtime)
  on JSON-derived returns that were otherwise widening to `Any`, and a
  `Mapping[str, str]` narrowing for `_locate_binary`'s `env` parameter
  (`os.environ` isn't literally a `dict`). `pytest_plugin.py`'s guarded
  `import pytest` type-checks cleanly against pytest's own inline types
  (pytest ships PEP 561 support; no stub package or `type: ignore` needed).

  Tests: `make test-sdks` (Python 41/41, TypeScript 49/49) and
  `make test-pytest-plugin` (58/58) all green; `tsc --noEmit` (both
  `tsconfig.json` and `tsconfig.build.json`) clean; `mypy --strict offshoot`
  clean; dry-run wheel/sdist (`make dry-run-python-sdk`) and tarball
  (`make dry-run-ts-sdk`) both pass unchanged — no file-list updates
  needed on either SDK. No Go changes.

- **Operator docs + Kubernetes recipe + the M4 sweep** (Milestone 4 Task
  8): [docs/operations.md](docs/operations.md) is the new operator-facing
  page — a metrics reference table (all sixteen `offshoot_*` metric
  families, grep-verified against `internal/daemon/metrics.go` and against
  a real `curl /metrics` scrape of a locally built binary: sixteen `# TYPE`
  lines, sixteen table rows, one-to-one), the six-state branch-state table
  with precedence, the eight-type event schema and drop-slow-consumer
  semantics, the `-ro-cache-budget` mechanics (LRU `.last-used` clock,
  writable-never-evicted guarantee, the eviction-vs-`CheckoutAt` TOCTOU),
  the HTTP/auth threat model (single-tenant same-host-or-trusted-network,
  not multi-tenant isolation; constant-time token compare; loopback
  default with an explicit non-loopback ack + token requirement; "treat
  stderr as sensitive at startup"; `-token`'s argv-visible-via-`ps` gap and
  why `OFFSHOOT_TOKEN` is preferred on a shared host; `/debug/pprof`
  behind the same auth), and a tuning-flags table for all five `serve`
  knobs including the `-flush-every`/`-snapshot-every` cost interaction.
  Its first paragraph restates the single-node scope explicitly (PM
  Amendment 12) and links the FAQ's no-cluster stance.

  [docs/recipes/kubernetes.md](docs/recipes/kubernetes.md) is a new sidecar
  recipe: one `offshoot serve` + one agent container per Pod, a shared
  `emptyDir` for BOTH the unix socket and the checkout tree (restated why:
  a checkout is a real SQLite file the agent process opens directly —
  `OFFSHOOT_CHECKOUTS`/the socket path cannot cross a network filesystem),
  `-http 127.0.0.1` for `/metrics`/`/healthz` with `exec`-based liveness/
  readiness probes (an `httpGet` probe is dispatched against the Pod IP,
  which a loopback-only bind never answers), a Prometheus scrape-annotation
  example with the honest caveat that it needs a deliberate non-loopback
  opt-in (`-http-allow-non-loopback` + a real token) to actually be
  reachable from outside the Pod, and an explicit "no StatefulSet-for-HA
  story" scope note linking the FAQ. The manifest
  (`docs/recipes/k8s/offshoot-sidecar.yaml`) is real and apply-able, not
  pseudocode: schema-validated with both `kubectl apply --dry-run=client`
  and `--dry-run=server` against a disposable local `rancher/k3s` control
  plane (no live cluster was otherwise available), both passing.

  Sweep: README gained an operator paragraph under "Daemon mode" (metrics/
  HTTP/events/budgets, linking both new docs) and a fifth "operator
  surface" note in "Integration surface"; its stale "Resource behavior"
  claim that "there are no resource budgets yet" is corrected now that
  Task 5's `-ro-cache-budget` shipped. `docs/reference.md` cross-links
  `docs/operations.md` from its `-http ADDR` and eventing sections rather
  than duplicating the operator-focused tables. `docs/status.md` gained
  three new deliberately-deferred rows this milestone's self-review named
  but hadn't yet recorded (TLS, per-branch at-rest metrics by default,
  metrics push/remote-write), reworded the FD-budget row from "not yet
  implemented" to "deliberately deferred" with the `internal/dbfile`
  reasoning, retired two stale forward-references to "the operator-facing
  docs/operations.md page itself is Task 8" now that the page exists, and
  gained a new "Standing nag: user-gated launch items" section consolidating
  every button-press-only action (org transfer, PyPI/npm name claims,
  registry listing submissions, domain/Homebrew/container-image claims)
  that no further engineering resolves. `ROADMAP.md`'s Milestone 4 bullets
  are checked off against what actually shipped.

  Verification: every command in every changed/new doc was run verbatim,
  not transcribed from memory — the metrics table's scrape example is a
  real `go build` + `serve -http` + `curl /metrics` run; the Kubernetes
  manifest was actually dry-run-applied (see above) rather than merely
  YAML-parsed. `make test-sdks` and a full `go test ./... -count=1` both
  green (no code changed this task — docs and CHANGELOG/ROADMAP/status.md
  only).

## [0.1.2] - 2026-08-06

Milestone 3: the eval-harness release. The target persona's first hour is
now paved end to end — install, seed once, fork per test, inspect, export,
clean up — from Python (`offshoot.pytest_plugin`) or TypeScript
(`testkit`), with [docs/eval-harness.md](docs/eval-harness.md) as the
serious tutorial and framework recipes for Claude Code, the OpenAI Agents
SDK, LlamaIndex, and CrewAI in [docs/recipes/](docs/recipes/). New protocol
surface (`dbs`, metadata/timestamps, `export`, read-only historical
checkouts) lands first, the fixture plugin and TS testkit are built on top
of it, and `offshoot diff` closes the daily "what changed between these two
attempts" loop. Two Milestone-2 performance follow-ups (settling-flush
suppression; sidecar refresh on clean close) ride along because the
fixture's session-per-test pattern is exactly the workload they fix. SDK
publishing is prepared (workflows, manifests) but actual PyPI/npm
publication, and the MCP registry/LangGraph listing submissions, stay
user-gated — see [docs/status.md](docs/status.md) and
[ROADMAP.md](ROADMAP.md)'s Milestone 3 section for exactly what shipped vs.
what's a stated, pre-written deferral.

### Added

- **Docs sweep** (Milestone 3 Task 8): [docs/eval-harness.md](docs/eval-harness.md),
  the serious tutorial — install, the pytest plugin end to end (named-seed
  factory, fork-per-test, `pytest-xdist` with the measured per-worker
  number, mid-test flush checkpoints, golden assertions via `offshoot_dump`
  — never byte-compare, why), export for handoff/debug, `offshoot diff` for
  the failed-vs-passed loop, TTL hygiene, a CI recipe (this repo's own
  `ci.yml` `sdks` job as a live example), what it costs, and the TypeScript
  `testkit` section mirroring the same semantics for vitest/jest/`node:test`.
  Every pytest example in it was actually run against a real daemon while
  writing it (a scratch project, real output pasted in, not invented); the
  TypeScript `testkit` section's `node:test` example was likewise run
  against a real build of `sdk/typescript`. `sdk/python/README.md`'s
  pytest-fixture-plugin section is superseded by the tutorial as the
  primary teaching surface and now points to it, staying as the
  PyPI-landing-page-sized condensed reference.
  - `docs/recipes/claude-agent-sdk.md`: `claude mcp add` config for
    `offshoot mcp`, and a hooks pattern (fork on `SessionStart`, checkpoint
    on `Stop`, roll back on failure) — explicitly checked against the MCP
    daemon-mode reality documented in the README's MCP section and
    `docs/reference.md`: `offshoot mcp` never opens a session itself, so
    the recipe's actual content is opening the session via the CLI/SDK
    *before* the agent's first tool call, with the hooks shown as one way
    to automate that open/close, not a substitute for it. The hooks JSON
    shape itself is marked illustrative (Claude Code's own surface, not
    offshoot's, and outside what this task could pin the way the rest of
    this repo's docs are).
  - `docs/recipes/openai-agents.md`: the OpenAI Agents SDK's
    `SQLiteSession(session_id, db_path)` pointed at an offshoot checkout
    path, fork-per-attempt via the Python SDK. Actually run end to end
    against a real `pip install openai-agents` (no API key needed to prove
    the storage integration — `add_items`/`get_items` stand in for
    `Runner.run`); real output pasted in, including two forked attempts'
    histories staying independent after diverging from a shared checkpoint.
  - `docs/recipes/frameworks.md`: short, honest LlamaIndex/CrewAI notes —
    recipes, not adapters, per the plan's stance. Explicitly marked
    unverified against a live install this pass (LlamaIndex's SQL chat
    store needs the `aiosqlite` extra; CrewAI's package failed to build
    locally — `tiktoken`, one of its transitive dependencies, has no
    prebuilt wheel for this Python/platform combination) rather than
    presented with false confidence.
  - README: integration-surface refresh — pytest plugin, TypeScript
    `testkit`, `export`/`checkout_at`/`dbs` all get their own lines under
    the Python/TypeScript SDK sections; a new "Other agent frameworks"
    section points at `docs/recipes/`; the Docs line and Quickstart section
    both gain pointers to the new tutorial.
  - `docs/reference.md`: verified complete for `export`/`diff`/
    `checkout --at` (documented per-task already; no gaps found) — added
    one pointer line at the top to the tutorial for readers who want the
    narrative walkthrough instead of the flag-by-flag reference.
  - `docs/status.md`: two new deferral rows, both pre-written per PM
    Amendment — `create --from` daemon/SDK/MCP reach (needs an upload
    channel or a same-host path-trust story like `export`'s; CLI remains
    the import path) and MCP session open/close (the fixture plugin and TS
    testkit are now real lifecycle owners for their own harness workload,
    but neither is an MCP tool pair — the no-natural-owner reasoning from
    Milestone 2's amendment stands, confirmed rather than just carried
    forward).
  - `ROADMAP.md`: Milestone 3 checked off item-by-item against what
    actually shipped, with the two deferrals above plus listings
    submission and actual SDK publication called out as pre-written,
    user-gated deferral rows rather than silently-dropped scope; Milestone
    2's "MCP rides the daemon" bullet updated to note the fixture/testkit
    now exist without overclaiming they closed the MCP-tool-pair gap.
- **List databases** (Milestone 3 Task 1): new daemon protocol op `dbs`
  returns every database the store has at least one ref for
  (`store.Store.ListRefs`'s keys, sorted). CLI: `offshoot session dbs`.
  SDK: Python `Client.dbs() -> list[str]`, TypeScript
  `Client.dbs(): Promise<string[]>`.
- **Branch/checkpoint metadata and timestamps** (Milestone 3 Task 1):
  - `Ref.Meta map[string]string` (branch-level lineage metadata) and
    per-checkpoint `store.Checkpoint.CreatedAt`/`Meta` — all new, omitempty
    fields, no store schema bump (verified with a round-trip + old-ref/
    old-checkpoint decode test mirroring the existing TTL-fields test).
  - `ops.Workspace.Fork`/`Checkpoint` gain a `meta map[string]string` param
    (`nil` = none), capped at the ops layer via the new `ops.ValidateMeta`
    (at most 32 keys, keys ≤ 64 bytes, values ≤ 512 bytes — Global
    Constraints; clear errors naming the exact limit hit). `Fork`'s meta
    describes the new branch's lineage (`Ref.Meta`); `Checkpoint`'s meta
    describes that one checkpoint (`Checkpoint.Meta`). Every checkpoint-
    creating call site (`Create`'s `init`, `Checkpoint`, `Fork`'s `fork`,
    `Promote`'s `promote`, and a daemon session's named `flush`) now stamps
    `CreatedAt` (RFC3339 UTC).
  - `Rollback`'s kept-checkpoint relocation used to silently drop
    `CreatedAt`/`Meta` when rewriting a checkpoint's epoch to the new
    lineage's `1` — fixed to preserve both.
  - Daemon protocol: `BranchInfo` gains `touched_at` (the ref's activity-
    clock stamp) and `checkpoints_v2` (`[]{name, txid, created_at}`) —
    the existing `checkpoints` (bare names) field is untouched for wire
    compat. `fork` and `flush` ops gain an optional `meta` field; there is
    no separate daemon "checkpoint" op — a live session's named `flush` is
    how its checkpoints are created (parity note: `flush`'s `meta` is
    rejected if `name` is empty — there's no checkpoint for it to attach
    to). All new fields are additive/optional: an old-client-shaped request
    (no `meta`, reading only `checkpoints`) still works against the new
    daemon, pinned by wire-compat tests using hand-written JSON (not just a
    Go zero-value round trip).
  - CLI: `fork`/`checkpoint` gain repeatable `--meta k=v`.
  - SDK parity (Python + TypeScript): `fork`/`Session.flush` accept `meta`;
    `branches()`/`Client.branches` surface `touched_at`/`checkpoints_v2`
    (new `CheckpointInfo`/`Branch` fields).
  - MCP tool metadata exposure is explicitly out of scope for this task —
    `offshoot_fork`/`offshoot_checkpoint` pass `nil` meta; see
    `docs/status.md`.
- **Publish pipeline, prepared and gated** (Milestone 3 Task 7): the SDKs
  are ready to publish; actual publication needs the user to claim the
  `offshoot-db` PyPI name and `@offshoot-db` npm scope first (see
  CONTRIBUTING.md's new Release process section).
  - `.github/workflows/publish.yml`: triggered on `sdk-v*` tags or
    `workflow_dispatch`. Two jobs, PyPI (`pypa/gh-action-pypi-publish`,
    Trusted Publishing/OIDC, `id-token: write`) and npm (`npm publish
    --provenance`, registry auth via `NPM_TOKEN` today — npm's own OIDC
    Trusted Publishing is documented as the future swap). Both gated on
    the `PUBLISH_ENABLED` repository variable: off (the default) runs a
    full dry run — real sdist/wheel + `twine check` + wheel install/import
    test; real `npm pack` tarball + install/import test — everything short
    of the upload step. The same dry-run tier now runs in `ci.yml`'s
    `sdks` job on every PR (`make dry-run-sdks`), so a manifest mistake is
    caught long before a release tag exists.
  - `sdk/python/pyproject.toml` filled out for real publication: readme,
    `project.urls` (Homepage/Repository/Issues/Changelog/Documentation),
    authors, classifiers, SPDX `license = "Apache-2.0"` (no redundant
    `License ::` classifier — current PEP 639 practice), reserved
    `[pytest]` extra ahead of Milestone 3 Task 4's fixture plugin.
    `sdk/python/README.md` added (PyPI landing page).
  - `sdk/typescript/package.json` filled out: `repository`/`bugs`/
    `homepage`, `files` whitelist (`dist`, `README.md` — excludes tests/
    tsconfig from the published tarball, verified with `npm pack
    --dry-run`), `publishConfig.access: public` (required for a scoped
    package to publish non-private), `prepublishOnly` builds `dist/`
    before packing. `sdk/typescript/README.md` added.
  - **Version discipline:** both SDKs publish in lockstep from one
    `sdk-v<version>` tag (not two `sdk-py-v`/`sdk-ts-v` tags) — simplest
    scheme for two SDKs on one wire protocol and one review cadence.
    `sdk/VERSION` is the single source of truth; `pyproject.toml`'s and
    `package.json`'s literal version fields are checked against it by the
    new `scripts/check_sdk_versions.py` (`make check-sdk-versions`, and
    the first step of `make dry-run-python-sdk`/CI); `scripts/
    check_sdk_tag_version.py` checks a pushed tag against the same file.
  - `server.json` (repo root): draft MCP registry manifest built only from
    fields this repo's own MCP docs establish (name/description/version/
    repository/command/args) — not submitted; the exact registry schema
    was deliberately not assumed from outside the repo, see
    `docs/launch/mcp-registry.md` and the TODO row in `docs/status.md`.
  - `docs/launch/langgraph-listing.md`: LangGraph community-integration PR
    text (title, description, listing-table entry) for
    `offshoot.langgraph.ThreadForks` — drafted, clearly marked not
    submitted (blocked on PyPI).
- **Export + read-only historical checkouts** (Milestone 3 Task 2):
  - `ops.Workspace.Export(db, branch, checkpoint, dstPath, force)`
    materializes any checkpoint (or head, when `checkpoint == ""`) to a
    plain SQLite file at `dstPath`, anywhere on the local filesystem, with
    zero ongoing relationship to the store afterward: no `.sum` sidecar,
    no lease. Refuses to overwrite an existing `dstPath` unless `force`.
    Reuses `materializeChainAt`/`materializeAt`'s existing atomic
    temp-file-in-the-destination's-own-directory + rename, so a failed
    export (fetch error, checksum mismatch) never leaves a partial or
    truncated file, and discards the `PostApplyChecksum` that machinery
    now threads through (Task 3) since there is no sidecar to stamp with
    it.
  - CLI: `offshoot export <db>[@branch[@checkpoint]] <out.db> [--force]`
    (`ops.ParseExportTarget` parses the triple-`@` target form).
  - Daemon `export` op: `db`/`branch`/`name` (checkpoint)/`path`
    (destination)/`force`. `path` must be an ABSOLUTE path — refused
    otherwise — per the same-host/same-user unix-socket trust model
    documented in `docs/reference.md`'s new daemon-ops section. Reads the
    branch's last DURABLE state from the store, never a live session's
    checkout: an open session's unflushed writes are NOT included, proven
    directly over the wire (`internal/daemon/export_test.go`'s
    `TestOpExportMissesUnflushedSessionWrites`).
  - `ops.Workspace.CheckoutAt(db, branch, checkpoint, force) (string,
    error)` materializes a NAMED checkpoint (no head alias) into a
    dedicated read-only cache path, `<store-root>/checkouts-ro/<db>/
    <branch>@<checkpoint>.db`, `chmod 0444`, distinct from and never
    touching the writable checkout path, its sidecar, or a live capture
    engine's file descriptors on it — safe alongside an open daemon
    session on the SAME branch. A repeat call with `force=false` is a pure
    cache hit (no store access at all — a checkpoint's content is
    immutable); `force=true` re-materializes and re-reads the store.
  - CLI: `offshoot checkout <db>[@branch] --at <checkpoint> --read-only
    [--force]` (must be given together).
  - Daemon `checkout-at` op: same semantics, server-side cache path.
  - SDK parity (Python + TypeScript): `Client.export(db, branch, out_path,
    checkpoint=None, force=False)`, `Client.checkout_at(db, branch,
    checkpoint, force=False)` (TypeScript: `checkoutAt`, options-object
    style matching the rest of that client).
  - README's Resource behavior section gains the read-only-checkout-cache
    paragraph, including the explicit "safe to `rm -rf` the entire
    `checkouts-ro` directory at any time" guarantee.
- **`offshoot.pytest_plugin` fixture plugin** (Milestone 3 Task 4): the
  seed-once-fork-many paved road for pytest-based eval/test suites, shipped
  as the `offshoot-db[pytest]` package extra and registered via a
  `pytest11` entry point (`sdk/python/offshoot/pytest_plugin.py`) — nothing
  to import by hand.
  - `offshoot_daemon` (session-scoped): locates the `offshoot` binary
    (`OFFSHOOT_BIN` env, else `PATH`), starts it on a fresh temp store +
    socket, terminates it at session end; `pytest.skip`s with install
    instructions when no binary is found, rather than failing.
  - `offshoot_db` (session-scoped): a NAMED-SEED FACTORY,
    `offshoot_db(name="default", seed=None)`. The first call for a name
    creates database `eval-{name}`, runs `seed` (a callable given a
    writable sqlite path, or a SQL string run via `sqlite3`), and flushes
    it to a checkpoint named `seed`; later calls for the same name are a
    pure memoization hit. `seed=None` falls back to the new `offshoot_seed`
    ini option (a path to a `.sql` file) — the zero-code default-seed case.
  - `offshoot_fork` (function-scoped): `offshoot_fork(seed_handle=None)`
    forks a fresh, worker-safe-named branch (`t-{worker}-{testname-hash}-
    {n}`, sanitized via the existing `offshoot.langgraph._sanitize`) from
    the seed checkpoint with a TTL (default `1h`, new `offshoot_ttl` ini
    option overrides it), opens a session, and returns an object with
    `.path`/`.client`/`.db`/`.branch`. Teardown closes the session then
    destroys the branch; a destroy failure is a `UserWarning`, never a test
    failure — the TTL is the crashed-run backstop.
  - `offshoot_dump(path) -> str`: `sqlite3 .dump` text — THE way to compare
    two offshoot-materialized SQLite files in a golden-file test. SQLite's
    on-disk bytes are not deterministic for identical logical content
    (page layout, freelist order, vacuum state can all differ) — **never
    byte-compare** two exported/checked-out `.db` files.
  - **xdist stance (locked):** one `offshoot` daemon + temp store PER
    WORKER (pytest has no cross-worker fixture-sharing mechanism), so a
    named seed's cost is paid once per worker, not once total. Measured
    (a `CREATE TABLE` + 200-row seed, macOS arm64): ~80-90ms per worker;
    a 2-worker `pytest -n2` run pays that twice, concurrently (~170ms of
    total seed work, ~85-90ms of wall-clock time). Documented in the
    module's docstring and `sdk/python/README.md`.
  - Found+fixed in passing: a SQL-string seed with no explicit
    `BEGIN`/`COMMIT` used to run every statement as its own autocommit
    transaction under `sqlite3.Connection.executescript` — measured ~1.6s
    for 200 unwrapped `INSERT`s vs. ~17ms wrapped in one transaction
    (~100x). `_run_seed` now wraps a bare multi-statement seed in one
    transaction automatically (a seed that already opens its own
    transaction, or a seed callable, is left alone).
  - Base SDK stays stdlib-only: the `pytest11` entry point is registered
    unconditionally in `pyproject.toml`, but the plugin module
    import-guards `pytest` (a stray direct import without the extra fails
    with an install-instruction message, not a bare traceback) — `pytest`
    is only ever actually imported when pytest itself is already running.
    Verified: `make test-sdks` (the plain-`unittest` suites) passes with no
    pytest installed at all.
  - Tests: `sdk/python/tests/test_pytest_plugin.py` — fixture-logic tests
    (naming, TTL, teardown ordering, factory memoization, skip-when-no-
    binary, destroy-failure-warns) against a directly started daemon, plus
    3 `pytester`-driven smoke scenarios (plugin loads via its entry point,
    fork-per-test isolation actually isolates, an xdist 2-worker run
    passes — the last one is also where the xdist numbers above were
    measured). New `make test-pytest-plugin` target (needs
    `offshoot-db[pytest]` + `pytest-xdist`, wired into `ci.yml`'s `sdks`
    job after `make test-sdks` already proved the base SDK pytest-free).
- **TypeScript testkit** (Milestone 3 Task 5): `sdk/typescript/src/testkit.ts`
  — the vitest/jest counterpart of `offshoot.pytest_plugin`, exported as a
  new subpath, `@offshoot-db/client/testkit` (package.json `exports` map
  added; previously only a bare `main`/`types` pair). Framework-agnostic
  FUNCTIONS, not fixtures — no vitest/jest runtime dependency (this SDK
  stays zero-runtime-deps) and nothing is registered automatically; wire
  them into your own `beforeAll`/`afterEach`.
  - `startDaemon(opts?) -> Promise<DaemonHandle>`: locates the `offshoot`
    binary (`OFFSHOOT_BIN` env, else `PATH` — a clear error naming both
    when neither has one; there is no skip tier here, unlike the pytest
    plugin, since this module has no test-framework integration to skip
    through), starts it on a fresh temp store + socket. Returns
    `{ sock, store, proc, stderrTail(), stop() }`; the caller stops it
    themselves (typically from a top-level `afterAll`).
  - `seedOnce(daemon, {name?, seed}) -> Promise<SeedHandle>`: a NAMED-SEED
    memoization cache keyed on `(daemon, name)`. `seed` is a SQL string, a
    path to a `.sql` file (detected: no newline, ends in `.sql`, and the
    file exists — there's no pytest.ini-style config file here to hold that
    decision separately), or an async `(dbPath) => void` callback. The
    first call for a name creates database `eval-{name}`, runs the seed,
    and checkpoints it `seed`; a later call for the same name with a
    *different* seed (fingerprinted: SQL/path text by content hash,
    callables by identity) throws a clear error rather than silently
    keeping the first one — same mismatch semantics as the pytest plugin's
    `offshoot_db`. Ports `_skip_leading_noise`/`_seed_opens_own_transaction`
    verbatim: a SQL-string seed is wrapped in one transaction unless it
    already opens its own (so a `sqlite3 .dump`'s text — `PRAGMA` before
    `BEGIN TRANSACTION` — works verbatim as a seed, and so a plain
    multi-statement seed doesn't pay one autocommit-transaction-per-
    statement). Since this SDK ships no SQLite driver, seeding (and
    `dump`, below) shells out to the `sqlite3` CLI, same as
    `test/client.test.ts` already does.
  - `forkPerTest(daemon, seedHandleOrName, opts?) -> Promise<ForkedSession>`:
    forks a fresh, worker-safe-named branch
    (`t-{worker}-{sanitized-hint}-{n}`; worker id from `VITEST_POOL_ID` or
    `JEST_WORKER_ID` when present, else `"local"`) from the seed's
    checkpoint with a TTL (default `"1h"`, `opts.ttl` overrides), opens a
    session, and returns `{ path, db, branch, client, flush(name?),
    close() }`. `seedHandleOrName` may be a `SeedHandle` or a plain string
    naming an already-`seedOnce`d name. `close()` closes the session and
    destroys the branch; either failing is a `console.warn`, never a
    throw — call it from `afterEach` with no surrounding try/catch. Each
    `ForkedSession` owns its own connection and teardown independently, so
    (unlike the pytest plugin's shared-per-test `_ForkFactory.teardown()`
    loop) one fork's cleanup trouble can never block another's by
    construction, not by an explicit try/except around each step.
  - `dump(path) -> Promise<string>`: `sqlite3 <path> .dump`'s text — THE
    golden-comparison method. SQLite's on-disk bytes are not deterministic
    across writes with identical logical content — never byte-compare two
    exported/checked-out `.db` files; compare `await dump(a) === await
    dump(b)` instead.
  - Tests: `sdk/typescript/test/testkit.test.ts`, run via `node:test`
    against a real daemon (mirroring the pytest plugin's direct-daemon
    tier): binary resolution (`OFFSHOOT_BIN`/`PATH`/neither), naming,
    TTL default+override, `seedOnce` memoization + fingerprint mismatch, a
    `.dump`-shaped seed, a path-to-`.sql` seed, an async-callback seed,
    fork-per-test isolation, the string-name shorthand, teardown-warn-not-
    throw against a killed daemon, and a golden-file (`dump`, not bytes)
    scenario — plus one integration test wiring `startDaemon`/`seedOnce`/
    `forkPerTest` into `node:test`'s own `before`/`after`/`beforeEach`/
    `afterEach`, the way a real suite would. Wired into the existing
    `make test-ts-sdk` target (it already globs `test-dist/test/*.test.js`)
    — same zero-extra-dependency constraint (`typescript`/`@types/node`
    devDeps only).
  - `make dry-run-ts-sdk`'s tarball exact-match assertion updated to expect
    `dist/testkit.js`/`dist/testkit.d.ts` alongside the existing
    `dist/client.*` files, and a second install-then-`import()` check
    added for the new `@offshoot-db/client/testkit` subpath (not just the
    package root).
- **`offshoot diff`** (Milestone 3 Task 6): `offshoot diff
  <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary]`
  materializes both sides READ-ONLY (never a live checkout, never a lease)
  and either streams `sqldiff`'s output over them or prints a
  `sqldiff`-free table-level row-count summary. CLI-only for this task — no
  daemon op, no SDK parity.
  - `ops.Workspace.MaterializeForDiff(db, branch, checkpoint)
    (DiffSide, error)`: a named checkpoint reuses the existing `CheckoutAt`
    read-only cache (a checkpoint's content is immutable, so this is a
    legitimate cache hit across repeated diffs); a bare head target is
    always freshly `Export`ed to a private temp file, because head moves
    and a head-keyed entry in the checkout-at cache could never be
    idempotently cached the way a checkpoint-keyed one is — this is the
    task's staleness decision, documented on `MaterializeForDiff`'s doc
    comment and pinned by `internal/ops/diff_test.go`'s
    `TestMaterializeForDiffHeadSideAlwaysReflectsANewWrite` (a write
    between two calls against the same head target is visible in the
    second one, never served stale) and the CLI-level
    `TestDiffCLIHeadSideReflectsNewWriteNotStaleCache`. `DiffSide.Close()`
    is a no-op for the cache-backed checkpoint case and removes the temp
    file/directory for the head case.
  - `ops.TableRowCounts(path) (map[string]int, error)` /
    `ops.DiffSummary(leftPath, rightPath) ([]TableDiff, error)`: the
    `--summary` engine, stdlib-`database/sql` + the `mattn/go-sqlite3`
    driver already vendored for `internal/capture`/`internal/ops` (no new
    Go module dependency), opened as
    `file:<abs>?mode=ro&immutable=1` — verified against a real `chmod
    0444` file (`TestTableRowCountsOpensReadOnlyEvenOnA0444File`) that
    SQLite accepts a `mode=ro` URI parameter layered on top of the
    driver's always-requested `SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE`
    flags (SQLite only rejects a mode LESS restrictive than the flags
    argument passed to `sqlite3_open_v2`; `ro` is strictly more
    restrictive). Lists tables from `sqlite_master` (excluding
    `sqlite_%` internals) and counts rows per table on both sides; a
    table present on only one side is reported `added`/`removed` wholesale
    rather than as a row-count delta. The two materialized paths may be
    two entirely different databases — cross-db diff is a legitimate eval-
    comparison shape and is explicitly tested
    (`TestDiffSummaryCLIWorksAcrossTwoDifferentDatabases`).
  - CLI `--summary` output: an aligned `text/tabwriter` table (TABLE/LEFT/
    RIGHT/STATUS columns, `status` one of `same`/`added`/`removed`/
    `changed (+N)`/`changed (-N)`) plus a trailing totals line.
  - Default mode requires the separate `sqldiff` binary (NOT included by
    installing plain `sqlite3` on every platform) and fails with a clear,
    per-OS-hinted error naming the install path when it's missing, plus
    `--summary` as the `sqldiff`-free alternative — found and fixed a
    wrong hint mid-task: a real local Homebrew install of the general
    `sqlite` formula (keg-only, 13 files) confirmed it does NOT include
    `sqldiff`; Homebrew ships `sqldiff` as its OWN separate formula
    (`brew install sqldiff`), corrected in both the error text and
    `docs/diff.md`. The Debian/Ubuntu hint (`sqlite3-tools`) is verified
    against a real `ubuntu:24.04` container's `apt-cache show
    sqlite3-tools` output (lists `/usr/bin/sqldiff`), and
    `.github/workflows/ci.yml`'s Linux job now installs that package
    alongside plain `sqlite3` so `offshoot diff`'s real-`sqldiff` CLI
    tests (`TestDiffCLISqldiffPresentStreamsOutput`,
    `TestDiffCLIIdenticalSidesProduceNoSqldiffOutput`) exercise the actual
    binary in CI instead of permanently skipping — they gate on
    `exec.LookPath("sqldiff")` the same way `requireSQLite3` gates on
    `sqlite3` itself, so a machine without it still skips cleanly.
  - `docs/diff.md`: the command, the raw by-hand `export`-twice-then-
    `sqldiff` recipe, the staleness rule, and a link to the FAQ's
    [no-merge stance](docs/faq.md#why-no-merge) — `offshoot diff` is a read
    tool; it never resolves anything.

### Fixed

- Settling-flush checksum-compare suppression (Milestone 2 follow-up):
  `rebaseline`'s first call now skips a session's mandatory startup settling
  flush when BOTH the checkout `Open` received was already proven
  byte-identical to the branch's head at the moment `Open` checked AND the
  LTX checksum recorded in that same checkout's `.sum` sidecar exactly
  matches what the checkout actually contains once the session's real
  startup rebase finishes running — the second check catches a write
  landing in the window between `Open` returning and its own startup
  rebase actually finishing, which the first check alone cannot see.
  Reading the checksum out of the local sidecar, rather than fetching it
  fresh from the store, costs `Open` no extra store call at all — critically,
  no download of the head object itself, which a full snapshot can be
  (post-Create, post-Fork, every 16th flush, and permanently for a
  read-only branch that never flushes, exactly defeating this
  optimization's own purpose). `ops.Checkout`/`Checkpoint`/`Rollback`/
  `Promote` all now record this checksum when they stamp a sidecar
  (`sumRecord.PostApplyChecksum`, `ltxio.EncodeSnapshot`/`MaterializeChain`
  now return it as a byproduct of work they already do); a sidecar that
  never recorded one settles exactly as before ("fail toward settling"). A
  read-only daemon session reopened against an unmodified checkout no
  longer uploads a full snapshot (previously measured 541.9MB at a 512MB
  db) for doing nothing. A first-ever open, or one against a dirty/stale
  checkout, still settles exactly as before.
- Sidecar refresh on clean Close (Milestone 2 follow-up): `Session.Close`
  now re-stamps the checkout's `.sum` sidecar — including its LTX checksum
  (the row above is what reads it back on the next `Open`) — when the
  close is provably clean (no session error, nothing left unflushed, at
  least one flush succeeded, the branch head hasn't moved past what was
  flushed, the replica was never rebuilt by anything beyond its own
  startup rebase, and — the stamped hash itself — the capture engine's own
  post-shutdown fingerprint, persisted only once its shutdown fully
  verified the checkout's WAL was cleanly folded in, is reused directly
  rather than independently re-derived), so the next `Open`/`Checkout`
  against the same db@branch clean-skips instead of re-materializing —
  restoring, for the daemon-reopen pattern, the disk/descriptor win
  Milestone 2 Task 1 already established for `Checkout`/`Checkpoint`/
  `Rollback`/`Promote`. Ledgered as a documented tradeoff, not a
  regression: a clean-and-current checkout is now served without chain
  validation across a session's clean close too, not only across those
  four `ops` entry points (see docs/status.md).

## [0.1.1] - 2026-08-06

Milestone 2: safe defaults for an unattended agent. The daemon now ships
work on its own cadence instead of relying on an explicit `flush`, forks and
`session.Open` are fast on the common single-snapshot-chain path (measured,
before and after, in `docs/benchmarks.md`), MCP rides an already-open daemon
session for live capture and now TTLs its own forks by default, `status`
answers "which branch is behind and why," and `docs/status.md` /
`docs/reference.md` / this file are the honest accounting of what shipped,
what's merely "shipped" (unverified in some dimension), and what was
deliberately deferred with a stated reason rather than just left undone.
Release tag lands after this branch merges.

### Added

- Fast-path fork (Task 6a, local half): `ops.Workspace.Fork`/`Rollback`/
  `Promote` now copy the source checkpoint's snapshot object directly,
  instead of materializing it to a temp file and re-encoding a fresh
  snapshot, whenever the checkpoint's chain (`store.Chain`) resolves to
  exactly one member and that member is a snapshot — the common case for an
  at-rest fork. New `internal/ops/reflink` package backs this on the local
  backend: a filesystem clone (`clonefile(2)` on darwin, the `FICLONE`
  ioctl on Linux via the new `golang.org/x/sys` dependency) when the
  filesystem supports it, silently falling back to a plain byte copy
  otherwise. `store.Backend` gains `CopyObject(dst, src string) error`; the
  local backend implements it (temp+rename for atomicity, matching its
  existing write pattern), and S3 returns the new `store.ErrCopyUnsupported`
  sentinel unconditionally for now — S3 server-side `CopyObject` (gated to
  objects ≤5GB) is a separate follow-up (Task 6b), so `Fork` against S3
  takes exactly its pre-existing slow path, unchanged. Falls back to the
  slow path automatically whenever the precondition doesn't hold (a branch
  that has been flushed through a daemon session's segment cadence past its
  last snapshot) or a backend can't perform the copy — never a hard
  failure. Measured 512MB fork 2.87s → 198ms locally on APFS (~14.5x;
  ~9.3ms for the copy itself, isolated from a separate pre-existing O(size)
  check — see docs/benchmarks.md), and a real, if smaller, win even without
  clone support (a Linux container without `FICLONE`: 3.02s → 513ms).
  `internal/store/storetest`'s shared conformance suite gained a
  `CopyObject` round-trip + `ErrNotFound` subtest that skips (rather than
  fails) against a backend returning the unsupported sentinel, so it
  already covers local, the in-process S3 fake, and the MinIO-gated
  real-provider suite.

- Fast-path fork (Task 6b, S3 half): `store.S3.CopyObject` now issues a
  real server-side `CopyObject` API call (a `PUT` carrying
  `X-Amz-Copy-Source`, no request body) instead of returning the
  `ErrCopyUnsupported` sentinel unconditionally — `ops.Fork`'s fast path
  (Task 6a) now fires against an S3 backend too, for any single-snapshot
  checkpoint at or under S3's 5GB single-request `CopyObject` limit.
  `CopyObject` HEADs the source first (one request, not a download) and
  returns `ErrCopyUnsupported` for anything over that limit — the same
  fallback signal Task 6a already wired `Fork` to treat as "use the slow,
  materialize-and-re-encode path" — rather than implementing the
  multipart `UploadPartCopy` API this backend doesn't support. The shared
  `storetest.RunConformance` `CopyObject` subtest (added in Task 6a) now
  runs for real against S3 instead of skipping on the sentinel, covering
  both the in-process fake and the MinIO-gated real-provider suite
  (`make test-s3`); `storetest.FakeS3` gained `CopyObject`-request
  handling (it previously had no concept of the header-only, bodyless
  copy request and would have silently overwritten the destination with
  an empty body) and a `SetSizeOverride` test hook so the 5GB gate can be
  exercised without allocating a real multi-gigabyte object. Measured
  (MinIO-local, not an AWS claim; `make bench-s3`) 512MB fork over S3
  4.57s → 1.03s (~4.4x) and 64MB 536.9ms → 153.0ms (~3.5x) — see
  docs/benchmarks.md.

- Benchmarks: `internal/ops/fork_bench_test.go` measures `Fork`'s current
  slow path (`copySnapshotToNewLineage` materializes the source checkpoint
  and re-encodes a fresh snapshot for the child lineage — O(size)),
  `Checkout`'s clean-skip fast path (still O(size): it still quiesces and
  SHA-256-hashes the whole file, just skips the rebuild), and
  `session.Open`'s latency, all against a shared 64MB/512MB size table (plus
  a `size=4GB` case, skipped by default under `-short`) so before/after
  numbers read off identical subtest names once Task 6's fast path lands.
  `BenchmarkSessionOpen` also measures, once per size, the stored size of a
  session's settling flush — the full snapshot every daemon session uploads
  on its first auto-flush tick, even read-only — confirming that cost is
  O(size) rather than fixed. `make bench` runs the default sweep locally;
  `make bench-s3` runs the same benchmarks against a real MinIO container.
  Measured baselines, method, and machine description (host + a Linux
  container) are recorded in `docs/benchmarks.md`.

- MCP rides a running daemon: `offshoot_checkpoint`, `offshoot_fork`, and
  `offshoot_checkout` each probe the daemon fresh on every call (never
  cached — the daemon may start after the MCP server does) and take a
  live-capture path when one is up, falling back silently to exactly
  today's at-rest behavior otherwise. `offshoot_checkpoint` on a branch
  with an open daemon session flushes it live (the daemon's `flush` op —
  no quiesce, no full-snapshot re-encode); `offshoot_fork` routes through
  the daemon's `fork` op whenever a daemon is up, which flushes an open
  source session first so an unflushed write always lands in the new
  branch; `offshoot_checkout` on a branch with an open session returns
  that session's own live checkout path instead of materializing a
  separate at-rest copy — but not a FENCED session's path: a session that
  lost its lease (e.g. to another writer) stays listed until closed, so
  `offshoot_checkout` checks its health and falls back to at-rest with a
  warning naming the session's error rather than handing over a path
  nothing is capturing anymore. No MCP tool opens a session itself — the
  good path requires a harness (the SDKs, `offshoot session open`, or your
  own loop) to have opened one already; without that, MCP checkpoints stay
  at-rest even with a daemon running. `offshoot mcp` gains `-socket PATH`
  (default: the same derivation `offshoot serve` uses for the store), so
  an MCP server and a daemon started against the same store agree on
  where to look without either hardcoding the path. `offshoot_rollback`,
  `offshoot_promote` (on its target), and `offshoot_destroy` now refuse
  (rather than silently fencing a live daemon session) when the daemon has
  any session — healthy or fenced — open on the affected branch: those
  ops repoint or delete a branch's ref outright at rest, bypassing the
  daemon entirely, so without the refusal they could clear a lease or
  repoint storage out from under a session the daemon still believes it
  owns.

- MCP forks expire by default: `offshoot_fork` gains a `ttl` argument (a Go
  duration string, or `"none"` for a branch that never expires) and falls
  back to `offshoot mcp -default-ttl` (default `24h`) when `ttl` is
  omitted — an explicit `ttl` on the call always wins, and `ttl:"none"`
  always yields no TTL even under a configured default. `-default-ttl 0` or
  `-default-ttl none` disables the default entirely. The fork tool's
  response now echoes the TTL it applied and, when there is one, the
  computed expiry timestamp, so both land in the agent's own transcript.
  Reaping still requires a running janitor (`offshoot serve`) — a
  daemonless `offshoot mcp` setup only sweeps expired branches when
  `offshoot gc` is run by hand, and both the tool's Description and the
  README now say so.

- Background flush: `session.Options.FlushEvery`, when > 0, flushes a
  session automatically on that cadence so an agent that never calls `Flush`
  still gets bounded data loss on crash (library default stays `0`, manual
  only). `Session.LastFlush()`/`LastFlushErr()` expose the most recent
  successful flush and the most recent automatic-flush failure — a failure
  is recorded and retried next tick (never one already superseded by a
  fresher success), never kills the session, and a session-closed race never
  surfaces as a spurious flush error. `offshoot serve` gains `-flush-every`
  (default `30s`, `0` disables, negative rejected as a usage error) and
  applies it to every session it opens — the daemon ships work on a cadence
  by default, the safe default lives at that boundary rather than in the
  library primitive. An idle tick (nothing committed via a captured
  transaction, AND no rebase, since the last successful flush) does nothing
  at all — no object write, no ref write — rather than uploading a pointless
  full snapshot every `SnapshotEvery` ticks forever; the rebase half of that
  check matters because a rebase's checkpoint can fold a real commit into
  the baseline without it ever passing through ordinary WAL capture, so
  every session performs one settling flush shortly after its startup
  rebase lands, even if the agent never writes anything.

- Observability (Milestone 2, Task 7): `status` now answers "which branch is
  behind and why." `capture.Engine` gains `Lag()`, WAL bytes a writer has
  committed but this engine's replica has not yet applied — tracked via a new
  atomic offset field (the same lock-free pattern `Rebased`/`Resumed` already
  use, since the engine has no general mutex covering its reader state) so it
  is safe to call from any goroutine concurrently with `Run`. `SessionInfo`
  (the daemon's "status" op) gains `durable_age` (time since the last
  successful flush, empty if never), `last_flush_at` (RFC3339), `flush_error`
  (the most recent automatic-flush failure, mirroring `LastFlushErr`'s
  clear-on-success semantics exactly — `ErrClosed` is never recorded, same as
  the library-level field it surfaces), and `capture_lag_bytes` (always
  present, 0 is meaningful). `offshoot session status`'s output line gains
  `lag=`, and — when set — `last_flush=`, `age=`, and `flush_error=`.
  Every session state transition (opened; flushed, tagged `kind=manual` or
  `kind=auto`, with its txid; flush-failed, with the error — `ErrClosed`
  races excluded, since a `Close`-in-progress isn't an operational failure;
  fenced, with the terminal cause; closed) now writes one structured
  `offshoot: session: db@branch: event key=value ...` line to stderr,
  matching the daemon janitor's existing `offshoot: janitor: ...` prefix
  family rather than a second format.

### Changed

- `Checkout` no longer re-materializes a checkout that is already clean at
  the branch's current head (sidecar fingerprint matches the file, and the
  fingerprint's recorded lineage/epoch/txid matches the ref right now); it
  returns the existing file as-is. Every `session.Open` calls `Checkout`, so
  this avoids an O(size) temp-file-and-rename plus a stranded `dbfile`
  descriptor on every open when the checkout hasn't drifted. Modified and stale
  checkouts are unaffected: they still warn and re-materialize exactly as
  before.

## [0.1.0] - 2026-08-05

Initial pre-release. offshoot brings git-like branching to SQLite databases:
create, fork, checkpoint, rollback, and promote — stock SQLite files, your
choice of storage.

### Added

- Core branch operations: `init`, `create` (optionally `--from` an existing
  file), `checkout`, `checkpoint`, `fork` (optionally from a checkpoint, with
  an optional TTL), `rollback --to`, `promote --onto`, `destroy`, `path`,
  `status`, and `gc` for unreachable-lineage collection.
- Two storage backends behind a common `Backend` interface: a local directory
  store and an S3-compatible store (AWS S3, Cloudflare R2, Tigris, MinIO),
  selected via a store spec (`./path`, `file:///abs/path`, `s3://bucket/prefix`).
- Compare-and-swap safety: every branch ref update is a conditional write.
  `OpenBackend` runs a CAS probe against the store at attach time and refuses
  to operate if conditional writes are not enforced, rather than silently
  degrading. Google Cloud Storage's S3-interop API is unsupported for this
  reason; MinIO is verified via the real-provider conformance suite, AWS S3
  and R2/Tigris are expected to pass but not yet run against for real.
  See `internal/store/s3_integration_test.go` / `make test-s3`.
- Leases with epoch fencing: a long-running writer claims a branch with a
  TTL'd lease; acquiring or reclaiming bumps the branch's epoch so a stale
  writer that resumes after losing its lease cannot corrupt the branch — its
  writes land in a superseded, unreferenced prefix instead.
- Daemon mode (`offshoot serve`): holds branch leases and captures every
  committed transaction while a session stays open, removing the
  quiesce-to-checkpoint constraint of at-rest mode. `offshoot session
  open/flush/status/close/shutdown` drive it from the CLI; flushes are
  incremental (only changed pages) with a full snapshot every 16th flush, so
  materializing a branch replays at most one snapshot plus fifteen segments
  (bounded replay).
- TTL reaping: branches can carry a TTL (set at fork time or via `offshoot
  touch`), measured from the last durable write or lease renewal. The
  daemon's janitor (`-reap-every`, `-gc-grace`) reaps expired, lease-free
  branches and periodically GCs tombstoned lineages. Protected branches
  (`main` by default) are never reaped.
- MCP server (`offshoot mcp`): exposes list, checkout, checkpoint, fork,
  rollback, promote, and destroy as MCP tools over stdio, so an agent can
  branch on its own initiative — with the same protected-branch guardrails
  as the CLI.
- Python and TypeScript SDKs (`sdk/python`, `sdk/typescript`): thin,
  dependency-light clients over the daemon's lifecycle API. Not yet
  published to PyPI/npm; import from a checkout of this repo. Exercised
  against a real daemon by `make test-sdks`.
- `offshoot.langgraph.ThreadForks`: a LangGraph checkpointer companion that
  maps each thread to its own offshoot branch, so rewinding and retrying a
  thread forks the database too. See `examples/langgraph-rewind/`.
- `offshoot version` prints the release version plus the Go runtime version
  and GOOS/GOARCH.
