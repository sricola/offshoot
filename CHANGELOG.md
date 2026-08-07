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

### Fixed

- Settling-flush checksum-compare suppression (Milestone 2 follow-up):
  `rebaseline`'s first call now skips a session's mandatory startup settling
  flush when the checkout `Open` received was already proven byte-identical
  to the branch's current head — a read-only daemon session reopened against
  an unmodified checkout no longer uploads a full snapshot (previously
  measured 541.9MB at a 512MB db) for doing nothing. A first-ever open, or
  one against a dirty/stale checkout, still settles exactly as before.
- Sidecar refresh on clean Close (Milestone 2 follow-up): `Session.Close`
  now re-stamps the checkout's `.sum` sidecar when the close is provably
  clean (no session error, nothing left unflushed, at least one flush
  succeeded, the branch head hasn't moved past what was flushed, and the
  replica was never rebuilt by anything beyond its own startup rebase), so
  the next `Open`/`Checkout` against the same db@branch clean-skips instead
  of re-materializing — restoring, for the daemon-reopen pattern, the
  disk/descriptor win Milestone 2 Task 1 already established for
  `Checkout`/`Checkpoint`/`Rollback`/`Promote`.

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
