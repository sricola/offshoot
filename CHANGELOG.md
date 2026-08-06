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

### Changed

- `Checkout` no longer re-materializes a checkout that is already clean at
  the branch's current head (sidecar fingerprint matches the file, and the
  fingerprint's recorded lineage/txid matches the ref right now); it returns
  the existing file as-is. Every `session.Open` calls `Checkout`, so this
  avoids an O(size) temp-file-and-rename plus a stranded `dbfile` descriptor
  on every open when the checkout hasn't drifted. Modified and stale
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
