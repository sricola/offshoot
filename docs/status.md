# Implementation status

offshoot's [design spec](superpowers/specs/2026-07-29-offshoot-design.md)
describes a v1 scope larger than what's shipped so far. This page is the
honest accounting: what's tested in anger, what's built but unverified in
some dimension, and what's still just a plan. If a guarantee you're relying
on isn't marked **shipped-and-tested** below, verify it yourself before
depending on it in production.

Status legend:

- **shipped-and-tested** — implemented, and exercised by an automated test
  (unit, integration, property, or torture) that would fail if it broke.
- **shipped** — implemented and in the code path you'll actually hit, but
  without the same weight of testing (e.g. verified against one provider,
  not the class of providers it claims to support).
- **not yet implemented** — spec'd or planned, no code behind it yet. Each
  row links to the roadmap milestone tracking it.
- **deliberately deferred** — considered for this milestone and explicitly
  declined, with a stated reason, rather than simply not yet started; see
  the row's Notes for why.

## Core model

| Guarantee / feature | Status | Notes |
|---|---|---|
| Branch = named ref (lineage, epoch, head txid, checkpoints, TTL, protected flag) | shipped-and-tested | `internal/ops/ops.go`, `internal/store` |
| One writer per lineage, enforced by lease + epoch fencing | shipped-and-tested | `internal/ops/fencing_test.go`; every object write lands under the epoch current at write time |
| CAS on every ref mutation | shipped-and-tested | Local backend: `O_CREAT\|O_EXCL` lock file. S3-compatible: conditional `PutIf`, gated by an attach-time capability probe |
| Materialized fork (child gets an independent lineage) | shipped-and-tested | Both backends: `CopyObject` fast path for single-snapshot chains at or under a per-backend size (local: reflink, no real limit; S3: server-side copy, ≤5GB), falling back to materialize-and-re-encode otherwise — see Fork performance below |
| Checkpoint (named state within a branch, not inherited by children) | shipped-and-tested | |
| Rollback (repoint at new lineage seeded from a checkpoint) | shipped-and-tested | Kept checkpoints' snapshots are copied forward into the new lineage |
| Promote (repoint target at source's head via fork machinery) | shipped-and-tested | Protected targets require `--force` |
| Two-phase GC (tombstone → grace → sweep) | shipped-and-tested | `internal/ops/gc_test.go`, `gc_chain_test.go` |
| TTL reaping (from last durable write or lease renewal, whichever is later) | shipped-and-tested | `internal/ops/reap_test.go`, `reap_cas_test.go` |
| Layout versioning (`offshoot.json`, refuse-newer-than-known) | shipped-and-tested | |

## Storage backends

| Guarantee / feature | Status | Notes |
|---|---|---|
| Local directory backend | shipped-and-tested | Quickstart's default; no bucket required |
| MinIO | shipped-and-tested | `minio/minio:latest`, `make test-s3` passes the conformance suite for real (see README's provider table) |
| AWS S3 | shipped | Same S3-compatible code path as MinIO; not run against a real AWS account |
| Cloudflare R2 | shipped | Same code path; not yet run |
| Tigris | shipped | Same code path; not yet run |
| Google Cloud Storage | **not supported** | GCS's S3-interop API has no conditional writes; the attach probe refuses it outright rather than degrading |

## Daemon and durability

| Guarantee / feature | Status | Notes |
|---|---|---|
| Live WAL capture (continuous, foreign-connection-safe) | shipped-and-tested | `internal/capture/torture_test.go` — kill `-9` mid-write, repeated, verifies checksum + state equivalence after every bounce |
| Incremental LTX segments (flush ships only changed pages) | shipped-and-tested | |
| Bounded replay (snapshot every 16th flush; a read applies ≤1 snapshot + 15 segments) | shipped-and-tested | Cadence is `Options.SnapshotEvery` in the embeddable session library, not currently exposed by the daemon — see "Tuning surface" below |
| Explicit flush durability (`session status` reports durable-through txid) | shipped-and-tested | Durability advances only on `flush`/`checkpoint`; nothing ships to the store automatically between them |
| **Background flush interval** (`serve -flush-every`, on by default) | shipped-and-tested | `internal/session/flush_test.go` (`TestAutoFlushShipsWritesWithoutManualFlush`, `TestIdleAutoFlushWritesNothing`, `TestAutoFlushFailureSurfacesAndRecovers`), `internal/daemon/lifecycle_test.go` (`TestServeFlushesAutomaticallyWithoutAnExplicitFlushOp`). Default `30s`, `0` disables; bounds data loss on daemon crash to at most one `-flush-every` interval — see README's [What a flush costs](../README.md#what-a-flush-costs). One daemon-wide cadence applied to every session it opens — see "Per-session `FlushEvery` override" below for what this is not |
| Settling-flush checksum-compare suppression (skip the mandatory first-open full snapshot when content is provably unchanged since the last one) | shipped-and-tested | `rebaseline`'s very first call (`internal/session/session.go`) skips the settling flush when `ops.CheckoutProven` already proved the checkout `Open` received is byte-identical to the branch's CURRENT head (the `.sum` sidecar clean-and-current fast path) — no extra store read added, the proof reuses `Checkout`'s existing `GetRef`. A first-ever open, or one against a dirty/stale checkout, still settles exactly as before. `internal/session/flush_test.go`: `TestReadOnlySessionWithCleanCheckoutMakesNoStoreWrites` (zero backend writes across many ticks, including the settle window), `TestSettleStillHappensWhenCheckoutWasStaleAtOpen` (a stale-at-open checkout still settles); M2's `TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle` and `TestFlushLoopRetriesAfterRebaseDuringUpload` stay green unmodified — a mid-session rebase-on-divergence is untouched by this change |
| Sidecar refresh on clean Close (rewrite the checkout's `.sum` sidecar to the branch's current head txid when a session closes cleanly, not just on `Checkout`/`Checkpoint`/`Rollback`/`Promote`) | shipped-and-tested | `Session.Close` now re-stamps the sidecar (`ops.RefreshSum`) when the close is provably clean: no session error, nothing left unflushed (`autoFlushPending()`), at least one flush actually succeeded, the branch head hasn't moved past what this session flushed, and the replica was never rebuilt by anything beyond its own mandatory startup rebase (`Session.singleStartupRebase` — a mid-session rebase-on-divergence's snapshot isn't provably a mirror of the live checkout, so that case is left stale on purpose). Split around the capture engine's own shutdown join: the pending decision needs the engine alive (a final `DrainNow`), the physical stamp needs the checkout's WAL already checkpointed and the engine's own connection already closed, to avoid a same-process SQLite lock-drop hazard (see `internal/dbfile`'s package comment). `internal/session/flush_test.go`: `TestCloseRefreshesSidecarSoReopenCleanSkips` (open→write→flush→close→reopen clean-skips, same inode), `TestCloseAfterFailedFlushDoesNotStampSidecar`, `TestSidecarNotStampedAfterMidSessionRebase` |
| Per-session `FlushEvery` override (a distinct cadence per `session open` call, over the wire) | **deliberately deferred (YAGNI)** | `-flush-every` is one setting for the whole daemon (`internal/daemon/server.go`'s `SetFlushEvery`), applied to every session it opens; the daemon protocol's `open` op has no field for a caller to request a different cadence for just one session. Not scoped in [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) or planned elsewhere yet — revisit if a real caller needs mixed cadences on one daemon |
| Connection contract enforcement (WAL-mode probe, rollback-journal detection) | shipped-and-tested | Violation hard-fails the branch rather than diverging silently |
| Reflink/clonefile fork, server-side S3 copy fork (design spec's `~40ms` figure was the target this work aimed at, not a number reproduced below — see notes) | shipped-and-tested (single-snapshot-chain window; S3 additionally gated to ≤5GB) | Tasks 6a+6b of [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) — `ops.Workspace.Fork`'s `copySnapshotToNewLineage` copies the source snapshot object directly (local: `internal/ops/reflink`, `clonefile(2)` on darwin / `FICLONE` on Linux, silent plain-copy fallback otherwise; S3: `store.S3.CopyObject`, a real server-side `CopyObject` API call, no download or re-upload through this process) instead of materializing and re-encoding, when the source checkpoint's chain resolves to exactly one snapshot. Falls back to the pre-6a path once a daemon session has flushed segments past the last snapshot, or — S3 only — when the source object exceeds S3's 5GB single-request `CopyObject` limit (`store.ErrCopyUnsupported`; multipart `UploadPartCopy` is not implemented, out of scope). Measured (MinIO-local, not an AWS claim) 512MB fork ~198ms on APFS (was 2.87s pre-6a) and ~1.03s over S3 (was 4.57s pre-6b); ~9.3ms for the local copy alone in isolation from an unrelated pre-existing O(size) check, the closest this suite gets to the design spec's `~40ms` figure directly — see docs/benchmarks.md |
| Async fork-point upload (`pending` marker + fork pin on GC) | **deliberately deferred** | Scoped in [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents)'s original fork-performance bullet but not built this pass: today's fork-point snapshot upload is synchronous, inside the fork call (made cheap in the common case by the reflink/`CopyObject` fast path above). Deferred deliberately: making the upload async introduces a new `pending` chain state that readers, GC, and fencing would all have to understand mid-upload, and that crash-recovery surface deserves its own design pass rather than folding into this milestone |

## TTL, GC, and janitor

| Guarantee / feature | Status | Notes |
|---|---|---|
| `offshoot serve -reap-every` / `-gc-grace` (janitor loop) | shipped-and-tested | `-reap-every 0` disables the janitor; `offshoot gc` remains available on demand |
| Protected branches never reaped | shipped-and-tested | |
| Live lease always defers reaping | shipped-and-tested | |

## Integration surface

| Guarantee / feature | Status | Notes |
|---|---|---|
| CLI (create/checkout/checkpoint/fork/rollback/promote/destroy/touch/gc/status/lease) | shipped-and-tested | See [docs/reference.md](reference.md) for every command |
| MCP server, 7 tools, at rest | shipped-and-tested | `offshoot mcp`; protected-branch rules enforced same as CLI |
| MCP forks carry a TTL | shipped-and-tested | `offshoot_fork` takes `ttl` (explicit always wins) and falls back to `offshoot mcp -default-ttl` (default `24h`; `0`/`none` disables) when omitted; `ttl:"none"` overrides even a configured default. The response echoes the applied TTL and computed expiry. Reaping still requires a running janitor (`offshoot serve`) — a daemonless MCP setup sweeps expired branches only on `offshoot gc` |
| MCP rides the daemon (live capture, session-aware checkpoint) | shipped-and-tested | `internal/mcp/daemon_test.go`; `offshoot_checkpoint`/`offshoot_fork`/`offshoot_checkout` each probe the daemon per call and take its live path when a session is already open (and healthy) on the branch (opened by a harness — no MCP tool opens one itself, i.e. the good path requires a harness-opened session); a fenced session falls back to at-rest with a warning naming its error; no session, or no daemon, and the tool runs exactly at rest as before. `offshoot_rollback`/`offshoot_promote` (target)/`offshoot_destroy` refuse — even with `force`, which has no effect on this refusal — rather than proceed at rest when the daemon has any session (healthy or fenced) on the affected branch; `offshoot_promote`'s `source` is not guarded the same way (`TestPromoteFromOpenSourceProceedsAtRest`) — an open session there doesn't block the promote, but the promoted state is the source's last-flushed head, not any unflushed write in that session. `offshoot mcp -socket PATH` names the daemon to ride |
| Python SDK (stdlib-only, over the unix-socket lifecycle API) | shipped-and-tested | `sdk/python`; not yet published to PyPI ([Milestone 3](../ROADMAP.md#milestone-3--the-eval-harness-release)) |
| TypeScript SDK (zero runtime deps) | shipped-and-tested | `sdk/typescript`; not yet published to npm ([Milestone 3](../ROADMAP.md#milestone-3--the-eval-harness-release)) |
| LangGraph `ThreadForks` companion | shipped-and-tested | `examples/langgraph-rewind/` runs it end to end |
| Read-only checkouts / sessions | **not yet implemented** | [Milestone 3](../ROADMAP.md#milestone-3--the-eval-harness-release) — one writable checkout per branch per daemon is enforced; there is no sanctioned read-only mode alongside it yet |

## Observability and security

| Guarantee / feature | Status | Notes |
|---|---|---|
| `offshoot status` (branch state, checkpoints, TTL remaining) | shipped-and-tested | |
| `session status` (durable-through txid, epoch, holder, errors) | shipped-and-tested | |
| Structured branch-state-transition logging | shipped-and-tested | `internal/session/session_test.go` (`TestSessionTransitionLogsOpenedFlushedClosed`), `internal/session/flush_test.go` (`TestAutoFlushTransitionLogsRecordKindAutoAndFailure`) — every session state transition (opened; flushed, tagged `kind=manual`/`kind=auto` with its txid; flush-failed with the error, `ErrClosed` races excluded; fenced with the terminal cause; closed) writes one `offshoot: session: db@branch: event key=value ...` line to stderr, matching the daemon janitor's existing `offshoot: janitor: ...` line family |
| Prometheus `/metrics` (capture lag, durable-through age, GC backlog, cache usage, latencies) | **not yet implemented** | [Milestone 4](../ROADMAP.md#milestone-4--operable-at-scale) |
| HTTP binding + single-token auth | **not yet implemented** | [Milestone 4](../ROADMAP.md#milestone-4--operable-at-scale) — the daemon speaks only a local unix socket today; there is no network listener and no auth token anywhere in the code |
| Branch state taxonomy (`active / detached / dirty / error / pending`) | **not yet implemented** | [Milestone 4](../ROADMAP.md#milestone-4--operable-at-scale) — today a session either works or surfaces a plain error string; the richer taxonomy the design spec describes isn't implemented as distinct states |
| Protected-branch flag (default on for `main`) | shipped-and-tested | Enforced by CLI, daemon, and MCP alike |

## Resource behavior

| Guarantee / feature | Status | Notes |
|---|---|---|
| Checkout-cache disk budget + LRU eviction | **not yet implemented** | [Milestone 4](../ROADMAP.md#milestone-4--operable-at-scale) — no eviction of cold read-only materializations exists; every checkout persists until explicitly destroyed |
| FD budget with idle-checkout eviction | **not yet implemented** | [Milestone 4](../ROADMAP.md#milestone-4--operable-at-scale) |
| `SnapshotEvery` tuning exposed via the daemon | **not yet implemented** | [Milestone 4](../ROADMAP.md#milestone-4--operable-at-scale) — configurable in the embeddable session library (`Options.SnapshotEvery`), but `offshoot serve` always uses the default of 16 |
| Current per-session disk/FD costs documented | shipped | Documented in README's [Resource behavior](../README.md#resource-behavior): per-session FD footprint is small and fixed; disk is the sharper cost via `internal/dbfile`'s never-closed descriptors (see that package's doc comment), now mostly avoided on a clean re-open by `Checkout`'s clean-skip fast path, but not reclaimed for a checkout that does get re-materialized. "shipped" rather than "shipped-and-tested" per this page's own legend — no automated test enforces documentation accuracy |

## Platform

| Guarantee / feature | Status | Notes |
|---|---|---|
| Linux + macOS | shipped-and-tested | |
| Windows | **not supported (non-goal)** | Capture path and lock/SHM probing are POSIX-dependent; see [ROADMAP non-goals](../ROADMAP.md#non-goals-v1) |
| Multi-node orchestration / clustering | **not supported (non-goal)** | Shared-bucket safety is guaranteed by fencing; placement/failover/routing are explicitly out of scope — see [ROADMAP non-goals](../ROADMAP.md#non-goals-v1) |
| Merge (three-way) | **not supported (non-goal)** | Forks are pick-a-winner via `promote`; the escape hatch is `sqldiff` over two checkouts — see [docs/faq.md](faq.md#why-no-merge) |
