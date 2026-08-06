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

## Core model

| Guarantee / feature | Status | Notes |
|---|---|---|
| Branch = named ref (lineage, epoch, head txid, checkpoints, TTL, protected flag) | shipped-and-tested | `internal/ops/ops.go`, `internal/store` |
| One writer per lineage, enforced by lease + epoch fencing | shipped-and-tested | `internal/ops/fencing_test.go`; every object write lands under the epoch current at write time |
| CAS on every ref mutation | shipped-and-tested | Local backend: `O_CREAT\|O_EXCL` lock file. S3-compatible: conditional `PutIf`, gated by an attach-time capability probe |
| Materialized fork (child gets an independent lineage) | shipped-and-tested | Byte-copy today, not reflink — see Fork performance below |
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
| **Background flush interval** (`serve -flush-every`, on by default) | not yet implemented | [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) — today's single sharpest edge: a daemon that dies between flushes loses everything since the last one |
| Connection contract enforcement (WAL-mode probe, rollback-journal detection) | shipped-and-tested | Violation hard-fails the branch rather than diverging silently |
| Reflink/clonefile fork (`~40ms` claim in the design spec) | **not yet implemented** | [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) — fork is a synchronous local byte copy today, O(size), unbenchmarked at any scale. The spec's launch-demo number describes the *planned* mechanism, not the current one |
| Async fork-point upload (`pending` marker + fork pin on GC) | **not yet implemented** | [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) — today's fork-point snapshot upload is synchronous, inside the fork call |

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
| MCP rides the daemon (live capture, session-aware checkpoint) | shipped-and-tested | `internal/mcp/daemon_test.go`; `offshoot_checkpoint`/`offshoot_fork`/`offshoot_checkout` each probe the daemon per call and take its live path when a session is already open (and healthy) on the branch (opened by a harness — no MCP tool opens one itself); a fenced session falls back to at-rest with a warning naming its error; no session, or no daemon, and the tool runs exactly at rest as before. `offshoot_rollback`/`offshoot_promote` (target)/`offshoot_destroy` refuse rather than proceed at rest when the daemon has any session (healthy or fenced) on the affected branch. `offshoot mcp -socket PATH` names the daemon to ride |
| Python SDK (stdlib-only, over the unix-socket lifecycle API) | shipped-and-tested | `sdk/python`; not yet published to PyPI ([Milestone 3](../ROADMAP.md#milestone-3--the-eval-harness-release)) |
| TypeScript SDK (zero runtime deps) | shipped-and-tested | `sdk/typescript`; not yet published to npm ([Milestone 3](../ROADMAP.md#milestone-3--the-eval-harness-release)) |
| LangGraph `ThreadForks` companion | shipped-and-tested | `examples/langgraph-rewind/` runs it end to end |
| Read-only checkouts / sessions | **not yet implemented** | [Milestone 3](../ROADMAP.md#milestone-3--the-eval-harness-release) — one writable checkout per branch per daemon is enforced; there is no sanctioned read-only mode alongside it yet |

## Observability and security

| Guarantee / feature | Status | Notes |
|---|---|---|
| `offshoot status` (branch state, checkpoints, TTL remaining) | shipped-and-tested | |
| `session status` (durable-through txid, epoch, holder, errors) | shipped-and-tested | |
| Structured branch-state-transition logging | **not yet implemented** | [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) ("3am observability, first half") |
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
| Current per-session disk/FD costs documented | **not yet implemented** | [Milestone 2](../ROADMAP.md#milestone-2--safe-by-default-for-agents) ("resource behavior documented") |

## Platform

| Guarantee / feature | Status | Notes |
|---|---|---|
| Linux + macOS | shipped-and-tested | |
| Windows | **not supported (non-goal)** | Capture path and lock/SHM probing are POSIX-dependent; see [ROADMAP non-goals](../ROADMAP.md#non-goals-v1) |
| Multi-node orchestration / clustering | **not supported (non-goal)** | Shared-bucket safety is guaranteed by fencing; placement/failover/routing are explicitly out of scope — see [ROADMAP non-goals](../ROADMAP.md#non-goals-v1) |
| Merge (three-way) | **not supported (non-goal)** | Forks are pick-a-winner via `promote`; the escape hatch is `sqldiff` over two checkouts — see [docs/faq.md](faq.md#why-no-merge) |
