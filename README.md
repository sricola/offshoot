# offshoot

Branch SQLite like git: create, fork, checkpoint, rollback, and promote
SQLite databases — stock SQLite files, your storage, one binary.

**Status: pre-alpha — local and S3-compatible stores working (Plans 2-3); capture spike GO (Plan 1).**
Requires Go 1.24+, cgo, and the `sqlite3` CLI for tests. Linux and macOS only.

## Quickstart (60 seconds, no server, no bucket)

    go build -o offshoot ./cmd/offshoot
    ./offshoot init
    ./offshoot create app
    sqlite3 "$(./offshoot path app)" "CREATE TABLE users (name); INSERT INTO users VALUES ('ada');"
    ./offshoot checkpoint app v1
    ./offshoot fork app attempt-1        # instant branch
    sqlite3 "$(./offshoot path app@attempt-1)" "DELETE FROM users;"   # destructive experiment
    ./offshoot rollback app@attempt-1 --to fork                        # undo it
    ./offshoot promote app@attempt-1 --onto main --force               # or ship it
    ./offshoot status

Plan-2 (local mode) notes: checkpoints are full snapshots; checkout paths are
fixed at `<store>/checkouts/{db}/{branch}.db`; operations require the
checkout to be quiescent (no live writers). Daemon mode with live capture,
incremental segments, and S3/R2/Tigris backends is Plan 3.

## Storage

    offshoot -store ./.offshoot init                 # local directory (default)
    offshoot -store s3://my-bucket/offshoot init     # S3-compatible bucket

offshoot's safety rests on compare-and-swap: every branch ref update is a
conditional write. At attach time it **probes the store** and refuses to run
if conditional writes are not enforced, rather than silently degrading. That
probe re-runs on every command (every CLI invocation attaches fresh) — a
handful of sequential round trips against a remote store, paid every time by
design (fail-closed beats a cached "it was fine last time"); a long-lived
daemon in Plan 4 will amortize it across a session instead of paying it per
command.

Configuration for `s3://` specs — credentials come from the AWS SDK default
chain (environment, shared config, IAM role):

| Variable | Meaning |
|---|---|
| `OFFSHOOT_S3_ENDPOINT` | Custom endpoint (R2, Tigris, MinIO) |
| `OFFSHOOT_S3_REGION` | Region; defaults to `auto` when an endpoint is set |
| `OFFSHOOT_S3_PATH_STYLE` | `1` for path-style addressing (MinIO) |
| `OFFSHOOT_CHECKOUTS` | Where checkouts are materialized (remote stores) |

### Provider support

A provider is listed as supported only after the conformance suite and CAS
probe pass against it for real (`make test-s3`) — the in-process fake used in
unit tests proves nothing about a real provider.

| Provider | Status |
|---|---|
| MinIO | verified — `minio/minio:latest` [1], run 2026-07-31; `make test-s3` PASS |
| AWS S3 | expected to pass (conditional writes GA since Nov 2024); not yet run |
| Cloudflare R2 | expected to pass; not yet run |
| Tigris | expected to pass; not yet run |
| Google Cloud Storage (S3 interop) | **unsupported** — no conditional writes on the S3 API; the probe refuses it |

[1] `minio/minio:latest` digest: `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`

Checkouts are always real local SQLite files; only the snapshots and refs
live in the store.

## Leases and fencing

A long-running writer (Plan 5's daemon) claims a branch with a lease:

    offshoot lease list
    offshoot lease acquire app@main --ttl 60s
    offshoot lease release app@main

Acquiring or reclaiming a branch **bumps its epoch**, and every object is
written under the epoch current at the time. A writer that pauses, loses its
lease, and later resumes therefore writes into a superseded prefix that no ref
points at — it cannot corrupt the branch, and its garbage is collected with the
lineage. Expiry is wall-clock and advisory; the guarantee against an
uncooperative writer comes from the epoch fence and ref compare-and-swap, not
from the clock.

`offshoot lease acquire` exits immediately, so its lease expires unless a
long-running process renews it. It exists for inspection and for breaking a
stuck lease.

## Daemon mode

At rest, every offshoot command opens the store, does its work, and exits — so
a checkpoint has to quiesce the database. The daemon removes that constraint:
it holds the branch under lease and captures every committed transaction while
your agent keeps writing.

    offshoot serve &                      # holds leases, captures continuously
    offshoot session open app             # prints the checkout path to write to
    sqlite3 "$(offshoot session open app)" "INSERT INTO t VALUES ('agent wrote this');"
    offshoot session flush app v1          # durable in the store, writer never paused
    offshoot session status                # durable txid per session
    offshoot session close app             # releases the lease

**Durability is explicit.** Between flushes, writes are committed to SQLite but
not yet in the store; `session status` reports the txid each session is durable
through. A session that loses its lease is fenced and stops — it will not write
under a dead epoch — and `session status` shows the error.

The daemon serves a unix socket (mode 0600) under your cache directory, one per
store; override with `OFFSHOOT_SOCKET`. Daemon and agent must share a kernel
and a local filesystem: the checkout is a real SQLite file both processes open.

Design: docs/superpowers/specs/2026-07-29-offshoot-design.md
Capture-spike evidence: docs/superpowers/specs/2026-07-29-offshoot-spike-report.md
