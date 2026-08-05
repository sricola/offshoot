# offshoot

Branch SQLite like git: create, fork, checkpoint, rollback, and promote
SQLite databases — stock SQLite files, your storage, one binary.

**Status: pre-alpha — local and S3-compatible stores working (Plans 2-3); capture spike GO (Plan 1).**
Requires Go 1.24+, cgo, and the `sqlite3` CLI for tests. Linux and macOS only.

## Quickstart (60 seconds, no server, no bucket)

    go build -o offshoot ./cmd/offshoot
    ./offshoot init
    ./offshoot create app
    sqlite3 "$(./offshoot checkout app)" "CREATE TABLE users (name); INSERT INTO users VALUES ('ada');"
    ./offshoot checkpoint app v1
    ./offshoot fork app attempt-1        # instant branch
    sqlite3 "$(./offshoot checkout app@attempt-1)" "DELETE FROM users;"   # destructive experiment
    ./offshoot rollback app@attempt-1 --to fork                        # undo it
    ./offshoot promote app@attempt-1 --onto main --force               # or ship it
    ./offshoot status

Runnable demo: [`examples/parallel-attempts/`](examples/parallel-attempts/)
forks a database three ways, races three migrations against the forks,
promotes the one that's actually correct, and discards the other two —
`./examples/parallel-attempts/run.sh`.

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

    offshoot serve &                       # holds leases, captures continuously
    sleep 1                                # let the listener come up
    P=$(offshoot session open app)         # capture the checkout path once
    sqlite3 "$P" "CREATE TABLE t (v); INSERT INTO t VALUES ('agent wrote this');"
    offshoot session flush app v1          # durable in the store, writer never paused
    offshoot session status                # durable txid per session
    offshoot session close app             # releases the lease

**Durability is explicit.** Between flushes, writes are committed to SQLite but
not yet in the store; `session status` reports the txid each session is durable
through. A session that loses its lease is fenced and stops — it will not write
under a dead epoch — and `session status` shows the error.

### TTLs and the reaping janitor

A branch can carry a TTL, set at fork time or any time after:

    offshoot fork app attempt-1 --ttl 2h        # reap-eligible 2h after last activity
    offshoot touch app@attempt-1 --ttl 30m      # resets the clock, changes the TTL
    offshoot touch app@attempt-1                # resets the clock, TTL unchanged
    offshoot touch app@attempt-1 --ttl none     # clears the TTL

Per the design spec:

> TTL is measured from the last durable write (last shipped segment) or lease
> renewal, whichever is later; `offshoot touch` resets it explicitly. A branch
> with an active lease is never reaped — expiry defers until the lease is
> released or times out (a wedged holder loses the lease first, then TTL
> applies). Creating a child does not extend the parent. Branches without a
> TTL live until destroyed.

In practice: a daemon session holding a branch open keeps renewing its lease,
so the janitor can never reap a branch this daemon is actively writing to,
TTL or not; `offshoot touch` is how you defer expiry on a branch nobody has
open. Protected branches (`main`, by default) are never reaped regardless of
TTL. `offshoot status` and the daemon's own responses report the TTL
re-rendered through Go's canonical `time.Duration.String()` — a fork
requested with `--ttl 1h` reads back as `ttl=1h0m0s`, not the literal `1h` it
was given.

`offshoot serve` runs the janitor — TTL reaping plus the periodic GC sweep —
on an interval:

    offshoot serve -reap-every 1m -gc-grace 15m   # both are the defaults

`-reap-every 0` disables the janitor entirely: neither TTL reaping nor the
periodic GC sweep runs (GC is still available on demand via `offshoot gc`).
`-gc-grace` is how long a tombstoned (unreachable) lineage's storage must sit
before a later cycle actually deletes it — `0` makes it eligible for deletion
on the very next cycle after being marked, rather than disabling GC.

### What a flush costs

A daemon flush writes only the pages that changed since the previous flush —
the capture engine already knows exactly which those are. Every sixteenth
flush writes a full snapshot instead, so materializing a branch never replays
an unbounded chain: a read applies one snapshot plus at most fifteen
segments. (That cadence is configurable when embedding the session library
directly — `Options.SnapshotEvery` — but the daemon does not currently
expose it, so every daemon-managed session uses the default of sixteen.)

The at-rest `offshoot checkpoint` still writes a full snapshot every time. It
runs without a daemon, so it has no record of which pages changed and would
have to diff the whole database to find out. If you checkpoint large databases
in a loop, run a daemon.

The daemon serves a unix socket (mode 0600) under your cache directory, one per
store; override with `OFFSHOOT_SOCKET`, or pass `-socket PATH` to `offshoot
serve`. If you use `-socket PATH` on `serve`, pass the same `-socket PATH` to
every `offshoot session ...` command too (or export `OFFSHOOT_SOCKET`
instead) — the CLI has no other way to find a non-default socket. Daemon and
agent must share a kernel and a local filesystem: the checkout is a real
SQLite file both processes open.

Design: docs/superpowers/specs/2026-07-29-offshoot-design.md
Capture-spike evidence: docs/superpowers/specs/2026-07-29-offshoot-spike-report.md

## Integration surface

Four ways to talk to offshoot (the design spec's "integration surface"): the
CLI above needs no daemon and no SDK; everything else below is a client of
the daemon's lifecycle API and requires `offshoot serve` already running.

### MCP

`offshoot mcp` speaks the Model Context Protocol on stdio, so an agent can
branch on its own initiative instead of asking you to run commands:

    claude mcp add offshoot -- offshoot -store ./.offshoot mcp

The agent gets seven tools — list, checkout, checkpoint, fork, rollback,
promote, destroy — described so it knows *when* to use them: fork before a
risky migration, checkpoint when tests pass, roll back when they don't,
promote the attempt that worked.

Destructive tools respect the same protected-branch rules as the CLI: an agent
can fork and experiment freely, but promoting onto or destroying `main`
requires an explicit force, and the refusal tells the agent so.

### Python SDK

`sdk/python` is a stdlib-only, thin client over the daemon's lifecycle API —
it never opens SQLite itself and can't do anything the CLI can't; it just
lets your process drive a running daemon instead of shelling out. Not yet
published to PyPI — import it from a checkout of this repo:

    offshoot -store ./.offshoot init
    offshoot serve -socket /tmp/o.sock &

```python
import sys; sys.path.insert(0, "sdk/python")
import offshoot

with offshoot.connect("/tmp/o.sock") as c:
    c.create("app")
    s = c.open("app")              # sqlite3.connect(s.path); write; commit
    s.flush("v1")                  # durable in the store, writer never paused
    c.fork("app", "main", "try", ttl="2h")
    s.close()
```

### TypeScript SDK

`sdk/typescript` is the same thin client, zero runtime dependencies. Also not
yet published to npm — build and import it from a checkout of this repo:

    offshoot -store ./.offshoot init
    offshoot serve -socket /tmp/o.sock &
    (cd sdk/typescript && npm install --no-audit --no-fund && npm run build)

```ts
import { connect } from "./sdk/typescript/dist/client.js";

const c = await connect("/tmp/o.sock");
await c.create("app");
const s = await c.open("app");     // sqlite3 s.path; write; commit
await s.flush("v1");               // durable in the store, writer never paused
await c.fork("app", "main", "try", { ttl: "2h" });
await s.close();
await c.close();
```

Both SDKs are exercised against a real daemon by `make test-sdks` (needs
`python3` and `node`/`npm` on PATH — not part of the default `make test`,
which stays hermetic to the Go suite).

### LangGraph

`offshoot.langgraph.ThreadForks` is a checkpointer *companion*, not a
`BaseCheckpointSaver`: it maps each LangGraph thread to its own offshoot
branch, so rewinding a thread to an earlier checkpoint and retrying also
forks the *database* from that same point — the retry never inherits what
the original attempt wrote after it. See
[`examples/langgraph-rewind/`](examples/langgraph-rewind/), runnable with
`python3 examples/langgraph-rewind/agent.py` — no server or bucket needed
(it builds `offshoot` and starts its own private daemon).
