# offshoot

Branch SQLite like git: create, fork, checkpoint, rollback, and promote
SQLite databases — stock SQLite files, your storage, one binary.

**Status: prerelease (0.1.x).** The CLI and daemon both work today: local and
S3-compatible stores, live WAL capture with incremental segments, leases and
TTL reaping, an MCP server, and Python/TypeScript SDKs. See
[docs/status.md](docs/status.md) for exactly what's shipped-and-tested,
what's shipped-but-unverified, and what's still on the [roadmap](ROADMAP.md).
Requires Go 1.24+, cgo, and the `sqlite3` CLI for tests. Linux and macOS only.

**Install:** build from source (see Quickstart below) — tagged binaries land
with the first 0.1.x release. No package-manager install yet.

**Docs:** [FAQ](docs/faq.md) (why not Litestream / LiteFS / Turso / Dolt / `cp`) ·
[CLI reference](docs/reference.md) · [architecture](docs/architecture.md) ·
[branch diff](docs/diff.md) ·
[implemented/deferred status](docs/status.md) · [roadmap](ROADMAP.md) ·
[**the eval-harness tutorial**](docs/eval-harness.md) (seed-once-fork-many
for pytest/vitest/`node:test`, from install to CI) ·
[framework recipes](docs/recipes/) (Claude Code hooks, OpenAI Agents SDK,
LlamaIndex/CrewAI)

**Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md) has dev setup and the
test tiers; [SECURITY.md](SECURITY.md) covers vulnerability reporting.
Release notes live in [CHANGELOG.md](CHANGELOG.md).

## Versioning

offshoot is in the 0.1.x prerelease series (tags `v0.1.0`, `v0.1.1`, …). The
CLI surface and the on-disk storage format may still change release to
release. Compatibility is never left to guesswork: the store's layout version
detects a mismatch outright — a newer binary refuses to write a layout an
older one wouldn't understand. 1.0 is reserved for the point the storage
format freezes.

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

Building an eval harness or a test suite around this instead of a one-off
script? [docs/eval-harness.md](docs/eval-harness.md) is the paved road:
seed once, fork per test, xdist/vitest parallelism, golden-file assertions,
TTL cleanup, and a CI recipe — for Python (`offshoot.pytest_plugin`) and
TypeScript (`testkit`) alike. Two more commands round out the inspect/debug
loop that tutorial builds on: `offshoot export <db>@<branch>[@checkpoint]
out.db` copies a checkpoint out to a plain file for handoff, and `offshoot
diff a@x b@y [--summary]` answers "what changed between these two attempts"
— see [docs/diff.md](docs/diff.md) and
[docs/reference.md](docs/reference.md).

At rest (no daemon running): checkpoints are full snapshots; checkout paths
are fixed at `<store>/checkouts/{db}/{branch}.db`; operations require the
checkout to be quiescent (no live writers). Daemon mode (below) layers live
capture, incremental segments, and S3/R2/Tigris backends on top of the same
commands.

## Storage

    offshoot -store ./.offshoot init                 # local directory (default)
    offshoot -store s3://my-bucket/offshoot init     # S3-compatible bucket

offshoot's safety rests on compare-and-swap: every branch ref update is a
conditional write. At attach time it **probes the store** and refuses to run
if conditional writes are not enforced, rather than silently degrading. That
probe re-runs on every command (every CLI invocation attaches fresh) — a
handful of sequential round trips against a remote store, paid every time by
design (fail-closed beats a cached "it was fine last time"); a long-lived
daemon (see Daemon mode below) amortizes it across a session instead of
paying it per command.

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

A long-running writer — offshoot's daemon (see Daemon mode below) — claims a branch with a lease:

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

By default the daemon also ships every open session's work on a timer,
independent of any explicit `flush`:

    offshoot serve -flush-every 30s        # the default; 0 disables it

`-flush-every` bounds how much committed-but-unflushed work is ever at risk:
worst case, a daemon that dies loses at most one `-flush-every` interval's
worth of writes, instead of everything since the last manual flush. `-flush-every
0` turns the timer off and returns to durability that only advances when
something calls `flush` explicitly — this project's original behavior, still
available if you want it. The cadence is a daemon-wide setting applied to
every session `offshoot serve` opens; there's no way yet to give one session
a different cadence than the rest (see [docs/status.md](docs/status.md)).

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

A session's very first auto-flush after `session open` used to be an
unconditional full snapshot, no matter where that landed in the
sixteen-flush cadence — even for a session that never wrote anything —
because closing a startup-rebase race required treating the checkout as
though it might have changed. It no longer does, when TWO things are both
provably true: the checkout `Open` received was already byte-identical to
the branch's head at the moment `Open` checked (the same `.sum` sidecar
clean-and-current fast path ["Resource behavior"](#resource-behavior) below
describes), AND the LTX checksum recorded in that SAME sidecar — stamped
there by `Checkout`/`Checkpoint`/`Rollback`/`Promote`, or by a session's
own clean `Close` — exactly matches what the checkout actually contains
once the session's real startup rebase finishes running. That second check
is not redundant with the first: `Open` can return to its caller before
its own startup rebase has actually finished (closing a different, older
race — see `internal/session/session.go`'s `rebaseline` doc comment), so a
write landing in that narrow window is folded into the checkout without
the first check ever seeing it; the checksum comparison is what catches
exactly that case and forces a real settle instead of silently losing the
write. Reading that checksum out of the local sidecar rather than fetching
it fresh from the store costs `Open` nothing extra — no store round trip
at all, and specifically no download of the head object itself, which is
exactly what an earlier, since-reverted version of this fix did on every
single `Open`, undoing its own benefit for a read-only session against a
snapshot head. Only when both hold — the common case for a read-only
daemon session reopened against a checkout nothing has touched since —
does this settling flush upload nothing at all. A session whose checkout
instead had to be (re)materialized first (a branch's first-ever open, a
dirty/stale local checkout, or one another writer moved past)
still pays the settling flush exactly as before: one full-snapshot upload
sized to the database, not fixed — measured at 541.9MB uploaded for a
512MB source database. It happens at most once per session either way, not
on every tick after that — see
[docs/benchmarks.md](docs/benchmarks.md#settling-flush-cost-task-2-controller-decision)
for the measurement and `internal/session/session.go`'s `rebaseline` doc
comment for the exact suppression condition and its proof obligation.

Under continuous writing, the daemon's default `-flush-every 30s` combined
with the default sixteen-flush snapshot cadence means a full snapshot ships
roughly every 8 minutes (30s × 16 ticks) for as long as the agent keeps
writing between every tick. An idle session — nothing committed and no
rebase since the last successful flush — skips the tick entirely (no object
write, no ref write at all), so a quiet session pays none of this; the cost
scales with how much the agent is actually writing, not with wall-clock time
alone.

The at-rest `offshoot checkpoint` still writes a full snapshot every time. It
runs without a daemon, so it has no record of which pages changed and would
have to diff the whole database to find out. If you checkpoint large databases
in a loop, run a daemon.

Forking is a different cost from flushing. At rest, `offshoot fork` (and the
same machinery behind `rollback`/`promote`) copies the source snapshot object
directly — a filesystem clone or a backend-side server copy, not a
materialize-and-re-encode round trip — whenever the checkpoint being forked
resolves to a single, unsegmented snapshot; once a daemon session has
flushed even one segment past a branch's last snapshot, fork falls back to
the slower materialize-and-re-encode path for that checkpoint. Measured
before/after numbers for both paths, both backends, are in
[docs/benchmarks.md](docs/benchmarks.md).

The daemon serves a unix socket (mode 0600) under your cache directory, one per
store; override with `OFFSHOOT_SOCKET`, or pass `-socket PATH` to `offshoot
serve`. If you use `-socket PATH` on `serve`, pass the same `-socket PATH` to
every `offshoot session ...` command too (or export `OFFSHOOT_SOCKET`
instead) — the CLI has no other way to find a non-default socket. Daemon and
agent must share a kernel and a local filesystem: the checkout is a real
SQLite file both processes open.

### Resource behavior

There are no resource budgets yet — no checkout-cache disk limit, no FD
limit, no eviction of cold sessions (all [Milestone 4](ROADMAP.md#milestone-4--operable-at-scale)).
In today's code, an open session's own FD footprint is small and fixed
(capture engine reader, WAL reader, lease-renewal timer — none of it scales
with how long the session stays open or how much it writes). Disk is the
sharper cost: `Checkout` no longer re-materializes a checkout that's already
clean and current at the branch's head, which removes the common case of
stranding a leftover file descriptor — and the full copy of the database
behind it — on every session open. But a checkout that *does* get
re-materialized (dirty, stale, or destroyed/reaped while an earlier open's
descriptor still points at its now-unlinked inode) still strands one
descriptor, and the disk behind it, for the life of the daemon process —
there is no reclamation path for that yet.

That skip fires whenever the checkout's `.sum` sidecar matches the branch
ref's current head — and that now stays true across the daemon's default
config too: a session's clean `Close` refreshes the sidecar to whatever it
last durably flushed (not just `Checkout`, `Checkpoint`, `Rollback`, and
`Promote` anymore), and the paired settling-flush suppression above means a
reopened, unmodified session doesn't even need that flush to happen for the
sidecar to already be current — a session that only ever read never needs a
refresh at all, since nothing about the checkout changed since `Open`.

"Clean" has real limits, though. A `Close` after a flush that failed, or on
a session whose lease was fenced, does not stamp the sidecar — the
checkout's true durable state is ambiguous at that point, and guessing
"clean" would risk a later reopen silently skipping content that was never
actually saved — so that session's checkout re-materializes on the next
open, once. So does a session that ever took a mid-session
rebase-on-divergence (a real WAL discontinuity, not the ordinary startup
rebase): the replica's provenance is no longer a straight, unbroken line
back to the checkout `Open` originally seeded it from, so `Close`
conservatively leaves the sidecar alone rather than risk stamping content
the checkout's own bytes never physically received. And the stamp itself
is only ever taken from the capture engine's OWN post-shutdown fingerprint
(computed only once its shutdown fully verifies the checkout's WAL was
cleanly and completely folded in) rather than re-derived independently — a
foreign write landing in the narrow window around the engine's final
checkpoint leaves that verification unmet, and `Close` correctly leaves the
sidecar unstamped rather than risk fingerprinting content the engine itself
refused to vouch for. Outside those cases — the overwhelming majority of
sessions in practice — a daemon that keeps reopening the same branch stays
flat: no re-materialize, no stranded descriptor, on any reopen of a
checkout nothing else touched since the prior session's clean close.
Restarting the daemon reclaims everything a fresh process never opened.

One more property worth naming explicitly, not a limit but a tradeoff: once
a checkout's sidecar is clean-and-current (however it got that way), the
next `Checkout` is served straight from disk without ever consulting the
object store's chain — including if that chain has since been corrupted.
This was already true for a sidecar stamped by `Checkout`/`Checkpoint`/
`Rollback`/`Promote` (Milestone 2 Task 1); it now also applies after an
ordinary session's clean close. See
[docs/status.md](docs/status.md)'s "Clean-and-current checkout served
without chain validation" row.

**Read-only historical checkouts** (`offshoot checkout --at <checkpoint>
--read-only`, `Client.checkout_at`/`checkoutAt`) live in a completely
separate tree from the writable-checkout cache described above:
`<store-root>/checkouts-ro/<db>/<branch>@<checkpoint>.db`, one file per
`(db, branch, checkpoint)` ever materialized this way, `chmod 0444`. This
cache has none of the above's costs or caveats: no `.sum` sidecar, no lease,
no stranded `dbfile` descriptor (nothing in this codebase ever opens a
`checkouts-ro` file through a live capture engine, so the stray-close lock
hazard that motivates `internal/dbfile` in the first place doesn't apply to
it), and no reclamation problem to solve later — **it is safe to `rm -rf`
the entire `checkouts-ro` directory at any time**; the next call for
anything under it just rebuilds what it needs from the store. A repeat call
for the same checkpoint is a cheap cache hit (the file already exists, a
checkpoint's content never changes, so it's returned as-is with no store
access at all) unless `--force`/`force=True` is given. `offshoot export`'s
output has the identical zero-ongoing-relationship property, just written
wherever the caller pointed it rather than under a fixed cache path.

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

Agent-created forks expire by default, so an agent that forks and forgets
doesn't leak branches forever: `offshoot_fork` applies `offshoot mcp
-default-ttl` (default `24h`) to any call that omits its own `ttl`; pass
`ttl:"<duration>"` on the call to override it, or `ttl:"none"` to fork a
branch that never expires even under a configured default:

    offshoot mcp -default-ttl 12h        # forks default to 12h unless overridden
    offshoot mcp -default-ttl none       # forks are immortal unless a call sets ttl

The tool's response echoes the TTL it applied and, when there is one, the
computed expiry timestamp, so both are visible in the agent's own
transcript. **A TTL alone does not reap anything** — reaping is the
janitor's job (`offshoot serve`), and `offshoot mcp` runs no daemon of its
own, so a daemonless MCP setup only sweeps expired branches when
`offshoot gc` is run by hand.

**MCP rides a running daemon when one is up, but only for a branch a
session is already open on.** `offshoot mcp` never opens a session itself —
that's still a harness's job (the SDKs, `offshoot session open`, or your own
loop) — but on every call, `offshoot_checkpoint`, `offshoot_fork`, and
`offshoot_checkout` each check fresh whether the daemon has one open for the
branch in question. If so: `offshoot_checkpoint` flushes it live through the
daemon (no quiesce, no full-snapshot re-encode, no lease collision);
`offshoot_fork` forks through the daemon, which flushes an open source
session first so an unflushed write always lands in the child; and
`offshoot_checkout` returns that session's own live checkout path instead of
materializing a separate at-rest copy. **Without an already-open session,
every one of those tools runs exactly as it does with no daemon at all** —
a daemon merely running in the background changes nothing on its own.
`offshoot mcp -socket PATH` names the daemon to ride; omit it and MCP derives
the same default socket `offshoot serve` does for the store, so the two
agree without either side hardcoding a path.

`offshoot_rollback`, `offshoot_promote` (checked against its `target`), and
`offshoot_destroy` take the opposite stance from checkpoint/fork/checkout:
each **refuses outright — even with `force`** — whenever the daemon has any
session, healthy or fenced, open on the affected branch, rather than
proceeding at rest. All three repoint or delete a branch's ref directly,
bypassing the daemon entirely; without the refusal, one of these calls could
clear a lease or delete/repoint storage out from under a session the daemon
still believes it owns. `force` has no effect on *this* refusal, whatever
else it does at the ops layer underneath (it overrides a live lease on
destroy; promote never gates on the target's lease at all, force or not,
and clears it unconditionally as a repoint side effect — see the
`offshoot promote` entry in [docs/reference.md](docs/reference.md)). The
remedy here is the same regardless: close the session first (`offshoot
session close`, or the SDK/harness equivalent) and retry.
`offshoot_promote`'s `source` is the one exception:
an open session there does not block the promote, but the promoted state is
the source's last-flushed/checkpointed head, not any write still unflushed
in that live session.

**In short: the good path for `offshoot mcp` requires a harness-opened
session** — the SDKs, `offshoot session open`, or your own loop, opened
*before* the agent's tool calls. Without one, every tool still works, just
entirely at rest, exactly as if no daemon were running.

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

`Client` also exposes `branches()`, `dbs()`, `export()` (materialize a
checkpoint or head to a plain file), and `checkout_at()` (a read-only
historical checkout).

**Testing with pytest?** `pip install "offshoot-db[pytest]"` registers
`offshoot_daemon`/`offshoot_db`/`offshoot_fork` fixtures automatically —
seed once, fork a fresh isolated branch per test, TTL-backstopped cleanup,
`pytest-xdist` parallelism (one daemon per worker). Full tutorial:
[docs/eval-harness.md](docs/eval-harness.md); condensed reference:
`sdk/python/README.md`'s pytest-fixture-plugin section.

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

`Client` also exposes `branches()`, `dbs()`, `export()`, and
`checkoutAt()` — the same surface as the Python client above.

**Testing with vitest/jest/`node:test`?** `@offshoot-db/client/testkit`
(`startDaemon`/`seedOnce`/`forkPerTest`/`dump`) is the framework-agnostic
counterpart of the pytest fixtures above — same seed-once-fork-many
semantics, wired into whatever `beforeAll`/`afterEach` hooks your test
runner calls. See [docs/eval-harness.md](docs/eval-harness.md)'s
TypeScript section and `sdk/typescript/README.md`'s testkit section.

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

### Other agent frameworks

LangGraph is the one framework with a real companion package; everyone
else gets a short recipe instead of an adapter — see
[docs/recipes/](docs/recipes/): Claude Code's MCP config and hooks pattern
([claude-agent-sdk.md](docs/recipes/claude-agent-sdk.md)), the OpenAI
Agents SDK's `SQLiteSession` pointed at an offshoot checkout path
([openai-agents.md](docs/recipes/openai-agents.md)), and short honest notes
on LlamaIndex and CrewAI
([frameworks.md](docs/recipes/frameworks.md)).
