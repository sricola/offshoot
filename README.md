# offshoot

Fork-per-attempt databases for AI agents and eval harnesses. Branch SQLite
like git — create, fork, checkpoint, rollback, promote — as stock SQLite
files, on your storage, with one binary.

An agent attempt or eval run needs a real database it can trash: mocks
aren't real, re-seeding is slow, and container or VM snapshots version a
whole machine to get at one file. offshoot branches the database itself —
copy-on-write forks of stock SQLite files over a local directory or an
S3-compatible bucket. A shared fork of a 100 MB database adds
[377 bytes](docs/benchmarks.md#added-object-store-bytes-per-fork-100-mb-database)
to the store — about 280,000× less than a copy — and every checkout is a
plain `.db` file any SQLite tool opens. Try N migrations or N agent
attempts on N forks, `promote` the winner, and let the losers expire.

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
`./examples/parallel-attempts/run.sh`. Real recording:
[`docs/demo/parallel-attempts.cast`](docs/demo/parallel-attempts.cast)
(play locally with `asciinema play`).

<details>
<summary>Transcript of that demo, from a real run — nothing doctored</summary>

```
==> building offshoot
==> creating a database with some data
    3 orders, checkpoint 'before-migration'
==> keeping the pre-migration state on its own branch
    forked 'pre-migration' from the 'before-migration' checkpoint — promote wipes main's own checkpoint history, so this fork is what actually survives
==> forking three attempts (instant, no copy)
==> running the migrations in parallel forks
    attempt-1: FAIL
    attempt-2: FAIL
    attempt-3: PASS
==> winner: attempt-3
==> promoting the winner onto main
    promoted
==> discarding the losers
==> main now has the migrated data:
    id|total|total_cents
    1|19.99|1999
    2|8.70|870
    3|4.35|435
==> and the pre-migration state is still one command away, on its own branch:
    offshoot checkout shop@pre-migration
```

</details>

Building an eval harness or a test suite around this instead of a one-off
script? [docs/eval-harness.md](docs/eval-harness.md) is the paved road:
seed once, fork per test, xdist/vitest parallelism, golden-file assertions,
TTL cleanup, and a CI recipe — for Python (`offshoot.pytest_plugin`) and
TypeScript (`testkit`) alike. `offshoot export` copies a checkpoint out to
a plain file for handoff, and `offshoot diff` answers "what changed between
these two attempts" — see [docs/diff.md](docs/diff.md) and
[docs/reference.md](docs/reference.md).

## Why it's different

- **Copy-on-write forks, measured.** A shared fork writes two tiny
  objects — 377 B for a 100 MB database, flat from 1 to 100 forks — and
  forking a named checkpoint takes ~9–12 ms whether the database is 12 MB
  or 1 GB. A diverging child pays only for the pages it changes (~776 B
  per single-row transaction). Numbers, method, and the honest caveats:
  [docs/benchmarks.md](docs/benchmarks.md#copy-on-write-fork-cost-v02x).
- **kill&nbsp;-9 durable.** The torture harness runs a stock `sqlite3` CLI
  writer and `SIGKILL`s it mid-write on roughly half of every round, while
  bouncing the capture engine mid-traffic every 10th round; the replica
  must converge to byte-identical dump output after every round. A 300 s
  run is ~3,500 rounds — zero divergence — and it runs in CI on a nightly
  cadence: [docs/testing.md](docs/testing.md#the-kill--9-torture-harness).
- **Stock everything.** A checkout *is* a SQLite file — no forked engine,
  no special VFS on the read path — and `offshoot export` materializes any
  branch or checkpoint to a plain `.db` with zero ongoing relationship to
  the store. The exit hatch is `cp`, and the pre-1.0
  [stability contract](docs/stability.md) guarantees any format break ships
  with a migration or a documented export path in the same release.
- **Agent-native.** MCP tools so the agent forks before risky work and
  promotes what passed ([`offshoot mcp`](#mcp)); TTL'd branches that reap
  themselves so a forgotten attempt doesn't leak; pytest fixtures and a
  vitest/jest testkit for fork-per-test isolation
  ([docs/eval-harness.md](docs/eval-harness.md)); a LangGraph companion
  and framework recipes ([docs/recipes/](docs/recipes/)).

## What offshoot deliberately doesn't do

- **No row-level merge.** The workload is fork-many-keep-one: `promote`
  the winner whole, let the losers TTL away. Real merge would forfeit the
  single-fenced-writer invariant the safety story rests on — if you need
  it, [Dolt is built for that](docs/faq.md#can-i-merge-two-branches).
- **No multi-writer branches.** Exactly one leased, epoch-fenced writer
  per lineage; two agents writing "at once" get two forks and a `promote`
  ([why](docs/faq.md#why-one-writer-per-branch)).
- **No managed service, no multi-node.** Your bucket, your binary,
  Apache-2.0; replication, failover, and the word "cluster" are explicitly
  out of scope for v1 ([non-goals](ROADMAP.md#non-goals-v1),
  [why not Turso/LiteFS](docs/faq.md)).

More "why not X" (Litestream, Dolt, Neon, plain `cp`):
[docs/faq.md](docs/faq.md).

## Install

- **Homebrew**:
  `brew tap sricola/offshoot https://github.com/sricola/offshoot && brew install offshoot`
  (formula lives in-repo at [`Formula/offshoot.rb`](Formula/offshoot.rb))
- **Docker:**
  `docker run --rm -v offshoot-data:/data ghcr.io/sricola/offshoot:latest init`
  — images publish to GHCR on every tagged release; the store lives in the
  `/data` volume, so reuse `-v offshoot-data:/data` across commands
  (`... offshoot:latest create app`, `... offshoot:latest serve`, and so on)
- **Prebuilt binaries**:
  `offshoot_vX_os_arch.tar.gz` (+ `.sha256`) from the
  [releases page](https://github.com/sricola/offshoot/releases), published
  for each tagged release
- **From source:** the Quickstart above (Go 1.25+, cgo)

Requires Go 1.25+ and cgo to build, and the `sqlite3` CLI for tests. Linux
and macOS only. **Windows:** use WSL2 — the linux binaries, Docker image,
and build-from-source all work there as-is. Native Windows is unsupported:
offshoot leans on POSIX file semantics (unix sockets, POSIX locks) that
don't map cleanly to Windows
([why](docs/faq.md#why-no-windows-support)).

## Status

**Prerelease (0.2.x).** What's shipped and exercised by tests that would
fail if it broke: local and S3-compatible stores behind a shared
conformance suite, copy-on-write forks, live WAL capture with incremental
segments, leases with epoch fencing, CAS on every ref update, TTL reaping
and GC, checkpoint/rollback/promote/export/diff, the daemon with metrics
and events, an MCP server, and Python/TypeScript SDKs with test fixtures.
[docs/status.md](docs/status.md) is the honest per-feature accounting —
shipped-and-tested vs. shipped-but-unverified vs. still on the
[roadmap](ROADMAP.md) — and [docs/testing.md](docs/testing.md) shows the
CI gates behind the "tested" column.

The caveats, stated plainly: the CLI surface and the on-disk storage
format may still change before 1.0 — but never silently. Every store
records a layout version, and a binary that doesn't understand a store's
layout refuses the whole store rather than guessing (0.2.0's first
copy-on-write fork exercised exactly that gate for real — see
[CHANGELOG.md](CHANGELOG.md)). Any format break ships in the same release
with a migration or a documented `export` → `create --from` path: the
[stability contract](docs/stability.md) is the full promise, including the
proposed v1.0 criteria. 1.0 is reserved for the point the storage format
freezes.

## Storage

    offshoot -store ./.offshoot init                 # local directory (default)
    offshoot -store s3://my-bucket/offshoot init     # S3-compatible bucket

offshoot's safety rests on compare-and-swap: every branch ref update is a
conditional write. At attach time it **probes the store** and refuses to
run if conditional writes are not enforced, rather than silently
degrading. That probe re-runs on every command (every CLI invocation
attaches fresh) — fail-closed beats a cached "it was fine last time"; a
long-lived daemon (below) amortizes it across a session instead of paying
it per command.

Configuration for `s3://` specs — credentials come from the AWS SDK default
chain (environment, shared config, IAM role):

| Variable | Meaning |
|---|---|
| `OFFSHOOT_S3_ENDPOINT` | Custom endpoint (MinIO, or any S3-compatible endpoint) |
| `OFFSHOOT_S3_REGION` | Region; defaults to `auto` when an endpoint is set |
| `OFFSHOOT_S3_PATH_STYLE` | `1` for path-style addressing (MinIO) |
| `OFFSHOOT_CHECKOUTS` | Where checkouts are materialized (remote stores) |

### Provider support

A provider is listed as supported only after the conformance suite and CAS
probe pass against it for real (`make test-s3`) — the in-process fake used
in unit tests proves nothing about a real provider.

| Provider | Status |
|---|---|
| MinIO | verified in CI — the conformance suite runs against real MinIO on every PR and push to main |
| AWS S3 | verified — `TestS3RealProvider` (probe + conformance + multipart) passed against a real bucket (us-east-1, 2026-08-13) |
| Google Cloud Storage (S3 interop) | **unsupported** — no conditional writes on the S3 API; the probe refuses it ([why](docs/faq.md#why-no-google-cloud-storage)) |

[1] `minio/minio:latest` digest: `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`

Checkouts are always real local SQLite files; only the snapshots and refs
live in the store.

At rest (no daemon running): checkpoints are full snapshots; checkout
paths are fixed at `<store>/checkouts/{db}/{branch}.db`; operations
require the checkout to be quiescent (no live writers). Daemon mode
(below) layers live capture, incremental segments, and continuous
durability on top of the same commands.

## Daemon mode

At rest, every offshoot command opens the store, does its work, and exits —
so a checkpoint has to quiesce the database. The daemon removes that
constraint: it holds the branch under lease and captures every committed
transaction while your agent keeps writing.

    offshoot serve &                       # holds leases, captures continuously
    sleep 1                                # let the listener come up
    P=$(offshoot session open app)         # capture the checkout path once
    sqlite3 "$P" "CREATE TABLE t (v); INSERT INTO t VALUES ('agent wrote this');"
    offshoot session flush app v1          # durable in the store, writer never paused
    offshoot session status                # durable txid per session
    offshoot session close app             # releases the lease

**Durability is explicit and reported.** Between flushes, writes are
committed to SQLite but not yet in the store; `session status` reports the
txid each session is durable through. By default the daemon also ships
every open session's work on a timer:

    offshoot serve -flush-every 30s        # the default; 0 disables it

`-flush-every` bounds how much committed-but-unflushed work is ever at
risk: worst case, a daemon that dies loses at most one interval's worth of
writes. `0` returns to durability that advances only on explicit `flush`.
The cadence is daemon-wide, not per-session
([docs/status.md](docs/status.md)). A session that loses its lease is
fenced and stops — it will not write under a dead epoch — and
`session status` shows the error.

The daemon serves a unix socket (mode 0600) under your cache directory,
one per store; override with `OFFSHOOT_SOCKET` or `-socket PATH` (pass the
same to every `offshoot session ...` command). Daemon and agent must share
a kernel and a local filesystem: the checkout is a real SQLite file both
processes open.

### Leases and fencing

A long-running writer — the daemon — claims a branch with a lease:

    offshoot lease list
    offshoot lease acquire app@main --ttl 60s
    offshoot lease release app@main

Acquiring or reclaiming a branch **bumps its epoch**, and every object is
written under the epoch current at the time. A writer that pauses, loses
its lease, and later resumes writes into a superseded prefix that no ref
points at — it cannot corrupt the branch, and its garbage is collected
with the lineage. Expiry is wall-clock and advisory; the guarantee against
an uncooperative writer comes from the epoch fence and ref
compare-and-swap, not from the clock
([docs/testing.md](docs/testing.md#fencing-and-cas-in-two-paragraphs)).
`offshoot lease acquire` exits immediately, so its lease expires unless
renewed — it exists for inspection and for breaking a stuck lease.

### TTLs and the reaping janitor

A branch can carry a TTL, set at fork time or any time after:

    offshoot fork app attempt-1 --ttl 2h        # reap-eligible 2h after last activity
    offshoot touch app@attempt-1 --ttl 30m      # resets the clock, changes the TTL
    offshoot touch app@attempt-1                # resets the clock, TTL unchanged
    offshoot touch app@attempt-1 --ttl none     # clears the TTL

TTL is measured from the last durable write or lease renewal, whichever is
later. A branch with an active lease is never reaped — a daemon session
holding a branch open keeps renewing, so the janitor can never reap a
branch it's actively writing to. Protected branches (`main`, by default)
are never reaped regardless of TTL; branches without a TTL live until
destroyed. TTLs read back re-rendered through Go's canonical duration form
(`--ttl 1h` reports as `ttl=1h0m0s`).

`offshoot serve` runs the janitor — TTL reaping plus the periodic GC
sweep — on an interval:

    offshoot serve -reap-every 1m -gc-grace 15m   # both are the defaults

`-reap-every 0` disables the janitor entirely (GC stays available on
demand via `offshoot gc`); `-gc-grace` is how long a tombstoned lineage's
storage sits before a later cycle actually deletes it.

### What a flush costs

A daemon flush writes only the pages that changed since the previous
flush. Every sixteenth flush writes a full snapshot instead, so
materializing a branch never replays an unbounded chain: a read applies
one snapshot plus at most fifteen segments. `offshoot serve
-snapshot-every N` tunes that cadence (default 16) — lower N means
cheaper, more tightly bounded reads; higher N amortizes the full-snapshot
upload across more flushes — see `offshoot serve`'s entry in
[docs/reference.md](docs/reference.md) for the full trade-off. An idle
session — nothing committed since the last successful flush — skips the
tick entirely, so a quiet session pays nothing; cost scales with what the
agent actually writes, not wall-clock time.

A session whose checkout had to be (re)materialized at open pays one
settling full-snapshot flush, once per session; a session reopened against
a clean, current checkout uploads nothing at all for it. The measurement
and the exact suppression condition:
[docs/benchmarks.md](docs/benchmarks.md#settling-flush-cost-task-2-controller-decision)
and `internal/session/session.go`'s `rebaseline` doc comment.

The at-rest `offshoot checkpoint` still writes a full snapshot every time —
it runs without a daemon, so it has no record of which pages changed. If
you checkpoint large databases in a loop, run a daemon.

Forking is a different cost from flushing — usually no storage cost at
all. `offshoot fork` shares the parent's already-durable objects through a
base pointer: the child records where it forked from and writes new
objects only as it diverges, so N forks of a G-byte database cost
near-zero added store bytes rather than N×G, and reads stay bounded by
construction. The asymmetry to know: **fork shares; `promote`,
`rollback`, and `compact` each materialize a full independent copy**
(measured numbers in [docs/benchmarks.md](docs/benchmarks.md)). Destroying
a parent stays instant, but its bytes are reclaimed only once no surviving
shared child still reads through them — `offshoot compact` cuts that cord
on demand. The first shared fork bumps the store to layout version 2,
which locks pre-copy-on-write binaries out of the whole store — the
refusal is the protection. Full model:
[docs/reference.md](docs/reference.md)'s `fork`/`compact`/`destroy`
sections and
[docs/operations.md](docs/operations.md#storage-sharing-copy-on-write-forks);
the storage-cost ledger, stated plainly:
[docs/faq.md](docs/faq.md#storage-cost-honestly).

### Metrics, HTTP, and events

`offshoot serve -http ADDR` starts a loopback-by-default,
token-authenticated HTTP listener alongside the unix socket:
`GET /metrics` (Prometheus text exposition, zero new dependencies),
`GET /healthz`, `POST /rpc` (the same protocol the socket speaks),
`GET /events` (Server-Sent Events for flush/fork/reap/eviction/fencing),
and token-gated `GET /debug/pprof/*`. Six computed branch states answer
"what's this branch doing right now", and `-ro-cache-budget` bounds the
read-only checkout cache with LRU eviction. All of it is single-node:
[docs/operations.md](docs/operations.md) has the metrics reference, states
table, event schema, budget mechanics, and the HTTP threat model in one
place; [docs/recipes/kubernetes.md](docs/recipes/kubernetes.md) has a real
sidecar manifest.

### Resource behavior

An open session's FD footprint is small and fixed. Disk is the sharper
cost: `Checkout` reuses a checkout that's already clean and current at the
branch's head instead of re-materializing it, so a daemon that keeps
reopening the same untouched branch stays flat. A checkout that *does* get
re-materialized (dirty, stale, or destroyed while an earlier descriptor
still points at it) strands one descriptor — and the disk behind it — for
the life of the daemon process; restarting the daemon reclaims everything.
The tradeoff to know: a clean-and-current checkout is served straight from
disk without consulting the store's chain. Full mechanics and caveats:
[docs/operations.md](docs/operations.md#budgets) and
[docs/status.md](docs/status.md)'s resource-behavior rows.

**Read-only historical checkouts** (`offshoot checkout --at <checkpoint>
--read-only`) live in a separate `checkouts-ro/` tree — one `chmod 0444`
file per `(db, branch, checkpoint)`, no sidecar, no lease, no stranded
descriptor — and **it is safe to `rm -rf` the entire `checkouts-ro`
directory at any time**; the next call rebuilds what it needs from the
store. `offshoot export`'s output has the same
zero-ongoing-relationship property, written wherever you pointed it.

## Integration surface

Four ways to talk to offshoot: the CLI above needs no daemon and no SDK;
everything below is a client of the daemon's lifecycle API and requires
`offshoot serve` already running. A fifth, operator-facing surface rides
alongside without changing any of them: `serve -http ADDR` exposes the
same lifecycle API over HTTP (see
[Metrics, HTTP, and events](#metrics-http-and-events)).

### MCP

`offshoot mcp` speaks the Model Context Protocol on stdio, so an agent can
branch on its own initiative instead of asking you to run commands:

    claude mcp add offshoot -- offshoot -store ./.offshoot mcp

The agent gets seven tools — list, checkout, checkpoint, fork, rollback,
promote, destroy — described so it knows *when* to use them: fork before a
risky migration, checkpoint when tests pass, roll back when they don't,
promote the attempt that worked. See it work end to end:
[docs/demo/mcp-walkthrough.md](docs/demo/mcp-walkthrough.md), a real
captured session.

Destructive tools respect the same protected-branch rules as the CLI: an
agent can fork and experiment freely, but promoting onto or destroying
`main` requires an explicit force, and the refusal tells the agent so.

Agent-created forks expire by default, so an agent that forks and forgets
doesn't leak branches forever: `offshoot_fork` applies `offshoot mcp
-default-ttl` (default `24h`) to any call that omits its own `ttl`; pass
`ttl:"<duration>"` to override, or `ttl:"none"` for a branch that never
expires. The response echoes the TTL applied and the computed expiry, so
both are visible in the agent's transcript. **A TTL alone does not reap
anything** — reaping is the janitor's job (`offshoot serve`), and
`offshoot mcp` runs no daemon of its own; a daemonless MCP setup only
sweeps expired branches when `offshoot gc` is run by hand.

**MCP rides a running daemon when one is up, but only for a branch a
session is already open on.** `offshoot mcp` never opens a session itself —
that's a harness's job (the SDKs, `offshoot session open`, or your own
loop). With an open session on the branch: `offshoot_checkpoint` flushes
live through the daemon (no quiesce), `offshoot_fork` forks through the
daemon (flushing first, so an unflushed write always lands in the child),
and `offshoot_checkout` returns the session's live checkout path. Without
one, every tool runs exactly as it does with no daemon at all.
`offshoot_rollback`, `offshoot_promote` (checked against its `target`),
and `offshoot_destroy` take the opposite stance: each **refuses outright —
even with `force`** — whenever the daemon has any session open on the
affected branch, because all three repoint or delete a ref out from under
a session the daemon still owns; close the session first and retry
(`offshoot_promote`'s `source` is the one exception — an open session
there doesn't block, but the promoted state is the last-flushed head, not
unflushed writes). Details:
[docs/reference.md](docs/reference.md). **In short: the good path for
`offshoot mcp` is a harness-opened session, opened before the agent's
tool calls.**

### Python SDK

`sdk/python` is a stdlib-only, thin client over the daemon's lifecycle
API — it never opens SQLite itself and can't do anything the CLI can't; it
just lets your process drive a running daemon instead of shelling out. Not
yet published to PyPI — import it from a checkout of this repo:

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

`Client` also exposes `branches()`, `dbs()`, `export()`, and
`checkout_at()` (a read-only historical checkout).

**Testing with pytest?** `pip install "offshoot-db[pytest]"` registers
`offshoot_daemon`/`offshoot_db`/`offshoot_fork` fixtures automatically —
seed once, fork a fresh isolated branch per test, TTL-backstopped cleanup,
`pytest-xdist` parallelism (one daemon per worker). Full tutorial:
[docs/eval-harness.md](docs/eval-harness.md); condensed reference:
`sdk/python/README.md`.

### TypeScript SDK

`sdk/typescript` is the same thin client, zero runtime dependencies. Also
not yet published to npm — build and import it from a checkout of this
repo:

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
counterpart of the pytest fixtures above. See
[docs/eval-harness.md](docs/eval-harness.md)'s TypeScript section and
`sdk/typescript/README.md`.

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
([openai-agents.md](docs/recipes/openai-agents.md)), and short honest
notes on LlamaIndex and CrewAI
([frameworks.md](docs/recipes/frameworks.md)).

## Docs

[FAQ](docs/faq.md) (why not Litestream / LiteFS / Turso / Dolt / `cp`) ·
[CLI reference](docs/reference.md) · [architecture](docs/architecture.md) ·
[branch diff](docs/diff.md) ·
[implemented/deferred status](docs/status.md) · [roadmap](ROADMAP.md) ·
[stability contract](docs/stability.md) (pre-1.0 promises, v1.0 criteria) ·
[how offshoot is tested](docs/testing.md) (torture numbers, CI gates) ·
[benchmarks](docs/benchmarks.md) (measured, with method) ·
[CI recipes](docs/ci-recipes.md) (seed-once/fork-per-attempt Actions
workflows) · [Grafana dashboard](docs/grafana-dashboard.json) ·
[**the eval-harness tutorial**](docs/eval-harness.md) (seed-once-fork-many
for pytest/vitest/`node:test`, from install to CI) ·
[framework recipes](docs/recipes/) (Claude Code hooks, OpenAI Agents SDK,
LlamaIndex/CrewAI) ·
[**operations**](docs/operations.md) (metrics, branch states, eventing,
budgets, HTTP/auth threat model — single node, see that page's first
paragraph) · [Kubernetes sidecar recipe](docs/recipes/kubernetes.md)

## Contributing and license

[CONTRIBUTING.md](CONTRIBUTING.md) has dev setup and the test tiers,
including `make ci-local` (mirrors CI's job matrix locally — fast
pre-merge signal, not a substitute for the real CI gate);
[SECURITY.md](SECURITY.md) covers vulnerability reporting;
[docs/operations.md](docs/operations.md) documents the daemon's threat
model. Release notes live in [CHANGELOG.md](CHANGELOG.md). License:
[Apache-2.0](LICENSE).
