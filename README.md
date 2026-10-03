<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
  <img src="docs/assets/logo-light.svg" alt="offshoot" width="340">
</picture>

**Branch SQLite like git** — fork-per-attempt databases for AI agents, eval harnesses and tests.<br>Create, fork, checkpoint, rollback, promote — as stock SQLite files, on your disk or your S3 bucket, with one binary.

[![release](https://img.shields.io/github/v/release/sricola/offshoot?style=flat-square&labelColor=1b1a17&color=3c7a1a)](https://github.com/sricola/offshoot/releases) [![ci](https://img.shields.io/github/actions/workflow/status/sricola/offshoot/ci.yml?branch=main&style=flat-square&labelColor=1b1a17&color=3c7a1a)](https://github.com/sricola/offshoot/actions/workflows/ci.yml) [![license](https://img.shields.io/badge/license-Apache--2.0-3c7a1a?style=flat-square&labelColor=1b1a17)](LICENSE) [![docs](https://img.shields.io/badge/docs-sricola.github.io%2Foffshoot-3c7a1a?style=flat-square&labelColor=1b1a17)](https://sricola.github.io/offshoot/docs/) [![openssf scorecard](https://img.shields.io/ossf-scorecard/github.com/sricola/offshoot?label=openssf%20scorecard&style=flat-square&labelColor=1b1a17&color=3c7a1a)](https://scorecard.dev/viewer/?uri=github.com/sricola/offshoot)

[Install](#install) · [Quickstart](#quickstart) · [Why not X](#why-not-cp-litestream-litefs-turso-dolt-or-neon) · [For agents](#for-ai-agents-mcp) · [For tests and evals](#for-tests-and-eval-harnesses) · [How it works](#how-it-works) · [Limits](#what-it-does-not-do) · [Docs](#docs)

`v0.2.16` pre-1.0 · `377 B` per shared fork of a 100 MB database · every checkout a stock `.db` file · writer `kill -9` tested

</div>

---

An agent attempt, an eval run, or a test needs a real database it can
trash. Mocks aren't real, re-seeding is slow, and container or VM
snapshots version a whole machine to get at one file. offshoot branches
the database itself: copy-on-write forks of ordinary SQLite files, stored
in a local directory or an S3-compatible bucket.

```text
              fork ┌─ attempt-1 ●──✗            expires (TTL)
                   │
  main ●───────●───┼─ attempt-2 ●──●──✓   ──►   promote: main repoints here
       seed    cp  │
                   └─ attempt-3 ●──✗            expires (TTL)
```

Each attempt gets its own `.db` file that any SQLite tool opens. A fork of
a 100 MB database adds [377 bytes](docs/benchmarks.md#added-object-store-bytes-per-fork-100-mb-database)
to the store, about 280,000× less than a copy, and a diverging fork pays
only for the pages it changes. Run N migrations or N agent attempts on N
forks, `promote` the one that worked, and let the rest expire.

There is no merge and no conflict resolution: the winner is promoted
whole and the losers reap themselves. That is a design position
([what it does not do](#what-it-does-not-do)), not a gap.

## Install

```sh
brew tap sricola/offshoot https://github.com/sricola/offshoot && brew trust sricola/offshoot && brew install offshoot
```

| Channel | Command |
|---|---|
| **Homebrew** (macOS, Linux) | the line above; recent Homebrew requires `brew trust` for third-party taps. Formula: [`Formula/offshoot.rb`](Formula/offshoot.rb) |
| **Prebuilt binaries** | `offshoot_vX.Y.Z_{linux,darwin}_{amd64,arm64}.tar.gz` on the [releases page](https://github.com/sricola/offshoot/releases); signed with cosign and attested with SLSA provenance since v0.2.11. [How to verify](https://sricola.github.io/offshoot/docs/installation/#verify-what-you-downloaded) |
| **Docker** | `docker run --rm -v offshoot-data:/data ghcr.io/sricola/offshoot:latest init` — multi-arch images on GHCR per release; keep the store in the `/data` volume |
| **`go install`** | `go install github.com/sricola/offshoot/cmd/offshoot@latest` (Go 1.26+, cgo) |
| **From source** | `git clone https://github.com/sricola/offshoot && cd offshoot && go build -o offshoot ./cmd/offshoot` |

**Platforms:** Linux and macOS, amd64 and arm64. On Windows use WSL2; the
Linux binaries, the Docker image and the source build all work there.
Native Windows is unsupported ([why](docs/faq.md#why-no-windows-support)).

Store setup, S3 configuration and the fail-closed probe:
[installation guide](docs/installation.md).

## Quickstart

Sixty seconds, no server, no bucket. Needs the `sqlite3` CLI.

    offshoot init
    offshoot create app
    sqlite3 "$(offshoot checkout app)" "CREATE TABLE users (name); INSERT INTO users VALUES ('ada');"
    offshoot checkpoint app v1
    offshoot fork app attempt-1                                       # instant, copy-on-write
    sqlite3 "$(offshoot checkout app@attempt-1)" "DELETE FROM users;"   # destructive, on the fork
    sqlite3 "$(offshoot checkout app)" "SELECT * FROM users;"           # main still says: ada
    offshoot rollback app@attempt-1 --to fork                           # undo the fork's work
    offshoot promote app@attempt-1 --onto main --force                  # ...or ship it
    offshoot status

That is most of the surface. Every command and flag: [CLI reference](docs/reference.md).

| Command | What it does |
|---|---|
| `create` / `checkout` | new database / materialize a working copy and print its path, a plain `.db` file |
| `checkpoint` | name the checkout's current state as a point you can fork from or roll back to |
| `fork` | branch from the head or a checkpoint: instant, copy-on-write, optional `--ttl` |
| `rollback` / `promote` | repoint a branch at a checkpoint / repoint a target at a branch's head. Each keeps the old head as a TTL'd safety fork (`<branch>-pre-rollback`, `<target>-pre-promote`, 24h by default) so it can be undone |
| `protect` / `unprotect` | refuse unforced `destroy` and `promote --onto`, and never reap. `main` is protected by default; an MCP agent cannot force past it |
| `diff` / `export` | content-aware per-table summary (or `sqldiff`) between two branches or checkpoints / copy any state out to a plain file with no further relationship to the store |
| `destroy` / `gc` | delete a branch / collect unreachable objects |
| `serve` / `session` | the daemon: leases, live capture of every committed transaction, flush without pausing the writer ([below](#the-daemon-live-capture)) |
| `mcp` | the same verbs as MCP tools ([below](#for-ai-agents-mcp)) |

Two things that surprise people: checkpoints belong to one branch and are
not inherited by its forks (start a fork from a parent's checkpoint with
`fork --at`), and `promote` replaces the target's checkpoint list with the
source's. Walkthrough with explanations: [quickstart](docs/quickstart.md).

## Why not `cp`, Litestream, LiteFS, Turso, Dolt, or Neon?

| Instead of | The difference |
|---|---|
| **`cp database.db`** | A copy costs the whole file per attempt and gives you no history, no expiry, no atomic promote, and nothing stopping two attempts from writing the same file. offshoot shares unchanged pages, names states, reaps forks on a TTL, and promotes with one compare-and-swap. On a filesystem that can reflink, it clones checkouts too. [more](docs/faq.md#why-not-just-cp) |
| **Litestream** | Streams one database's WAL to object storage for backup and restore. No branches, no forks. offshoot captures the same way but stores branches you can fork from, roll back and promote. [more](docs/faq.md#why-not-litestream) |
| **LiteFS** | Replicates one SQLite database across nodes behind a FUSE filesystem, for availability. Not branching. offshoot is single-node and branches instead of replicating. [more](docs/faq.md#why-not-litefs) |
| **Turso / libSQL** | Turso Cloud has native branching as a managed service, on libSQL (a SQLite fork). offshoot is the self-hosted version of that one feature: stock SQLite, one binary, a directory or bucket you already control. [more](docs/faq.md#why-not-turso) |
| **Dolt** | A version-controlled SQL database with row-level merge, and its own engine. offshoot deliberately has no merge; if you need to merge two attempts' rows, Dolt is built for that. [more](docs/faq.md#why-not-dolt) |
| **Neon-style branching** | Database branching as a managed Postgres service. offshoot gives the same fork-and-promote workflow to a SQLite file on your disk or bucket, with no service. [more](docs/faq.md#why-not-neon-style-branching) |

The shape offshoot optimizes for is **fork many → mutate independently →
keep or promote one → discard the rest**. It is not replication,
distributed SQLite, multi-writer, or general database version control.

## What you get

- **Copy-on-write forks, measured.** A shared fork writes two tiny objects:
  377 B for a 100 MB database, flat from 1 to 100 forks. Forking a named
  checkpoint takes about 9–10 ms whether the database is 12 MB or 1 GB. A
  diverging fork pays about 761 B per single-row transaction. Numbers,
  method, hardware and caveats: [benchmarks](docs/benchmarks.md#copy-on-write-fork-cost-v02x).
- **Stock everything.** A checkout *is* a SQLite file. `offshoot export`
  materializes any branch or checkpoint to a plain `.db` with no ongoing
  relationship to the store. The store itself holds ordinary LTX objects
  that the upstream `ltx` tool can replay without offshoot
  ([recovering without offshoot](docs/operations.md)).
- **Safety on compare-and-swap, not on luck.** Every branch update is a
  conditional write with exactly one winner. Each store is probed at
  attach time and refused if conditional writes are not enforced. Writers
  hold leases with epochs; a writer that loses its lease is fenced and
  cannot corrupt the branch. [How it is tested](docs/testing.md).
- **Crash-tested capture.** The torture harness `SIGKILL`s a stock
  `sqlite3` writer mid-transaction on roughly half of thousands of rounds
  and requires the replica to converge byte-for-byte every time; it runs
  nightly. What it does and does not cover is in
  [durability](#durability-what-is-proven).
- **Agent-native.** MCP tools described so an agent forks before risky
  work and promotes what passed; forks that expire by default; pytest
  fixtures and a vitest/jest testkit for fork-per-test isolation; a
  LangGraph companion and framework recipes.
- **Nothing silent.** Every store records a layout version, and a binary
  that does not understand a store refuses it rather than guessing.
  Releases are signed and carry SLSA provenance and an SBOM.

## What it does not do

- **No row-level merge.** Promote the winner whole; let the losers expire.
  ([why](docs/faq.md#can-i-merge-two-branches))
- **One writer per branch.** Exactly one leased, epoch-fenced writer per
  lineage. Two agents writing "at once" get two forks and a promote.
  ([why](docs/faq.md#why-one-writer-per-branch))
- **One daemon per store, one host per checkout.** Two daemons sharing a
  bucket are not supported; the daemon and the writer must share a kernel
  and a filesystem, because the checkout is a real file both open.
  ([limitations](docs/limitations.md#one-daemon-per-store))
- **No managed service, no cluster.** Your bucket, your binary, Apache-2.0.
  Replication and failover are out of scope.
- **Linux and macOS only.** WSL2 works; native Windows does not.

The full, plainly worded list, with the performance envelope and the
remaining concurrency windows: [limitations](docs/limitations.md).

## Can I leave?

Yes, at any time. Checkouts are plain SQLite files: `cp` one and you are
done. `offshoot export db@branch@checkpoint out.db` writes any historical
state to a plain file. The pre-1.0 [stability contract](docs/stability.md)
promises that any storage-format break ships in the same release with a
migration or a documented export path, never silently: a binary that does
not understand a store's layout version refuses the whole store.

## For AI agents (MCP)

`offshoot mcp` speaks the Model Context Protocol on stdio, so an agent can
branch on its own initiative instead of asking you to run commands:

    claude mcp add offshoot -- offshoot -store ./.offshoot mcp

Claude Code plugin (MCP server, a skill that teaches the loop, advisory hooks):

    claude plugin marketplace add sricola/offshoot
    claude plugin install offshoot@offshoot

Cursor: [![Install in Cursor](https://cursor.com/deeplink/mcp-install-dark.svg)](https://cursor.com/en/install-mcp?name=offshoot&config=eyJjb21tYW5kIjoib2Zmc2hvb3QiLCJhcmdzIjpbIm1jcCJdfQ==)

The agent gets nine tools — list, checkout, checkpoint, fork, rollback,
promote, destroy, touch, diff — described so it knows *when* to use them:
fork before a risky migration, checkpoint when tests pass, roll back when
they don't, diff two attempts, promote the one that worked. The loop it
is taught:

```text
    checkpoint
         |
       fork
      / | \
     A  B  C
     x  ✓  x
        |
      promote
```

Guardrails are set by the human running the server, not by the agent:
`main` is protected and an agent's `force` is refused unless the server
was started with `-allow-force`; agent-created forks expire after 24h by
default (`-default-ttl`); promote and rollback always keep a safety fork;
every refusal tells the agent what to do instead. A real captured session:
[MCP walkthrough](docs/demo/mcp-walkthrough.md). Details, hooks pattern,
and recipes for the OpenAI Agents SDK, LlamaIndex and CrewAI:
[agents guide](docs/agents.md), [recipes](docs/recipes/).

The same primitive stands on its own without an agent: migration dry
runs, speculative schema changes, retry loops, anything that needs a
database it can afford to lose.

## For tests and eval harnesses

Seed once, fork per test, assert, let the forks expire. Python:

```python
# pip install "offshoot-db[pytest] @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python"
def test_checkout_flow(offshoot_fork):          # a fresh branch per test, from a shared seed
    conn = sqlite3.connect(offshoot_fork.path)  # a plain SQLite file
    conn.execute("DELETE FROM orders")          # destroy it freely
```

TypeScript (`sdk/typescript`, zero runtime dependencies, built from this
repo) ships the same as a framework-agnostic testkit:
`startDaemon` / `seedOnce` / `forkPerTest` / `dump`. Both work with
`pytest-xdist` and vitest/jest parallelism, one daemon per worker.

Neither SDK is published to PyPI or npm yet; both install from this
repository as shown in their READMEs: [Python](sdk/python/README.md),
[TypeScript](sdk/typescript/README.md). The tutorial, including golden-file
assertions and a CI recipe: [eval-harness guide](docs/eval-harness.md).
For pass^k evals (tau2-bench, Inspect AI, promptfoo), the
fork-per-attempt / diff-per-attempt pattern and a runnable example:
[eval recipes](docs/recipes/eval-harnesses.md), [`examples/eval-pass-k/`](examples/eval-pass-k/).

A runnable demo of the whole story: [`examples/parallel-attempts/run.sh`](examples/parallel-attempts/)
forks a database three ways, races three migrations, promotes the correct
one and discards the rest, in about three seconds. A transcript and an
asciinema recording of a real run are in [`docs/demo/`](docs/demo/).

## The daemon: live capture

At rest, every command opens the store, does its work and exits, so a
checkpoint has to quiesce the database first. The daemon removes that
constraint: it holds the branch under a lease and captures every committed
transaction while your process keeps writing.

    offshoot serve &                       # holds leases, captures continuously
    P=$(offshoot session open app)         # the checkout path
    sqlite3 "$P" "CREATE TABLE t (v); INSERT INTO t VALUES ('agent wrote this');"
    offshoot session flush app v1          # durable in the store; the writer never paused
    offshoot session status                # durable txid per session
    offshoot session close app

**Durability is explicit and reported.** Between flushes, writes are
committed to SQLite but not yet in the store. `-flush-every` (default
30s) bounds how much committed-but-unflushed work a dying daemon can
lose; `session status` reports the txid each session is durable through.
Flushes write only the pages that changed, with a full snapshot every
16th flush so reads stay bounded. The SDKs, the MCP server and the HTTP
API are all clients of this daemon. Flags, metrics, events, branch
states, the HTTP threat model and a Kubernetes sidecar recipe:
[operations](docs/operations.md).

## Storage

    offshoot -store ./.offshoot init                 # local directory (default)
    offshoot -store s3://my-bucket/offshoot init     # S3-compatible bucket

Checkouts are always local SQLite files; the store holds snapshots,
segments and refs. For `s3://` specs, credentials come from the AWS SDK
default chain; `OFFSHOOT_S3_ENDPOINT`, `OFFSHOOT_S3_REGION` and
`OFFSHOOT_S3_PATH_STYLE` configure compatible endpoints.

A provider is listed as supported only after the conformance suite and the
conditional-write probe pass against it for real:

| Provider | Status |
|---|---|
| AWS S3 | verified: probe, conformance and multipart against a real bucket, nightly in CI |
| RustFS | verified on every pull request and push (`rustfs/rustfs:1.0.0`, digest-pinned) |
| MinIO | verified through v0.2.9; no longer in CI since MinIO withdrew its community images. The code path is unchanged |
| Google Cloud Storage (S3 interop) | **unsupported**: no conditional writes on its S3 API; the probe refuses it ([why](docs/faq.md#why-no-google-cloud-storage)) |

## How it works

- A **branch** is a ref: a small JSON document naming a **lineage** and a
  head transaction id, updated only by compare-and-swap.
- A lineage's history is a chain of **LTX objects**: full snapshots plus
  segments carrying only the changed pages. Reads replay one snapshot and
  at most fifteen segments.
- A **fork** writes a base pointer to its parent's chain instead of
  copying it; it writes new objects only as it diverges. `promote` and
  `rollback` repoint refs the same way; `compact` makes a branch
  self-contained on demand.
- A **checkout** is the materialized file; a sidecar records which state
  it embodies, so a clean checkout is reused without re-materializing.
  Where the filesystem can clone (APFS, and XFS or btrfs with reflinks;
  both are exercised in CI), checkouts are cloned from a cache.
- The **daemon** reads the checkout's WAL frames to capture transactions,
  keeps a replica, and flushes segments under its lease's epoch.
- Every store carries a **layout version**; an older binary refuses a
  newer store.

Longer: [core concepts (glossary)](docs/concepts.md), [architecture](docs/architecture.md).

## Durability: what is proven

- **Proven by test, nightly:** a stock `sqlite3` writer `SIGKILL`ed
  mid-transaction never diverges the capture replica; every object read
  from the store is checksum-verified (CRC64, per-page and rolling
  checksums) and a corrupt object fails closed; a crash at any point in
  checkpoint, flush, fork, rollback, promote or compact leaves at most an
  unreferenced object, because the ref update that commits is always
  last (an at-rest checkpoint also takes the branch lease first, and a
  crash leaves that lease on the branch until it lapses after 30 s or
  `offshoot lease release` frees it); a fenced writer cannot advance a
  ref; fuzzing covers the decoder, the sidecar and the wire protocol.
- **Also proven by test, nightly:** the capture engine itself survives
  `SIGKILL`. A second harness runs the capturer in a child process, kills
  it with `SIGKILL` mid-traffic every round, restarts it on the same state
  and replica, and requires the replica to match the source after a
  graceful drain; every restart after a kill takes the rebase path, and
  none has diverged.
- **Designed for, not torture-tested:** every rename into place is
  followed by a directory fsync, so a flushed state on a local store is
  meant to survive power loss, not only process death. No harness cuts
  power.
- **Explicitly at risk:** committed-but-unflushed writes in a daemon
  session, bounded by `-flush-every`.

[How offshoot is tested](docs/testing.md) names each harness, and
[limitations](docs/limitations.md) states the remaining windows plainly.

## Status

**v0.2.16, pre-1.0.** What is shipped and exercised by tests that would
fail if it broke:

- local and S3-compatible stores behind a shared conformance suite
- copy-on-write forks; checkpoint, rollback, promote, compact, export, diff
- live WAL capture with incremental segments
- leases with epoch fencing, and CAS on every ref update
- TTL reaping and reachability GC
- the daemon, with metrics, events and an HTTP API; the MCP server
- Python and TypeScript SDKs with test fixtures; a LangGraph checkpointer

The CLI surface and the storage format may still change before 1.0, never
silently ([stability contract](docs/stability.md)). The per-feature
accounting of what is tested versus merely shipped: [status](docs/status.md).
Where it is going: [roadmap](ROADMAP.md) · [changelog](CHANGELOG.md).

## Docs

Rendered: **<https://sricola.github.io/offshoot/docs/>**

**Understand it** — [introduction](docs/introduction.md) · [core concepts (glossary)](docs/concepts.md) · [architecture](docs/architecture.md) · [FAQ](docs/faq.md) · [stability contract](docs/stability.md) · [how it is tested](docs/testing.md) · [benchmarks](docs/benchmarks.md)

**Use it** — [installation](docs/installation.md) · [quickstart](docs/quickstart.md) · [CLI reference](docs/reference.md) · [agents guide](docs/agents.md) · [eval-harness guide](docs/eval-harness.md) · [CI recipes](docs/ci-recipes.md) · [framework recipes](docs/recipes/) · [branch diff](docs/diff.md)

**Operate it** — [operations](docs/operations.md) · [Grafana dashboard](docs/grafana-dashboard.json) · [Kubernetes sidecar](docs/recipes/kubernetes.md) · [limitations](docs/limitations.md)

## Contributing, security, license

- [CONTRIBUTING.md](CONTRIBUTING.md): `git clone`, `make test`, the test
  tiers, and `make ci-local` to mirror CI.
- [SECURITY.md](SECURITY.md): how to report a vulnerability, and the threat
  model of what is actually enforced.
- [GOVERNANCE.md](GOVERNANCE.md): a single maintainer today, and how
  decisions on the storage format are made.
- License: [Apache-2.0](LICENSE).

---

<div align="center">

**⑂** &nbsp;fork it, trash it, promote the one that worked

[Apache-2.0](LICENSE) · [sricola.github.io/offshoot](https://sricola.github.io/offshoot/) · [releases](https://github.com/sricola/offshoot/releases)

</div>
