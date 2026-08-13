# offshoot-db

Python client for [offshoot](https://github.com/sricola/offshoot):
git-like branching for SQLite databases, built for agent workloads (fork a
DB per attempt, checkpoint on success, roll back on failure).

Stdlib-only, thin wrapper over the `offshoot` daemon's unix-socket lifecycle
API — it never opens SQLite itself, so it can't do anything the `offshoot`
CLI can't. the package pulls in no third-party dependencies.

## Install

```
# not yet on PyPI — install from the repo:
pip install "offshoot-db @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python"
```

## Quickstart

Start a store and a daemon (the `offshoot` binary — see the main repo's
[README](https://github.com/sricola/offshoot#readme) for install
options):

```
offshoot -store ./.offshoot init
offshoot serve -socket /tmp/o.sock &
```

Then drive it from Python:

```python
import offshoot

with offshoot.connect("/tmp/o.sock") as c:
    c.create("app")
    s = c.open("app")              # sqlite3.connect(s.path); write; commit
    s.flush("v1")                  # durable in the store, writer never paused
    c.fork("app", "main", "try", ttl="2h")
    s.close()
```

`Client` also exposes `branches()` (per-branch head txid, protected flag,
checkpoints, TTL) and `dbs()` (every database name in the store).

## Eventing: `Client.events()`

The daemon (`offshoot serve`) publishes one versioned JSON event per state
transition — `session_opened`, `flushed`, `flush_failed`, `fenced`,
`session_closed`, `reaped`, `evicted` (reserved), and the terminal
`dropped_slow_consumer`. `Client.events()` is a thin generator over it:

```python
with offshoot.connect("/tmp/o.sock") as c:
    for ev in c.events():
        print(ev.type, ev.db, ev.branch, ev.detail)
```

**Dedicated connection:** `events()` opens its own fresh unix-socket
connection — never `c`'s own connection. The daemon's `subscribe` op
permanently takes a connection out of request/response mode the instant
it acks (see the main repo's
[docs/reference.md](https://github.com/sricola/offshoot/blob/main/docs/reference.md#eventing-subscribe-op--get-events)),
so this method handles opening (and closing) that dedicated connection for
you — keep using `c` for ordinary `open`/`flush`/... calls exactly as
before.

`events()` is lazy (nothing is opened until you start iterating) and
yields `Event(v, ts, type, db, branch, detail)` dataclass instances in
publish order, forever, until the connection ends. Stop early with
`break` (or call `.close()` on the generator) to close the dedicated
socket — no file descriptor is leaked. If this consumer ever falls behind
the daemon's bounded per-subscriber buffer, the daemon drops it: the
terminal `dropped_slow_consumer` event is yielded like any other event and
the generator then simply stops (not raised as an error) — check
`ev.type` on the last event you saw if you care whether that happened.

## pytest fixture plugin

**This section is the condensed reference.** For the full tutorial —
install, the named-seed factory, fork-per-test, `pytest-xdist` parallelism
with the measured per-worker cost, mid-test flush checkpoints, golden
assertions via `offshoot_dump` (never byte-compare, and why), export for
handoff/debug, `offshoot diff` for the failed-vs-passed loop, TTL hygiene,
and a CI recipe — see
[docs/eval-harness.md](https://github.com/sricola/offshoot/blob/main/docs/eval-harness.md)
in the main repo.

```
pip install "offshoot-db[pytest] @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python"
```

registers `offshoot_daemon`, `offshoot_db`, and `offshoot_fork` with pytest
automatically (a `pytest11` entry point — nothing to import). The paved
road for seed-once-fork-many eval and test workloads:

```python
# conftest.py or pytest.ini: the zero-code default seed
# [pytest]
# offshoot_seed = tests/seed.sql

def test_agent_run(offshoot_fork):
    forked = offshoot_fork()          # forks from the seeded checkpoint
    conn = sqlite3.connect(forked.path)
    ...                                # exercise the agent against a
                                        # private, isolated copy
```

- `offshoot_daemon` (session-scoped): finds the `offshoot` binary
  (`OFFSHOOT_BIN` env, else `PATH`), starts it on a fresh temp store, and
  tears it down at session end. No binary found → the fixture `pytest.skip`s
  with install instructions, it doesn't fail the run (set ini
  `offshoot_require_binary = true` to make that a hard failure instead —
  CI's "don't silently skip everything" strict mode). `OFFSHOOT_BIN` set
  but pointing at something missing or not a file (e.g. a directory) is
  always a hard failure, regardless of that option — a typo in an explicit
  path is a misconfiguration, not "this machine doesn't have offshoot".
- `offshoot_db` (session-scoped): `offshoot_db(name="default", seed=None)`
  — a named-seed factory. The first call for a name creates `eval-{name}`,
  runs `seed` (a callable given a writable sqlite path, or a SQL string —
  a `sqlite3 <file> .dump`'s text works verbatim), and checkpoints it as
  `seed`; later calls for the same name are a pure memoization hit — a
  later call that passes a *different* seed for an already-seeded name
  raises a clear error rather than silently keeping the first one.
  `seed=None` falls back to the `offshoot_seed` ini option (a path to a
  `.sql` file, resolved against pytest's rootdir) — set that once and every
  test's default `offshoot_fork()` call needs no seed code at all.
- `offshoot_fork` (function-scoped): `offshoot_fork(seed_handle=None)` —
  `seed_handle` may be a handle from `offshoot_db`, a plain `str` naming an
  already-seeded name (`offshoot_fork("special")`), or `None` for the
  default seed. Forks a fresh, worker-safe-named branch from a seed
  checkpoint (TTL: 1h default, `offshoot_ttl` ini-overridable), opens a
  session, and returns an object with `.path`/`.client`/`.db`/`.branch`
  plus a `.flush(name="", meta=None)` passthrough for a mid-test named
  checkpoint. Teardown closes the session and destroys the branch; a
  destroy (or close) failure is a warning, never a test failure (the TTL is
  the backstop), and one fork's teardown failure never stops the rest of
  that test's forks from being cleaned up too.
- `offshoot_dump(path) -> str`: the golden-file comparison helper — `sqlite3
  <path> .dump` text, also available as a fixture of the same name
  (`def test_x(offshoot_dump): ...`). **Never byte-compare two SQLite
  files**; SQLite's on-disk bytes aren't deterministic for identical
  logical content. Compare `offshoot_dump(a) == offshoot_dump(b)` instead:

  ```python
  from offshoot.pytest_plugin import offshoot_dump

  assert offshoot_dump(golden_path) == offshoot_dump(forked.path)
  ```

**`pytest-xdist`:** this plugin runs one `offshoot` daemon + one temp store
**per worker** (pytest has no fixture-sharing mechanism across worker
processes), so each worker seeds its own copy of every named seed
independently — with N workers, seed cost is paid N times, not once, though
workers seed concurrently rather than serially. Measured on this repo's own
smoke test (a `CREATE TABLE` + 200-row seed, macOS arm64): one worker's full
seed-and-checkpoint step is ~80-90ms; a 2-worker run pays that twice, in
parallel (~170ms of total seed work, ~85-90ms of wall-clock time). If your
seed is expensive, keep worker count modest or seed a shared file
out-of-band and `create --from` it into each worker's store. Full detail —
including why wrapping a multi-statement seed in one transaction matters
(measured ~100x) — is in `offshoot/pytest_plugin.py`'s module docstring.

## LangGraph

Two integrations, two jobs: `offshoot.langgraph.ThreadForks` (in this
package) keeps your existing LangGraph checkpointer and forks only the
*application* database your agent's tools write to, one branch per thread;
[`langgraph-checkpoint-offshoot`](../python-langgraph/README.md) is the
inverse — a `BaseCheckpointSaver` that puts LangGraph's **own thread
state** in an offshoot-managed SQLite database, so threads themselves can
be forked, rolled back, promoted, and TTL-reaped.

## Links

- [Full docs, CLI reference, architecture](https://github.com/sricola/offshoot)
- [Changelog](https://github.com/sricola/offshoot/blob/main/CHANGELOG.md)
- [Issues](https://github.com/sricola/offshoot/issues)

Apache-2.0.
