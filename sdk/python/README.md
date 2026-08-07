# offshoot-db

Python client for [offshoot](https://github.com/offshoot-db/offshoot):
git-like branching for SQLite databases, built for agent workloads (fork a
DB per attempt, checkpoint on success, roll back on failure).

Stdlib-only, thin wrapper over the `offshoot` daemon's unix-socket lifecycle
API — it never opens SQLite itself, so it can't do anything the `offshoot`
CLI can't. `pip install offshoot-db` pulls in no third-party dependencies.

## Install

```
pip install offshoot-db
```

## Quickstart

Start a store and a daemon (the `offshoot` binary — see the main repo's
[README](https://github.com/offshoot-db/offshoot#readme) for install
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

## pytest fixture plugin

```
pip install "offshoot-db[pytest]"
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
  with install instructions, it doesn't fail the run.
- `offshoot_db` (session-scoped): `offshoot_db(name="default", seed=None)`
  — a named-seed factory. The first call for a name creates `eval-{name}`,
  runs `seed` (a callable given a writable sqlite path, or a SQL string),
  and checkpoints it as `seed`; later calls for the same name are a pure
  memoization hit. `seed=None` falls back to the `offshoot_seed` ini option
  (a path to a `.sql` file) — set that once and every test's default
  `offshoot_fork()` call needs no seed code at all.
- `offshoot_fork` (function-scoped): `offshoot_fork(seed_handle=None)` —
  forks a fresh, worker-safe-named branch from a seed checkpoint (TTL: 1h
  default, `offshoot_ttl` ini-overridable), opens a session, and returns an
  object with `.path`/`.client`/`.db`/`.branch`. Teardown closes the session
  and destroys the branch; a destroy failure is a warning, never a test
  failure (the TTL is the backstop).
- `offshoot_dump(path) -> str`: the golden-file comparison helper — `sqlite3
  <path> .dump` text. **Never byte-compare two SQLite files**; SQLite's
  on-disk bytes aren't deterministic for identical logical content. Compare
  `offshoot_dump(a) == offshoot_dump(b)` instead.

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

## Links

- [Full docs, CLI reference, architecture](https://github.com/offshoot-db/offshoot)
- [Changelog](https://github.com/offshoot-db/offshoot/blob/main/CHANGELOG.md)
- [Issues](https://github.com/offshoot-db/offshoot/issues)

Apache-2.0.
