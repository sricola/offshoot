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

`offshoot-db[pytest]` will add a session-scoped daemon, a seed-once fixture,
and a fork-per-test fixture with TTL cleanup — the paved road for
seed-once-fork-many eval and test workloads. Not yet shipped; tracked in
this repo's `docs/ROADMAP.md` (Milestone 3).

## Links

- [Full docs, CLI reference, architecture](https://github.com/offshoot-db/offshoot)
- [Changelog](https://github.com/offshoot-db/offshoot/blob/main/CHANGELOG.md)
- [Issues](https://github.com/offshoot-db/offshoot/issues)

Apache-2.0.
