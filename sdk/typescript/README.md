# @offshoot-db/client

TypeScript client for [offshoot](https://github.com/offshoot-db/offshoot):
git-like branching for SQLite databases, built for agent workloads (fork a
DB per attempt, checkpoint on success, roll back on failure).

Zero runtime dependencies, thin wrapper over the `offshoot` daemon's
unix-socket lifecycle API — it never opens SQLite itself, so it can't do
anything the `offshoot` CLI can't.

## Install

```
npm install @offshoot-db/client
```

## Quickstart

Start a store and a daemon (the `offshoot` binary — see the main repo's
[README](https://github.com/offshoot-db/offshoot#readme) for install
options):

```
offshoot -store ./.offshoot init
offshoot serve -socket /tmp/o.sock &
```

Then drive it from TypeScript:

```ts
import { connect } from "@offshoot-db/client";

const c = await connect("/tmp/o.sock");
await c.create("app");
const s = await c.open("app");     // sqlite3 s.path; write; commit
await s.flush("v1");               // durable in the store, writer never paused
await c.fork("app", "main", "try", { ttl: "2h" });
await s.close();
```

`Client` also exposes `branches()` (per-branch head txid, protected flag,
checkpoints, TTL) and `dbs()` (every database name in the store).

## Links

- [Full docs, CLI reference, architecture](https://github.com/offshoot-db/offshoot)
- [Changelog](https://github.com/offshoot-db/offshoot/blob/main/CHANGELOG.md)
- [Issues](https://github.com/offshoot-db/offshoot/issues)

Apache-2.0.
