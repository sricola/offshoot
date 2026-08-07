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

## Eventing: `Client.events()`

The daemon (`offshoot serve`) publishes one versioned JSON event per state
transition — `session_opened`, `flushed`, `flush_failed`, `fenced`,
`session_closed`, `reaped`, `evicted` (reserved), and the terminal
`dropped_slow_consumer`. `Client.events()` is a thin async iterator over
it:

```ts
const c = await connect("/tmp/o.sock");
for await (const ev of c.events()) {
  console.log(ev.type, ev.db, ev.branch, ev.detail);
}
```

**Dedicated connection:** `events()` opens its own fresh unix-socket
connection — never `c`'s own connection. The daemon's `subscribe` op
permanently takes a connection out of request/response mode the instant
it acks (see the main repo's
[docs/reference.md](https://github.com/offshoot-db/offshoot/blob/main/docs/reference.md#eventing-subscribe-op--get-events)),
so this method handles opening (and closing) that dedicated connection
for you — keep using `c` for ordinary `open`/`flush`/... calls exactly as
before.

`events()` returns an `AsyncGenerator<OffshootEvent>` yielding events in
publish order, forever, until the connection ends. `break` out of the
`for await` loop (or call `.return()` on the iterator) to close the
dedicated socket early — no file descriptor is leaked. If this consumer
ever falls behind the daemon's bounded per-subscriber buffer, the daemon
drops it: the terminal `dropped_slow_consumer` event is yielded like any
other event and the iterator then simply ends (not thrown as an error) —
check `ev.type` on the last event you saw if you care whether that
happened.

## testkit: seed-once-fork-many for vitest/jest/node:test

```
import { startDaemon, seedOnce, forkPerTest, dump } from "@offshoot-db/client/testkit";
```

The vitest/jest counterpart of the Python SDK's pytest fixture plugin —
framework-agnostic **functions**, not fixtures. Nothing registers itself
automatically and there's no vitest/jest runtime dependency (this package
stays zero-runtime-deps); wire these into your own `beforeAll`/`afterEach`:

```ts
import { startDaemon, seedOnce, forkPerTest, type DaemonHandle, type SeedHandle, type ForkedSession } from "@offshoot-db/client/testkit";

let daemon: DaemonHandle;
let seed: SeedHandle;
let fork: ForkedSession;

beforeAll(async () => {
  daemon = await startDaemon();                 // OFFSHOOT_BIN env, else PATH
  seed = await seedOnce(daemon, {
    name: "widgets",
    seed: "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);",
  });
});

afterAll(() => daemon.stop());

beforeEach(async () => {
  fork = await forkPerTest(daemon, seed);        // fresh, isolated branch, TTL 1h
});

afterEach(() => fork.close());                   // closes + destroys; warns, never throws

test("agent run against a private copy", async () => {
  // sqlite3 fork.path; write; commit — exercise your code against it
});
```

- `startDaemon(opts?) -> Promise<DaemonHandle>`: locates the `offshoot`
  binary (`OFFSHOOT_BIN` env, else `PATH` — a clear error naming both when
  neither has one), starts it on a fresh temp store + socket. Returns
  `{ sock, store, proc, stderrTail(), stop() }` — the caller stops it.
- `seedOnce(daemon, {name?, seed}) -> Promise<SeedHandle>`: a named-seed
  memoization cache keyed on `(daemon, name)`. `seed` is a SQL string, a
  path to a `.sql` file, or an async `(dbPath) => void` callback given the
  writable sqlite path. The first call for a name creates database
  `eval-{name}`, runs the seed, and checkpoints it `seed`; a later call for
  the same name with a *different* seed throws a clear error instead of
  silently keeping the first one. A `sqlite3 <file> .dump`'s text works
  verbatim as a seed.
- `forkPerTest(daemon, seedHandleOrName, opts?) -> Promise<ForkedSession>`:
  forks a fresh, worker-safe-named branch (uses `VITEST_POOL_ID` or
  `JEST_WORKER_ID` when present) from the seed's checkpoint with a TTL
  (default `"1h"`, `opts.ttl` overrides), opens a session, and returns
  `{ path, db, branch, client, flush(name?), close() }`.
  `seedHandleOrName` may be the handle `seedOnce` returned, or a plain
  string naming an already-`seedOnce`d name. `close()` closes the session
  and destroys the branch — call it from `afterEach`; a failure in either
  step is a `console.warn`, never a thrown error (the TTL is the backstop).
- `dump(path) -> Promise<string>`: `sqlite3 <path> .dump`'s text — THE way
  to compare two offshoot-materialized SQLite files in a golden-file test.
  **Never byte-compare two SQLite files**; SQLite's on-disk bytes aren't
  deterministic for identical logical content. Compare
  `(await dump(a)) === (await dump(b))` instead.

Like the rest of this package, seeding and `dump` shell out to the
`sqlite3` CLI rather than bundling a driver — install it the same way
`test/client.test.ts` expects it (`apt-get install sqlite3` /
`brew install sqlite3`).

## Links

- [Full docs, CLI reference, architecture](https://github.com/offshoot-db/offshoot)
- [Changelog](https://github.com/offshoot-db/offshoot/blob/main/CHANGELOG.md)
- [Issues](https://github.com/offshoot-db/offshoot/issues)

Apache-2.0.
