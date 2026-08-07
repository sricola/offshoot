// node:test end-to-end suite mirroring sdk/python/tests/test_client.py.
//
// Builds the offshoot binary (or reuses $OFFSHOOT_BIN), starts a daemon on a
// temp store+socket, and drives it through the TypeScript client. Writes and
// reads to checked-out SQLite files go through the `sqlite3` CLI via
// execFileSync since this SDK ships no DB driver. Skips cleanly if `go` or
// `sqlite3` are not on PATH.

import { test, before, after, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer, Socket, type Server } from "node:net";

import { connect, OffshootError, Client, type OffshootEvent, type Session } from "../src/client.js";

// test-dist/test/client.test.js -> repo root is four levels up.
const REPO = join(fileURLToPath(new URL(".", import.meta.url)), "..", "..", "..", "..");

// unref()'d so a pending sleep() timer never by itself keeps the process's
// event loop alive — see src/testkit.ts's identical `sleep` for why: without
// it, DaemonFixture.stop()'s race below can hold the process open for up to
// 10s after the daemon has already exited and stop() has logically finished.
function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms).unref());
}

function hasCmd(cmd: string): boolean {
  try {
    execFileSync(process.platform === "win32" ? "where" : "which", [cmd], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

const canRun = hasCmd("go") && hasCmd("sqlite3");

function buildBinary(dir: string): string {
  const fromEnv = process.env.OFFSHOOT_BIN;
  if (fromEnv) return fromEnv;
  const out = join(dir, "offshoot");
  execFileSync("go", ["build", "-o", out, "./cmd/offshoot"], { cwd: REPO });
  return out;
}

function sqlite3(dbPath: string, sql: string): string {
  return execFileSync("sqlite3", [dbPath, sql]).toString();
}

/** A daemon on a temp store+socket.
 *
 * bin, when given, reuses an already-built binary instead of building a
 * fresh one — used by tests that want a second, independently killable
 * daemon without paying buildBinary's cost again.
 */
class DaemonFixture {
  readonly dir: string;
  readonly bin: string;
  readonly store: string;
  readonly sock: string;
  proc: ChildProcess;
  private readonly stderrChunks: Buffer[] = [];

  private constructor(dir: string, bin: string, proc: ChildProcess) {
    this.dir = dir;
    this.bin = bin;
    this.store = join(dir, "store");
    this.sock = join(dir, "d.sock");
    this.proc = proc;
  }

  static async start(bin?: string): Promise<DaemonFixture> {
    const dir = mkdtempSync(join(tmpdir(), "offshoot-sdk-"));
    const binpath = bin ?? buildBinary(dir);
    const store = join(dir, "store");
    const sock = join(dir, "d.sock");
    // The store must exist before `serve` will open it (ops.Open refuses an
    // uninitialized store — see cmd/offshoot/main.go); mirrors the Python
    // SDK's DaemonFixture.
    execFileSync(binpath, ["-store", store, "init"], { stdio: ["ignore", "ignore", "pipe"] });
    const proc = spawn(binpath, ["-store", store, "serve", "-socket", sock], {
      stdio: ["ignore", "ignore", "pipe"],
    });
    const fx = new DaemonFixture(dir, binpath, proc);
    proc.stderr?.on("data", (c: Buffer) => fx.stderrChunks.push(c));

    const deadline = Date.now() + 10_000;
    while (!existsSync(fx.sock)) {
      if (Date.now() > deadline) {
        throw new Error("daemon did not start: " + Buffer.concat(fx.stderrChunks).toString());
      }
      if (fx.proc.exitCode !== null || fx.proc.signalCode !== null) {
        throw new Error(Buffer.concat(fx.stderrChunks).toString());
      }
      await sleep(50);
    }
    return fx;
  }

  async stop(): Promise<void> {
    if (this.proc.exitCode === null && this.proc.signalCode === null) {
      this.proc.kill();
      await Promise.race([
        new Promise<void>((resolve) => this.proc.once("exit", () => resolve())),
        sleep(10_000),
      ]);
    }
    rmSync(this.dir, { recursive: true, force: true });
  }
}

/** Number of the daemon process's currently-open unix-domain-socket file
 * descriptors (its listening socket plus one per live connection --
 * ordinary Client connections AND subscribe streams alike), via
 * `lsof -p <pid>` filtered to TYPE "unix". `undefined` if `lsof` isn't on
 * PATH (the fd-leak assertion in the events() test below is then skipped
 * -- best effort, not load-bearing on every CI runner).
 *
 * Deliberately filtered to TYPE "unix" rather than a raw total fd count:
 * internal/dbfile's checkout file descriptors are DELIBERATELY NEVER
 * closed for the life of the daemon (see docs/status.md's Resource
 * behavior table) -- opening a session over the course of a test
 * legitimately grows the daemon's REG-file fd count by design, which
 * would make a raw total-fd-count comparison spuriously fail regardless
 * of whether events()'s dedicated socket itself leaked. Counting only
 * "unix" rows isolates exactly the resource events()'s dedicated
 * connection actually holds.
 */
function daemonUnixSocketFdCount(pid: number): number | undefined {
  if (!hasCmd("lsof")) return undefined;
  let out: string;
  try {
    out = execFileSync("lsof", ["-p", String(pid)]).toString();
  } catch {
    return undefined; // lsof exits non-zero on some platforms for a live pid; best effort
  }
  return out.split("\n").filter((line) => line.split(/\s+/).includes("unix")).length;
}

/** Races `p` against a `ms`-long timer, returning a tagged result rather
 * than a bare union so a timeout can never be confused with (or need an
 * unsound cast away from) any real value `p` could resolve to. */
type RaceResult<T> = { timedOut: false; value: T } | { timedOut: true };

function raceWithTimeout<T>(p: Promise<T>, ms: number): Promise<RaceResult<T>> {
  return Promise.race([
    p.then((value): RaceResult<T> => ({ timedOut: false, value })),
    sleep(ms).then((): RaceResult<T> => ({ timedOut: true })),
  ]);
}

/** Repeatedly open db@main on c2 (via a FRESH events() connection each
 * attempt) until the helper observes the resulting session_opened,
 * bounded by a generous overall deadline -- returns the winning
 * session, its still-open async iterator, and the observed event.
 *
 * Why this exists instead of a fixed sleep: Task 4a's daemon subscribes
 * a connection to the bus strictly BEFORE acking "subscribe" (see
 * handleSubscribeOp's doc comment), but events()'s public contract gives
 * an outside caller no way to observe "the ack has been consumed" from
 * outside -- that read is fused with the wait for the first event inside
 * one opaque `.next()` step. A fixed sleep to paper over that (this
 * repo's CI has a documented history of exactly this flake shape -- lost
 * torture logs, a takeover flake, a pytester HOME issue) is exactly the
 * anti-pattern to avoid. Instead: start `it.next()` FIRST (before firing
 * the op that should produce its result), race it against a short
 * per-attempt timeout, and retry against a brand new events() connection
 * if it doesn't arrive in time (the bus never replays, so an open() that
 * fires before the subscription lands is simply never seen and must be
 * redone) -- no guessing, no fixed delay in the common case where the
 * first attempt wins.
 *
 * A timed-out attempt's `it` is abandoned rather than awaited further: a
 * best-effort `it.return?.()` is fired without being awaited (harmless,
 * and may eventually release its dedicated connection once its pending
 * `.next()` call settles on its own), but nothing here blocks waiting
 * for that to happen. Acceptable because this path is only exercised on
 * a genuine ack-vs-transition race, which a 1s per-attempt timeout makes
 * rare in practice; callers that also measure the daemon's fd count take
 * their baseline AFTER any retry churn for exactly this reason (see the
 * break-mid-stream test below).
 */
async function openUntilSubscribed(
  c: Client,
  c2: Client,
  db: string,
  perAttemptMs = 1000,
  overallMs = 15_000,
): Promise<{ session: Session; it: AsyncGenerator<OffshootEvent, void, void>; opened: OffshootEvent }> {
  const deadline = Date.now() + overallMs;
  let s: Session | undefined;
  for (;;) {
    if (Date.now() > deadline) {
      throw new Error("events() never observed session_opened before the retry deadline");
    }
    if (s) await s.close();
    const it = c.events();
    const pending = it.next(); // start reading BEFORE firing the triggering op
    s = await c2.open(db);
    const result = await raceWithTimeout(pending, perAttemptMs);
    if (!result.timedOut && !result.value.done) {
      return { session: s, it, opened: result.value.value };
    }
    if (result.timedOut) {
      void it.return?.().catch(() => {}); // best-effort, not awaited -- see doc comment
    }
  }
}

/** A minimal one-shot fake daemon: accepts exactly one connection on a
 * fresh temp unix socket, discards the `{"op":"subscribe"}` request line
 * it's sent, writes `ack` (default `{ok: true}`) then each of `lines`
 * (newline-terminated, in order), then ends the connection.
 *
 * `closeWithoutAck: true` short-circuits all of that: after reading (and
 * discarding) the request line, it ends the connection immediately,
 * WITHOUT ever writing an ack line -- simulating a daemon that accepted
 * the connection and read the request but crashed/raced-a-shutdown/hit a
 * transport hiccup before acking. `lines`/`ack` are ignored in this mode.
 *
 * Used to drive Client.events()'s decode path end to end -- including the
 * `dropped_slow_consumer` terminal event and the close-before-ack failure
 * mode -- against a scripted server that speaks the exact same
 * line-per-event wire shape the real daemon does, without needing to
 * force a real daemon's buffer-overflow/slow-consumer mechanics
 * deterministically from the SDK side (hard to do reliably; see
 * internal/daemon/events_test.go's
 * TestEventBusDropsSlowSubscriberWithTerminalEvent for that proof against
 * the real bus instead).
 */
async function fakeSubscribeDaemon(
  lines: string[],
  ack: Record<string, unknown> = { ok: true },
  closeWithoutAck = false,
): Promise<{ sockPath: string; close: () => Promise<void> }> {
  const dir = mkdtempSync(join(tmpdir(), "offshoot-fake-daemon-"));
  const sockPath = join(dir, "d.sock");
  const srv: Server = createServer((conn) => {
    conn.setEncoding("utf8");
    let buf = "";
    const onData = (chunk: string) => {
      buf += chunk;
      if (buf.includes("\n")) {
        conn.off("data", onData);
        if (closeWithoutAck) {
          conn.end();
          return;
        }
        conn.write(JSON.stringify(ack) + "\n");
        for (const line of lines) conn.write(line + "\n");
        conn.end();
      }
    };
    conn.on("data", onData);
  });
  await new Promise<void>((res) => srv.listen(sockPath, res));
  return {
    sockPath,
    close: () =>
      new Promise<void>((res) => {
        srv.close(() => res());
        rmSync(dir, { recursive: true, force: true });
      }),
  };
}

/** A Client wired to sockPath with a dummy, never-connected Socket in
 * place of a real request/response connection -- Client.events() only
 * ever reads its own retained socketPath (never `sock`), so this is
 * enough to exercise it standalone against fakeSubscribeDaemon without a
 * real offshoot daemon underneath. */
function fakeEventsClient(sockPath: string): Client {
  return new Client(new Socket(), sockPath);
}

let fixture: DaemonFixture | undefined;

before(async () => {
  if (canRun) fixture = await DaemonFixture.start();
});

after(async () => {
  if (fixture) await fixture.stop();
});

test("full lifecycle: create, open, write, flush, fork, checkout, branches, guards, touch, destroy", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c: Client = await connect(fixture!.sock);
  try {
    await c.create("app");
    const s = await c.open("app");
    sqlite3(s.path, "CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('one');");
    const txid = await s.flush("v1");
    assert.ok(txid > 0);

    // Fork from the LIVE session — the row must be there.
    await c.fork("app", "main", "try", { ttl: "1h" });
    const p = await c.checkout("app", "try");
    const rows = sqlite3(p, "SELECT count(*) FROM t;").trim();
    assert.equal(rows, "1");

    // branches reflects TTL and checkpoints.
    let info = new Map((await c.branches("app")).map((b) => [b.branch, b]));
    assert.ok(info.has("try"));
    assert.ok(info.get("try")!.ttl);
    assert.ok(info.get("main")!.checkpoints.includes("v1"));

    // main is protected by default.
    await assert.rejects(() => c.destroy("app", "main"), OffshootError);

    await c.touch("app", "try", { ttl: "none" });
    info = new Map((await c.branches("app")).map((b) => [b.branch, b]));
    assert.equal(info.get("try")!.ttl, "");

    await s.close();
    await c.destroy("app", "try");
    const remaining = new Set((await c.branches("app")).map((b) => b.branch));
    assert.ok(!remaining.has("try"));
  } finally {
    await c.close();
  }
});

// These two are pure response-parsing tests, no daemon needed:
// Object.create(Client.prototype) builds an instance without running the
// constructor (which requires a real Socket), and `_call` — a public but
// underscore-prefixed method meant only for this package's own client code
// (see its doc comment in src/client.ts) — is overridden per-instance to
// return a canned response, exactly as if it had come back over the wire.
test('branches: state defaults to "" when an older daemon omits it (wire-compat)', async () => {
  const client = Object.create(Client.prototype) as Client;
  // Milestone 4 Task 1 added BranchInfo.state to the wire protocol. An
  // older daemon predating that field sends a "branches" response with no
  // "state" key at all — Client.branches() must not throw or fabricate a
  // value; Branch.state must default to "" (additive, backward-compatible
  // field; see docs/reference.md's Branch states section).
  (client as unknown as { _call: Client["_call"] })._call = async () => ({
    ok: true,
    branches: [{ branch: "main", head_txid: 1 }],
  });
  const branches = await client.branches("app");
  assert.equal(branches.length, 1);
  assert.equal(branches[0].state, "");
});

test("branches: state is passed through when present", async () => {
  const client = Object.create(Client.prototype) as Client;
  (client as unknown as { _call: Client["_call"] })._call = async () => ({
    ok: true,
    branches: [{ branch: "main", head_txid: 1, state: "active" }],
  });
  const branches = await client.branches("app");
  assert.equal(branches[0].state, "active");
});

test("errors are loud", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await assert.rejects(
      () => c.checkout("nope", "main"),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.ok(err.message);
        return true;
      },
    );
  } finally {
    await c.close();
  }
});

test("rollback, promote, status", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  // Uses its own db so it can't interfere with the lifecycle test's
  // assertions even though they share one daemon fixture.
  const c = await connect(fixture!.sock);
  try {
    await c.create("rp");
    const s = await c.open("rp");
    const sessions = await c.status();
    assert.ok(sessions.some((st) => st.db === "rp" && st.branch === "main"));

    sqlite3(s.path, "CREATE TABLE t (v TEXT);");
    const cp1 = await s.flush("cp1");
    assert.ok(cp1 > 0);
    sqlite3(s.path, "INSERT INTO t VALUES ('x');");
    await s.flush("cp2");
    await s.close();

    const path = await c.rollback("rp", "main", "cp1");
    const rows = sqlite3(path, "SELECT count(*) FROM t;").trim();
    assert.equal(rows, "0"); // rolled back before the insert

    await c.fork("rp", "main", "feature");
    const txid = await c.promote("rp", "feature", "main", { force: true });
    assert.ok(txid > 0);
  } finally {
    await c.close();
  }
});

test("dbs lists every database sorted", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("dbs-zeta");
    await c.create("dbs-alpha");
    const names = await c.dbs();
    assert.ok(names.includes("dbs-zeta"));
    assert.ok(names.includes("dbs-alpha"));
    assert.deepEqual(names, [...names].sort());
  } finally {
    await c.close();
  }
});

test("fork meta and checkpoints_v2/touched_at", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("meta-app");
    await c.fork("meta-app", "main", "with-meta", { meta: { eval_run: "42" } });
    const info = new Map((await c.branches("meta-app")).map((b) => [b.branch, b]));
    const branch = info.get("with-meta");
    assert.ok(branch);
    assert.ok(branch!.touched_at);
    assert.ok(branch!.checkpoints.includes("fork"));
    const cpV2 = branch!.checkpoints_v2.find((cp) => cp.name === "fork");
    assert.ok(cpV2);
    assert.ok(cpV2!.txid > 0);
    assert.ok(cpV2!.created_at);
  } finally {
    await c.close();
  }
});

test("fork without meta still works", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("meta-app-2");
    const txid = await c.fork("meta-app-2", "main", "no-meta");
    assert.ok(txid > 0);
  } finally {
    await c.close();
  }
});

test("flush meta creates checkpoint with meta", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("meta-app-3");
    const s = await c.open("meta-app-3");
    sqlite3(s.path, "CREATE TABLE t (v);");
    const txid = await s.flush("v1", { meta: { agent: "claude" } });
    assert.ok(txid > 0);
    const info = new Map((await c.branches("meta-app-3")).map((b) => [b.branch, b]));
    const cpV2 = info.get("main")!.checkpoints_v2.find((cp) => cp.name === "v1");
    assert.ok(cpV2);
    assert.ok(cpV2!.created_at);
    await s.close();
  } finally {
    await c.close();
  }
});

test("flush without meta still works", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("meta-app-4");
    const s = await c.open("meta-app-4");
    const txid = await s.flush("v1");
    assert.ok(txid > 0);
    await s.close();
  } finally {
    await c.close();
  }
});

test("export: head and named checkpoint", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("export-app");
    const s = await c.open("export-app");
    sqlite3(s.path, "CREATE TABLE t (v); INSERT INTO t VALUES ('one');");
    await s.flush("v1");
    sqlite3(s.path, "INSERT INTO t VALUES ('two');");
    await s.flush("v2");
    await s.close();

    const out1 = join(fixture!.dir, "export-v1.db");
    await c.export("export-app", "main", out1, { checkpoint: "v1" });
    assert.equal(sqlite3(out1, "SELECT count(*) FROM t;").trim(), "1");

    const out2 = join(fixture!.dir, "export-head.db");
    await c.export("export-app", "main", out2);
    assert.equal(sqlite3(out2, "SELECT count(*) FROM t;").trim(), "2");
  } finally {
    await c.close();
  }
});

test("export: refuses overwrite without force, then force succeeds", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("export-force-app");
    const s = await c.open("export-force-app");
    sqlite3(s.path, "CREATE TABLE t (v);");
    await s.flush("v1");
    await s.close();

    const out = join(fixture!.dir, "export-force.db");
    await c.export("export-force-app", "main", out);
    await assert.rejects(() => c.export("export-force-app", "main", out), OffshootError);
    await c.export("export-force-app", "main", out, { force: true });
  } finally {
    await c.close();
  }
});

test("export: rejects a relative path", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("export-relpath-app");
    await assert.rejects(() => c.export("export-relpath-app", "main", "relative-out.db"), OffshootError);
  } finally {
    await c.close();
  }
});

test("export: misses a session's unflushed writes", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("export-unflushed-app");
    const s = await c.open("export-unflushed-app");
    sqlite3(s.path, "CREATE TABLE t (v); INSERT INTO t VALUES ('durable');");
    await s.flush("v1");
    sqlite3(s.path, "INSERT INTO t VALUES ('unflushed');"); // never flushed

    const out = join(fixture!.dir, "export-unflushed.db");
    await c.export("export-unflushed-app", "main", out);
    assert.equal(
      sqlite3(out, "SELECT count(*) FROM t;").trim(),
      "1",
      "export must not include a session's unflushed write",
    );

    await s.flush();
    const out2 = join(fixture!.dir, "export-after-flush.db");
    await c.export("export-unflushed-app", "main", out2);
    assert.equal(sqlite3(out2, "SELECT count(*) FROM t;").trim(), "2");
    await s.close();
  } finally {
    await c.close();
  }
});

test("checkoutAt: materializes a separate read-only path", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("checkout-at-app");
    const s = await c.open("checkout-at-app");
    sqlite3(s.path, "CREATE TABLE t (v); INSERT INTO t VALUES ('one');");
    await s.flush("v1");
    sqlite3(s.path, "INSERT INTO t VALUES ('two');");
    await s.flush("v2");

    const roPath = await c.checkoutAt("checkout-at-app", "main", "v1");
    assert.notEqual(roPath, s.path);
    assert.equal(sqlite3(roPath, "SELECT count(*) FROM t;").trim(), "1");
    const mode = statSync(roPath).mode & 0o777;
    assert.equal(mode, 0o444);

    // Repeat call is a cache hit: same path, no error.
    const again = await c.checkoutAt("checkout-at-app", "main", "v1");
    assert.equal(again, roPath);

    await s.close();
  } finally {
    await c.close();
  }
});

test("checkoutAt: requires a checkpoint name", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("checkout-at-empty-app");
    await assert.rejects(() => c.checkoutAt("checkout-at-empty-app", "main", ""), OffshootError);
  } finally {
    await c.close();
  }
});

test("checkoutAt: rejects a path-traversal checkpoint", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  // The SDK forwards `checkpoint` verbatim as the daemon's `name` field;
  // the server-side ops.Workspace.CheckoutAt fix must reject a crafted
  // value before it ever reaches CheckoutAtPath's filepath.Join, not just
  // an empty one.
  const c = await connect(fixture!.sock);
  try {
    await c.create("checkout-at-traversal-app");
    const s = await c.open("checkout-at-traversal-app");
    sqlite3(s.path, "CREATE TABLE t (v); INSERT INTO t VALUES ('writable');");
    await s.close();
    for (const bad of ["../../../etc/passwd", "..", "a/b", "../../checkouts/app/main"]) {
      await assert.rejects(() => c.checkoutAt("checkout-at-traversal-app", "main", bad), OffshootError);
    }
  } finally {
    await c.close();
  }
});

// --- events() decode-path tests (no real offshoot daemon needed) ---------

test("events(): dropped_slow_consumer is yielded then the stream ends", async () => {
  const lines = [
    JSON.stringify({
      v: 1,
      ts: "2026-08-07T00:00:00Z",
      type: "session_opened",
      db: "app",
      branch: "main",
      detail: { holder: "h1", epoch: 1 },
    }),
    JSON.stringify({ v: 1, ts: "2026-08-07T00:00:01Z", type: "dropped_slow_consumer" }),
  ];
  const fake = await fakeSubscribeDaemon(lines);
  try {
    const client = fakeEventsClient(fake.sockPath);
    const got: OffshootEvent[] = [];
    for await (const ev of client.events()) got.push(ev);
    // Yielded like any other event, then the async iterator simply ends
    // (the for-await loop exits normally) -- no thrown error, per this
    // method's documented contract.
    assert.equal(got.length, 2);
    assert.equal(got[0]!.type, "session_opened");
    assert.equal(got[0]!.db, "app");
    assert.equal(got[0]!.branch, "main");
    assert.deepEqual(got[0]!.detail, { holder: "h1", epoch: 1 });
    assert.equal(got[1]!.type, "dropped_slow_consumer");
    assert.equal(got[1]!.db, undefined);
  } finally {
    await fake.close();
  }
});

test("events(): fields default sensibly when the daemon omits them", async () => {
  const fake = await fakeSubscribeDaemon([JSON.stringify({ v: 1, ts: "2026-08-07T00:00:00Z", type: "reaped" })]);
  try {
    const client = fakeEventsClient(fake.sockPath);
    const got: OffshootEvent[] = [];
    for await (const ev of client.events()) got.push(ev);
    assert.equal(got.length, 1);
    assert.equal(got[0]!.v, 1);
    assert.equal(got[0]!.type, "reaped");
    assert.equal(got[0]!.db, undefined);
    assert.equal(got[0]!.branch, undefined);
    assert.equal(got[0]!.detail, undefined);
  } finally {
    await fake.close();
  }
});

test("events(): a not-ok subscribe ack rejects", async () => {
  const fake = await fakeSubscribeDaemon([], { ok: false, error: "boom" });
  try {
    const client = fakeEventsClient(fake.sockPath);
    await assert.rejects(
      async () => {
        for await (const _ev of client.events()) {
          // unreachable
        }
      },
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /boom/);
        return true;
      },
    );
  } finally {
    await fake.close();
  }
});

test("events(): a malformed event line raises OffshootError", async () => {
  const fake = await fakeSubscribeDaemon(["not json"]);
  try {
    const client = fakeEventsClient(fake.sockPath);
    await assert.rejects(async () => {
      for await (const _ev of client.events()) {
        // unreachable
      }
    }, OffshootError);
  } finally {
    await fake.close();
  }
});

test("events(): the daemon closing before ever acking raises OffshootError", async () => {
  // The daemon accepted the connection and read the subscribe request,
  // then closed WITHOUT ever writing an ack (a crash, a shutdown race, a
  // transport hiccup) -- NOT a legitimate empty stream (that's only
  // "acked, then disconnected with zero events"). Must throw, per
  // events()'s own documented contract ("only a genuine transport/
  // protocol failure ... throws") -- mirrors Python's identical
  // `test_daemon_closing_before_ever_acking_raises_offshoot_error`.
  const fake = await fakeSubscribeDaemon([], { ok: true }, true);
  try {
    const client = fakeEventsClient(fake.sockPath);
    await assert.rejects(
      async () => {
        for await (const _ev of client.events()) {
          // unreachable
        }
      },
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /closed the connection/);
        return true;
      },
    );
  } finally {
    await fake.close();
  }
});

// --- events() against a real daemon ---------------------------------------

test("events(): sees a real session lifecycle in order, on its own dedicated connection", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("events-lifecycle-app");
    const c2 = await connect(fixture!.sock);
    try {
      // Deterministic, zero-fixed-sleep sync for the ack-vs-transition
      // race -- see openUntilSubscribed's doc comment.
      const { session: s, it, opened } = await openUntilSubscribed(c, c2, "events-lifecycle-app");
      assert.equal(opened.type, "session_opened");
      assert.equal(opened.db, "events-lifecycle-app");
      assert.equal(opened.branch, "main");

      // Subscription is now proven live -- no more races. The rest of
      // the lifecycle is driven once, normally, and read directly off
      // the same winning `it`.
      sqlite3(s.path, "CREATE TABLE t (v);");
      let txid = 0;
      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        txid = await s.flush();
        if (txid) break;
        await sleep(100);
      }
      assert.ok(txid > 0);

      const flushed = await it.next();
      assert.equal(flushed.done, false);
      assert.equal(flushed.value!.type, "flushed");
      assert.equal(flushed.value!.db, "events-lifecycle-app");
      assert.equal(flushed.value!.detail?.kind, "manual");

      await s.close();
      const closed = await it.next();
      assert.equal(closed.done, false);
      assert.equal(closed.value!.type, "session_closed");
      assert.equal(closed.value!.db, "events-lifecycle-app");

      await it.return?.();
    } finally {
      await c2.close();
    }
  } finally {
    await c.close();
  }
});

test("events(): break mid-stream closes the dedicated socket (no leaked fd)", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("events-close-app");
    const c2 = await connect(fixture!.sock);
    try {
      // Same zero-fixed-sleep retry sync as the lifecycle test above.
      const { session: s, it, opened } = await openUntilSubscribed(c, c2, "events-close-app");
      assert.equal(opened.type, "session_opened");

      // Baseline sampled HERE, AFTER any retry churn above -- an
      // abandoned attempt's connection, if any, leaks (best-effort
      // cleanup only -- see openUntilSubscribed's doc comment) and so
      // belongs in the baseline, not the delta this assertion actually
      // cares about: whether closing the WINNING `it` below releases its
      // own fd.
      const baseline = daemonUnixSocketFdCount(fixture!.proc.pid!);

      // Stop iterating mid-stream -- return() propagates through
      // events()'s try/finally, closing the dedicated socket. Safe here:
      // the winning `it`'s only next() call already resolved (inside
      // openUntilSubscribed), so nothing else is concurrently driving it.
      await it.return?.();

      if (baseline !== undefined) {
        const deadline = Date.now() + 5_000;
        let after = baseline + 1;
        while (Date.now() < deadline) {
          after = daemonUnixSocketFdCount(fixture!.proc.pid!) ?? after;
          if (after <= baseline) break;
          await sleep(100);
        }
        assert.ok(
          after <= baseline,
          `daemon unix-socket fd count did not return to baseline after break() (baseline=${baseline}, after=${after}) -- possible leaked subscriber fd`,
        );
      }

      await s.close();

      // The daemon itself must be unaffected: an ordinary op on this
      // same connection still works after the dropped subscriber.
      await c.branches("events-close-app");
    } finally {
      await c2.close();
    }
  } finally {
    await c.close();
  }
});

test("daemon death mid-call raises OffshootError", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  // A dedicated, short-lived daemon (not the shared fixture) so killing it
  // doesn't disturb the other tests. Reuses the already-built binary to
  // avoid a second `go build`.
  const d = await DaemonFixture.start(fixture!.bin);
  try {
    const c = await connect(d.sock);
    try {
      await c.create("warm"); // prove the connection works before killing it
      d.proc.kill("SIGKILL");
      await new Promise<void>((resolve) => d.proc.once("exit", () => resolve()));
      await assert.rejects(() => c.status(), OffshootError);
    } finally {
      await c.close();
    }
  } finally {
    await d.stop();
  }
});
