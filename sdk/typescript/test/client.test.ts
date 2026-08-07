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

import { connect, OffshootError, Client } from "../src/client.js";

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
