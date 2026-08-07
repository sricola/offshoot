// node:test suite for sdk/typescript/src/testkit.ts, mirroring the
// direct-daemon tier of sdk/python/tests/test_pytest_plugin.py: naming,
// TTL, fingerprint mismatch, teardown-warn-not-throw with a killed daemon,
// and a .dump-shaped seed — plus one integration scenario wiring testkit
// into node:test's own before/after/beforeEach/afterEach, the way a real
// suite would.
//
// Builds the offshoot binary once (or reuses $OFFSHOOT_BIN) and shares it
// across tests via testkit's own startDaemon({ bin }); each test still
// gets its OWN daemon + temp store, mirroring test_pytest_plugin.py's
// function-scoped `daemon`/`client` fixtures. Skips cleanly if `go` or
// `sqlite3` are not on PATH — the handful of pure-error-path tests below
// (binary resolution, missing sqlite3 CLI) don't need either and always
// run.

import { test, describe, it, before, after, beforeEach, afterEach, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { connect, OffshootError } from "../src/client.js";
import { startDaemon, seedOnce, forkPerTest, dump, type DaemonHandle, type SeedHandle, type ForkedSession } from "../src/testkit.js";

// test-dist/test/testkit.test.js -> repo root is four levels up.
const REPO = join(fileURLToPath(new URL(".", import.meta.url)), "..", "..", "..", "..");

function hasCmd(cmd: string): boolean {
  try {
    execFileSync(process.platform === "win32" ? "where" : "which", [cmd], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

const canRun = hasCmd("go") && hasCmd("sqlite3");

let bin: string | undefined;
let binDir: string | undefined;

before(() => {
  if (!canRun) return;
  binDir = mkdtempSync(join(tmpdir(), "offshoot-testkit-bin-"));
  bin = join(binDir, "offshoot");
  execFileSync("go", ["build", "-o", bin, "./cmd/offshoot"], { cwd: REPO });
});

after(() => {
  if (binDir) rmSync(binDir, { recursive: true, force: true });
});

// --------------------------------------------------------------------------
// startDaemon: binary resolution — OFFSHOOT_BIN -> PATH -> clear error
// naming both. These don't need `go`/`sqlite3`: they only exercise the
// failure paths, never actually launch a daemon.
// --------------------------------------------------------------------------

function withEnv<T>(patch: Record<string, string | undefined>, fn: () => T): T {
  const prev: Record<string, string | undefined> = {};
  for (const k of Object.keys(patch)) prev[k] = process.env[k];
  for (const [k, v] of Object.entries(patch)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  try {
    return fn();
  } finally {
    for (const [k, v] of Object.entries(prev)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

test("startDaemon: OFFSHOOT_BIN pointing at a nonexistent path fails loud", async () => {
  await withEnv({ OFFSHOOT_BIN: "/definitely/not/a/real/offshoot/binary" }, async () => {
    await assert.rejects(
      () => startDaemon(),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /OFFSHOOT_BIN/);
        assert.match(err.message, /does not exist/);
        return true;
      },
    );
  });
});

test("startDaemon: OFFSHOOT_BIN pointing at a directory fails loud", async () => {
  await withEnv({ OFFSHOOT_BIN: tmpdir() }, async () => {
    await assert.rejects(
      () => startDaemon(),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /is not a file/);
        return true;
      },
    );
  });
});

test("startDaemon: no binary anywhere names both OFFSHOOT_BIN and PATH", async () => {
  await withEnv({ OFFSHOOT_BIN: undefined, PATH: "" }, async () => {
    await assert.rejects(
      () => startDaemon(),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /OFFSHOOT_BIN/);
        assert.match(err.message, /PATH/);
        return true;
      },
    );
  });
});

// --------------------------------------------------------------------------
// dump: golden-file framing + clear error when the sqlite3 CLI is missing.
// --------------------------------------------------------------------------

test("dump: is the right comparison, not raw bytes (VACUUM changes bytes, not the dump)", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const dir = mkdtempSync(join(tmpdir(), "offshoot-testkit-dump-"));
  try {
    const dbPath = join(dir, "a.sqlite");
    execFileSync("sqlite3", [
      dbPath,
      "CREATE TABLE t (v); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); DELETE FROM t WHERE v = 1;",
    ]);
    const bytesBefore = readFileSync(dbPath);
    const dumpBefore = await dump(dbPath);
    execFileSync("sqlite3", [dbPath, "VACUUM;"]);
    const bytesAfter = readFileSync(dbPath);
    const dumpAfter = await dump(dbPath);
    assert.notDeepEqual(bytesBefore, bytesAfter, "sanity: VACUUM must actually change the on-disk bytes");
    assert.equal(dumpBefore, dumpAfter, "dump text must be identical despite different bytes");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("dump: clear error when the sqlite3 CLI is missing", async () => {
  await withEnv({ PATH: "" }, async () => {
    await assert.rejects(
      () => dump("/nonexistent/whatever.db"),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /sqlite3/);
        return true;
      },
    );
  });
});

// --------------------------------------------------------------------------
// seedOnce: naming/memoization, fingerprint mismatch, .dump-shaped seed,
// path-to-.sql seed, async callback seed.
// --------------------------------------------------------------------------

async function withDaemon<T>(fn: (daemon: DaemonHandle) => Promise<T>): Promise<T> {
  const daemon = await startDaemon({ bin });
  try {
    return await fn(daemon);
  } finally {
    await daemon.stop();
  }
}

test("seedOnce: memoizes per name and detects a mismatched re-seed", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const h1 = await seedOnce(daemon, { name: "widgets", seed: "CREATE TABLE t (v)" });
    assert.equal(h1.db, "eval-widgets");
    assert.equal(h1.checkpoint, "seed");

    const h2 = await seedOnce(daemon, { name: "widgets", seed: "CREATE TABLE t (v)" });
    assert.equal(h2, h1, "identical repeat seed is a pure memoization hit (same object)");

    await assert.rejects(
      () => seedOnce(daemon, { name: "widgets", seed: "CREATE TABLE u (v)" }),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /DIFFERENT seed/);
        assert.match(err.message, /widgets/);
        return true;
      },
    );
  });
});

test("seedOnce: distinct names create distinct databases", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const a = await seedOnce(daemon, { name: "a", seed: "CREATE TABLE t (v)" });
    const b = await seedOnce(daemon, { name: "b", seed: "CREATE TABLE t (v)" });
    assert.notEqual(a.db, b.db);
    assert.equal(a.db, "eval-a");
    assert.equal(b.db, "eval-b");
  });
});

test("seedOnce: accepts a sqlite3 .dump-shaped seed verbatim (PRAGMA before BEGIN TRANSACTION)", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const dir = mkdtempSync(join(tmpdir(), "offshoot-testkit-dumpseed-"));
    try {
      const src = join(dir, "src.sqlite");
      execFileSync("sqlite3", [
        src,
        "CREATE TABLE t (v); INSERT INTO t VALUES ('one'); INSERT INTO t VALUES ('two');",
      ]);
      const dumpText = await dump(src);
      // Sanity: reproduces the shape a naive "starts with BEGIN" check would
      // miss (a leading PRAGMA line before BEGIN TRANSACTION).
      assert.match(dumpText.trim().toUpperCase(), /^PRAGMA/);
      assert.match(dumpText.toUpperCase(), /BEGIN TRANSACTION/);

      const handle = await seedOnce(daemon, { name: "dump-shaped", seed: dumpText });
      const c = await connect(daemon.sock);
      try {
        const p = await c.checkoutAt(handle.db, "main", handle.checkpoint);
        const rows = execFileSync("sqlite3", [p, "SELECT count(*) FROM t;"]).toString().trim();
        assert.equal(rows, "2");
      } finally {
        await c.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

test("seedOnce: accepts a path to a .sql file", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const dir = mkdtempSync(join(tmpdir(), "offshoot-testkit-pathseed-"));
    try {
      const sqlFile = join(dir, "seed.sql");
      writeFileSync(sqlFile, "CREATE TABLE t (v);\nINSERT INTO t VALUES ('from-file');\n");
      const handle = await seedOnce(daemon, { name: "from-file", seed: sqlFile });
      const c = await connect(daemon.sock);
      try {
        const p = await c.checkoutAt(handle.db, "main", handle.checkpoint);
        const v = execFileSync("sqlite3", [p, "SELECT v FROM t;"]).toString().trim();
        assert.equal(v, "from-file");
      } finally {
        await c.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

test("seedOnce: accepts an async callback seed given the writable path", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const handle = await seedOnce(daemon, {
      name: "callback",
      seed: async (dbPath: string) => {
        execFileSync("sqlite3", [dbPath, "CREATE TABLE t (v); INSERT INTO t VALUES ('from-callback');"]);
      },
    });
    const c = await connect(daemon.sock);
    try {
      const p = await c.checkoutAt(handle.db, "main", handle.checkpoint);
      const v = execFileSync("sqlite3", [p, "SELECT v FROM t;"]).toString().trim();
      assert.equal(v, "from-callback");
    } finally {
      await c.close();
    }
  });
});

// --------------------------------------------------------------------------
// forkPerTest: TTL default/override, worker-safe distinct naming,
// isolation, string-name shorthand, teardown-warn-not-throw.
// --------------------------------------------------------------------------

test("forkPerTest: default TTL is 1h, opts.ttl overrides it", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const seed = await seedOnce(daemon, { name: "ttl", seed: "CREATE TABLE t (v)" });

    const f1 = await forkPerTest(daemon, seed);
    try {
      const info = new Map((await f1.client.branches(seed.db)).map((b) => [b.branch, b]));
      assert.ok(info.get(f1.branch)!.ttl.startsWith("1h"));
    } finally {
      await f1.close();
    }

    const f2 = await forkPerTest(daemon, seed, { ttl: "2h" });
    try {
      const info = new Map((await f2.client.branches(seed.db)).map((b) => [b.branch, b]));
      assert.ok(info.get(f2.branch)!.ttl.startsWith("2h"));
    } finally {
      await f2.close();
    }
  });
});

test("forkPerTest: repeated calls get distinct, worker-safe branch names and isolated writes", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const seed = await seedOnce(daemon, { name: "iso", seed: "CREATE TABLE t (v)" });
    const f1 = await forkPerTest(daemon, seed);
    const f2 = await forkPerTest(daemon, seed);
    try {
      assert.notEqual(f1.branch, f2.branch);
      assert.match(f1.branch, /^t-local-/); // no VITEST_POOL_ID/JEST_WORKER_ID under node:test
      execFileSync("sqlite3", [f1.path, "INSERT INTO t VALUES ('only-on-f1');"]);
      const rows2 = execFileSync("sqlite3", [f2.path, "SELECT count(*) FROM t;"]).toString().trim();
      assert.equal(rows2, "0", "fork 2 must not see fork 1's write");
      const rows1 = execFileSync("sqlite3", [f1.path, "SELECT count(*) FROM t;"]).toString().trim();
      assert.equal(rows1, "1");
    } finally {
      await f1.close();
      await f2.close();
    }
  });
});

test("forkPerTest: accepts a bare string naming an already-seeded name", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    await seedOnce(daemon, { name: "by-name", seed: "CREATE TABLE t (v)" });
    const f = await forkPerTest(daemon, "by-name");
    try {
      assert.equal(f.db, "eval-by-name");
    } finally {
      await f.close();
    }
  });
});

test("forkPerTest: a bare string for an unseeded name fails clearly", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    await assert.rejects(
      () => forkPerTest(daemon, "never-seeded"),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /never-seeded/);
        return true;
      },
    );
  });
});

test("forkPerTest: close() teardown failures warn via console.warn, never throw, against a killed daemon", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const daemon = await startDaemon({ bin });
  const seed = await seedOnce(daemon, { name: "warn-test", seed: "CREATE TABLE t (v)" });
  const fork = await forkPerTest(daemon, seed);

  const originalWarn = console.warn;
  const warnings: string[] = [];
  console.warn = ((...args: unknown[]) => {
    warnings.push(args.map(String).join(" "));
  }) as typeof console.warn;
  try {
    daemon.proc.kill("SIGKILL");
    await new Promise<void>((resolve) => daemon.proc.once("exit", () => resolve()));
    await fork.close(); // must not throw despite the daemon being dead
  } finally {
    console.warn = originalWarn;
    await daemon.stop();
  }
  assert.ok(warnings.length >= 1, "expected at least one teardown warning");
  assert.ok(
    warnings.some((w) => w.includes(fork.branch)),
    "expected a warning naming the fork's branch",
  );
});

// --------------------------------------------------------------------------
// Integration scenario: testkit wired into node:test's own
// before/after/beforeEach/afterEach, as a real suite would use it.
// --------------------------------------------------------------------------

describe("integration: testkit wired into node:test's own before/after", () => {
  let daemon: DaemonHandle | undefined;
  let seed: SeedHandle | undefined;
  let session: ForkedSession | undefined;

  before(async () => {
    if (!canRun) return;
    daemon = await startDaemon({ bin });
    seed = await seedOnce(daemon, {
      name: "integration",
      seed: "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO widgets (name) VALUES ('gizmo');",
    });
  });

  after(async () => {
    if (daemon) await daemon.stop();
  });

  beforeEach(async () => {
    if (!canRun) return;
    session = await forkPerTest(daemon!, seed!);
  });

  afterEach(async () => {
    if (session) await session.close();
    session = undefined;
  });

  it("sees the seeded row and can write to its own isolated fork", (t: TestContext) => {
    if (!canRun) {
      t.skip("go and/or sqlite3 not on PATH");
      return;
    }
    const name = execFileSync("sqlite3", [session!.path, "SELECT name FROM widgets;"]).toString().trim();
    assert.equal(name, "gizmo");
    execFileSync("sqlite3", [session!.path, "INSERT INTO widgets (name) VALUES ('sprocket');"]);
    const count = execFileSync("sqlite3", [session!.path, "SELECT count(*) FROM widgets;"]).toString().trim();
    assert.equal(count, "2");
  });

  it("does not see the previous test's write (fork-per-test isolation)", (t: TestContext) => {
    if (!canRun) {
      t.skip("go and/or sqlite3 not on PATH");
      return;
    }
    const count = execFileSync("sqlite3", [session!.path, "SELECT count(*) FROM widgets;"]).toString().trim();
    assert.equal(count, "1", "only the seed row — the other test's 'sprocket' insert must not be here");
  });
});
