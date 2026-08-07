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
import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter as PATH_DELIMITER, join } from "node:path";
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

test("startDaemon: resolves the binary via PATH when OFFSHOOT_BIN is unset (success branch)", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const pathDir = mkdtempSync(join(tmpdir(), "offshoot-testkit-pathbin-"));
  try {
    symlinkSync(bin!, join(pathDir, "offshoot"));
    await withEnv({ OFFSHOOT_BIN: undefined, PATH: `${pathDir}${PATH_DELIMITER}${process.env.PATH ?? ""}` }, async () => {
      // No opts.bin and no OFFSHOOT_BIN: this can only succeed by finding
      // `offshoot` on PATH, exercising the fallback branch the "no binary
      // anywhere" test above never reaches.
      const daemon = await startDaemon({});
      try {
        const c = await connect(daemon.sock);
        try {
          await c.create("path-resolved-app");
        } finally {
          await c.close();
        }
      } finally {
        await daemon.stop();
      }
    });
  } finally {
    rmSync(pathDir, { recursive: true, force: true });
  }
});

// --------------------------------------------------------------------------
// stop(): must not hold the process open past resolving.
// --------------------------------------------------------------------------

test(
  "stop(): the internal exit-wait timer is unref'd, so it never holds the process open once stop() resolves",
  async (t: TestContext) => {
    if (!canRun) {
      t.skip("go and/or sqlite3 not on PATH");
      return;
    }
    // Runs in a genuinely separate child process, not in-process: the bug
    // this guards against (stop()'s losing `sleep(stopTimeoutMs)` race
    // branch left as a live, non-unref'd timer) only manifests as "the
    // process doesn't exit," which is only observable from outside that
    // process — an in-process assertion has no way to see it.
    const testkitJsUrl = new URL("../src/testkit.js", import.meta.url);
    const dir = mkdtempSync(join(tmpdir(), "offshoot-testkit-stoptimer-"));
    try {
      const scriptPath = join(dir, "child.mjs");
      writeFileSync(
        scriptPath,
        [
          `import { startDaemon } from ${JSON.stringify(testkitJsUrl.href)};`,
          `const daemon = await startDaemon({ bin: ${JSON.stringify(bin)} });`,
          `await daemon.stop();`,
          `// Deliberately nothing else: a live, non-unref'd timer left over`,
          `// from stop()'s internal race would keep this process alive for`,
          `// up to its 10s default even though stop() already resolved.`,
        ].join("\n"),
      );
      const start = Date.now();
      try {
        execFileSync(process.execPath, [scriptPath], { timeout: 4_000 });
      } catch (e: any) {
        assert.fail(
          `child process did not exit within 4s of stop() resolving — its internal timer is ` +
            `likely not unref'd, holding the event loop open: ${e.message}`,
        );
      }
      const elapsed = Date.now() - start;
      assert.ok(elapsed < 4_000, `stop() held the process open for ${elapsed}ms after resolving`);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  },
);

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

test("seedOnce: concurrent calls for the same name share one seed run, not two", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    let runs = 0;
    const seed = async (dbPath: string) => {
      runs++;
      // A small delay so both calls are genuinely in flight together, not
      // accidentally "serialized for free" by synchronous call-stack
      // ordering alone — this is what would catch seedOnce checking its
      // cache before starting, but only memoizing the SETTLED result (the
      // bug: two concurrent calls both see no cache entry yet, both start
      // seeding, and the loser's `cache.set` clobbers the winner's entry).
      await new Promise((resolve) => setTimeout(resolve, 50));
      execFileSync("sqlite3", [dbPath, "CREATE TABLE t (v); INSERT INTO t VALUES ('once');"]);
    };
    const [h1, h2] = await Promise.all([
      seedOnce(daemon, { name: "concurrent", seed }),
      seedOnce(daemon, { name: "concurrent", seed }),
    ]);
    assert.equal(runs, 1, "the seed callable must run exactly once for two concurrent same-name calls");
    assert.equal(h1, h2, "both concurrent calls resolve to the identical handle object");
    assert.equal(h1.db, "eval-concurrent");
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

test("seedOnce: two distinct callback functions for the same name is a mismatch, not a memoization hit", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const seedA = async (dbPath: string) => {
      execFileSync("sqlite3", [dbPath, "CREATE TABLE t (v);"]);
    };
    const seedB = async (dbPath: string) => {
      execFileSync("sqlite3", [dbPath, "CREATE TABLE u (v);"]);
    };
    await seedOnce(daemon, { name: "callback-mismatch", seed: seedA });
    await assert.rejects(
      () => seedOnce(daemon, { name: "callback-mismatch", seed: seedB }),
      (err: unknown) => {
        assert.ok(err instanceof OffshootError);
        assert.match(err.message, /DIFFERENT seed/);
        assert.match(err.message, /callback-mismatch/);
        return true;
      },
      "two distinct callables for the same name must fingerprint as different seeds",
    );
  });
});

test("seedOnce: the same callback reference reused for the same name is a memoization hit", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const seed = async (dbPath: string) => {
      execFileSync("sqlite3", [dbPath, "CREATE TABLE t (v);"]);
    };
    const h1 = await seedOnce(daemon, { name: "callback-memo", seed });
    const h2 = await seedOnce(daemon, { name: "callback-memo", seed });
    assert.equal(h2, h1, "the identical function reference reused must be a pure memoization hit (same object)");
  });
});

test("seedOnce: editing a .sql seed file on disk between calls is caught as a mismatch, not silently passed", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const dir = mkdtempSync(join(tmpdir(), "offshoot-testkit-editedseed-"));
    try {
      const sqlFile = join(dir, "seed.sql");
      writeFileSync(sqlFile, "CREATE TABLE t (v);");
      await seedOnce(daemon, { name: "edited-file", seed: sqlFile });

      // Same PATH string, but the file's on-disk CONTENT has changed —
      // must be caught (fingerprinting the raw path string, rather than
      // the resolved content, would miss this: the path text is
      // unchanged even though what it points at is not).
      writeFileSync(sqlFile, "CREATE TABLE u (v);");
      await assert.rejects(
        () => seedOnce(daemon, { name: "edited-file", seed: sqlFile }),
        (err: unknown) => {
          assert.ok(err instanceof OffshootError);
          assert.match(err.message, /DIFFERENT seed/);
          return true;
        },
      );
    } finally {
      rmSync(dir, { recursive: true, force: true });
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

test("forkPerTest: close() warns (not throws) when only the destroy step fails", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  await withDaemon(async (daemon) => {
    const seed = await seedOnce(daemon, { name: "destroy-warn", seed: "CREATE TABLE t (v)" });
    const fork = await forkPerTest(daemon, seed);

    // Close the session and destroy the branch out from under the fork,
    // the same way test_fork_factory_destroy_failure_warns_not_raises does
    // on the Python side — fork.client is the same connection fork.close()
    // itself will use, so its own session.close() (via the raw "close" op)
    // and client.destroy() calls below run cleanly, leaving nothing for
    // fork.close() to legitimately do when it runs afterward.
    await fork.client._call("close", { db: fork.db, branch: fork.branch });
    await fork.client.destroy(fork.db, fork.branch, { force: true });

    const originalWarn = console.warn;
    const warnings: string[] = [];
    console.warn = ((...args: unknown[]) => {
      warnings.push(args.map(String).join(" "));
    }) as typeof console.warn;
    try {
      await fork.close(); // must not throw despite there being nothing left to close/destroy
    } finally {
      console.warn = originalWarn;
    }
    assert.ok(
      warnings.some((w) => w.includes("destroying branch") && w.includes(fork.branch)),
      "expected a warning naming the branch-destroy failure",
    );
  });
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
