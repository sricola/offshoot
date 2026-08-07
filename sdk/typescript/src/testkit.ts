// offshoot's TypeScript testkit — the vitest/jest counterpart of the
// pytest fixture plugin (sdk/python/offshoot/pytest_plugin.py).
//
// Framework-agnostic FUNCTIONS, not fixtures: this module has no vitest or
// jest dependency (zero runtime deps, same as the rest of this package) and
// registers nothing automatically. Wire these into your own
// beforeAll/afterEach (or before/afterEach, or whatever your framework
// calls them) — see the "integration" test in test/testkit.test.ts for the
// shape. This mirrors the pytest plugin's semantics wherever a JS
// equivalent exists; see each function's doc comment for exactly what
// carried over and what didn't (mostly: no pytest.ini, so there is no
// zero-code "default seed" — every seedOnce() call names its seed
// explicitly).
//
// Four functions:
//
// - startDaemon(opts?): locates the `offshoot` binary (OFFSHOOT_BIN env,
//   else PATH — clear error naming both when neither has one), starts it
//   on a fresh temp store + socket, and returns a handle you stop yourself
//   (there is no session-scoped auto-teardown here — call `stop()` from
//   your framework's top-level teardown).
// - seedOnce(daemon, {name?, seed}): a NAMED-SEED memoization cache keyed
//   on the (daemon, name) pair. The first call for a name creates database
//   `eval-{name}`, runs `seed` — a SQL string, a path to a `.sql` file, or
//   an async `(dbPath) => void` callback — and checkpoints it `seed`.
//   Later calls for the same name are a pure memoization hit UNLESS the
//   seed doesn't match what actually seeded that name (fingerprinted: SQL/
//   path text by content hash, callables by identity) — that raises a
//   clear error rather than silently keeping the first seed, exactly like
//   the pytest plugin's `offshoot_db`.
// - forkPerTest(daemon, seedHandleOrName, opts?): forks a fresh, worker-
//   safe-named branch from the seed's checkpoint with a TTL (default
//   "1h", opts.ttl overrides), opens a session, and returns a
//   ForkedSession. Call `.close()` yourself (typically in afterEach) —
//   it closes the session and destroys the branch; either failing WARNS
//   (`console.warn`), never throws, so one fork's teardown trouble never
//   blocks the rest of your suite's cleanup (the TTL is the crashed-run
//   backstop either way). Each ForkedSession owns its own connection and
//   its own teardown, independent of every other one — unlike the pytest
//   plugin's `_ForkFactory`, there is no shared per-test list to iterate,
//   so "one fork's cleanup failure doesn't stop another's" holds by
//   construction, not by a try/except loop.
// - dump(path): `sqlite3 <path> .dump`'s text output — THE way to compare
//   two offshoot-materialized SQLite files in a golden-file test. SQLite's
//   on-disk bytes are not deterministic across writes with identical
//   logical content (page layout, freelist order, vacuum state can all
//   differ) — NEVER byte-compare two exported/checked-out `.db` files;
//   compare `await dump(a) === await dump(b)` instead.
//
// Like sdk/typescript/test/client.test.ts, seeding and dumping shell out to
// the `sqlite3` CLI (via node:child_process) rather than bundling a SQLite
// driver — this package ships no DB driver and never will.

import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter as PATH_DELIMITER, join } from "node:path";

import { type Client, connect, type FlushOptions, OffshootError, type Session } from "./client.js";

// --------------------------------------------------------------------------
// startDaemon
// --------------------------------------------------------------------------

/** A running `offshoot serve` process on its own temp store + socket — what
 * {@link startDaemon} returns. `proc` is exposed (not just a private
 * detail) so a test can kill it directly to exercise a dead-daemon failure
 * mode, mirroring the Python plugin's public `DaemonHandle.proc`. */
export interface DaemonHandle {
  readonly sock: string;
  readonly store: string;
  readonly proc: ChildProcess;
  /** The last n lines this daemon wrote to stderr, newest last. */
  stderrTail(n?: number): string;
  /** Terminate the daemon (SIGTERM, then SIGKILL after timeoutMs) and
   * remove its temp store/socket dir. Idempotent — safe to call more than
   * once (e.g. once from a test that killed the daemon itself, then again
   * from a shared `after` hook). */
  stop(timeoutMs?: number): Promise<void>;
}

export interface StartDaemonOptions {
  /** Overrides OFFSHOOT_BIN/PATH lookup entirely — mainly for tests/tools
   * that already resolved a binary once and want to reuse it. */
  bin?: string;
  /** Use this directory instead of a fresh `mkdtemp` one. Not removed by
   * `stop()` when given explicitly — the caller owns it. */
  workdir?: string;
  /** How long to wait for the daemon's socket to appear before giving up.
   * Default 10s. */
  timeoutMs?: number;
}

const INSTALL_INSTRUCTIONS =
  "offshoot binary not found. Set OFFSHOOT_BIN to its path, or put " +
  "`offshoot` on PATH. Install: see " +
  "https://github.com/offshoot-db/offshoot#install (from a source " +
  "checkout: `go build -o /path/to/offshoot ./cmd/offshoot`, then " +
  "`export OFFSHOOT_BIN=/path/to/offshoot`).";

/** A minimal, dependency-free `which`: the first executable regular file
 * named cmd found on PATH. */
function which(cmd: string): string | undefined {
  const pathEnv = process.env.PATH ?? "";
  for (const dir of pathEnv.split(PATH_DELIMITER)) {
    if (!dir) continue;
    const candidate = join(dir, cmd);
    try {
      const st = statSync(candidate);
      if (st.isFile() && (st.mode & 0o111) !== 0) return candidate;
    } catch {
      // not in this dir; keep looking
    }
  }
  return undefined;
}

/** OFFSHOOT_BIN (env, or opts.bin) if set — must point at an existing,
 * regular file, else a loud error naming the bad path — else the first
 * `offshoot` on PATH, else a loud error naming both places that were
 * checked. There is no "skip" tier here (unlike the pytest plugin): this
 * module has no test-framework integration to skip through, so every
 * caller gets the same clear thrown error and decides for itself whether
 * that means skip, fail, or something else. */
function locateBinary(explicit?: string): string {
  const wanted = explicit ?? process.env.OFFSHOOT_BIN;
  if (wanted) {
    if (!existsSync(wanted)) {
      throw new OffshootError(`OFFSHOOT_BIN=${JSON.stringify(wanted)} does not exist.`);
    }
    if (!statSync(wanted).isFile()) {
      throw new OffshootError(`OFFSHOOT_BIN=${JSON.stringify(wanted)} is not a file (is it a directory?).`);
    }
    return wanted;
  }
  const found = which("offshoot");
  if (!found) throw new OffshootError(INSTALL_INSTRUCTIONS);
  return found;
}

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

/** Locate the `offshoot` binary, init a fresh store under a temp dir, and
 * start `offshoot serve` on it, waiting for its socket to come up. */
export async function startDaemon(opts: StartDaemonOptions = {}): Promise<DaemonHandle> {
  const binpath = locateBinary(opts.bin);
  const ownedWorkdir = opts.workdir === undefined;
  const workdir = opts.workdir ?? mkdtempSync(join(tmpdir(), "offshoot-testkit-"));
  const store = join(workdir, "store");
  const sock = join(workdir, "d.sock");
  const timeoutMs = opts.timeoutMs ?? 10_000;

  try {
    execFileSync(binpath, ["-store", store, "init"], { stdio: ["ignore", "ignore", "pipe"] });
  } catch (e: any) {
    throw new OffshootError(
      `offshoot -store ${store} init failed: ${e.stderr ? e.stderr.toString() : e.message}`,
    );
  }

  const proc = spawn(binpath, ["-store", store, "serve", "-socket", sock], {
    stdio: ["ignore", "ignore", "pipe"],
  });
  const tail: string[] = [];
  proc.stderr?.on("data", (chunk: Buffer) => {
    for (const line of chunk.toString("utf8").split("\n")) {
      if (line.length > 0) tail.push(line);
    }
    while (tail.length > 200) tail.shift();
  });

  const stderrTail = (n = 20): string => tail.slice(-n).join("\n");

  const deadline = Date.now() + timeoutMs;
  while (!existsSync(sock)) {
    if (proc.exitCode !== null || proc.signalCode !== null) {
      throw new OffshootError(`offshoot serve exited before it started listening:\n${stderrTail()}`);
    }
    if (Date.now() > deadline) {
      proc.kill();
      throw new OffshootError(`offshoot serve did not start listening within ${timeoutMs}ms:\n${stderrTail()}`);
    }
    await sleep(20);
  }

  let stopped = false;
  const stop = async (stopTimeoutMs = 10_000): Promise<void> => {
    if (stopped) return;
    stopped = true;
    if (proc.exitCode === null && proc.signalCode === null) {
      proc.kill();
      await Promise.race([
        new Promise<void>((resolve) => proc.once("exit", () => resolve())),
        sleep(stopTimeoutMs).then(() => {
          if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
        }),
      ]);
    }
    if (ownedWorkdir) rmSync(workdir, { recursive: true, force: true });
  };

  return { sock, store, proc, stderrTail, stop };
}

/** `connect()`, but a dead/unreachable daemon raises {@link OffshootError}
 * with the daemon's stderr tail appended instead of a bare Node `Error`
 * (ECONNREFUSED/ENOENT/...) — mirrors the pytest plugin's `_connect`. Every
 * request on an already-open connection is already wrapped this way by
 * {@link Client}'s own `_call`; this closes the gap for the connection
 * attempt itself, which every function below makes fresh. */
async function connectWithTail(daemon: DaemonHandle): Promise<Client> {
  try {
    return await connect(daemon.sock);
  } catch (e) {
    const tail = daemon.stderrTail();
    const msg =
      `offshoot: could not connect to the daemon at ${daemon.sock}: ${(e as Error).message}` +
      (tail ? `\n--- offshoot daemon stderr tail ---\n${tail}` : "");
    throw new OffshootError(msg);
  }
}

// --------------------------------------------------------------------------
// seedOnce
// --------------------------------------------------------------------------

/** A seed: a SQL string, a path to a `.sql` file (detected — see
 * {@link looksLikeSeedFilePath} — not a separate argument shape, since
 * there's no pytest.ini-style config file here to hold that decision), or
 * an async callback given the writable sqlite path to seed however it
 * likes (not auto-wrapped in a transaction — it owns its own transaction
 * boundaries). */
export type Seed = string | ((dbPath: string) => void | Promise<void>);

export interface SeedOnceOptions {
  /** Memoization key. Default "default". */
  name?: string;
  seed: Seed;
}

/** What {@link seedOnce} returns: the seeded database + checkpoint to fork
 * subsequent test branches from. */
export interface SeedHandle {
  readonly db: string;
  readonly checkpoint: string;
  readonly name: string;
}

interface SeedCacheEntry {
  handle: SeedHandle;
  fingerprint: string;
}

// Keyed by DaemonHandle identity so two independently started daemons never
// share (or leak into) each other's memoized seeds, and so the cache for a
// stopped daemon is eligible for GC once nothing else references the
// handle.
const seedCaches = new WeakMap<DaemonHandle, Map<string, SeedCacheEntry>>();
// Function-identity fingerprints for callable seeds (mirrors Python's
// id()-based fingerprint for a callable seed).
const callableIds = new WeakMap<object, number>();
let nextCallableId = 0;

function cacheFor(daemon: DaemonHandle): Map<string, SeedCacheEntry> {
  let m = seedCaches.get(daemon);
  if (!m) {
    m = new Map();
    seedCaches.set(daemon, m);
  }
  return m;
}

/** True if s looks like a path to an existing `.sql` file rather than
 * inline SQL text: no newline, ends in `.sql`, and a file actually exists
 * there. A seed string containing a newline is never treated as a path —
 * real filesystem paths don't contain them, and a multi-statement inline
 * seed almost always does. */
function looksLikeSeedFilePath(s: string): boolean {
  const trimmed = s.trim();
  return (
    trimmed.length > 0 && trimmed.length < 4096 && !trimmed.includes("\n") && /\.sql$/i.test(trimmed) && existsSync(trimmed)
  );
}

async function resolveSeedSql(seed: string): Promise<string> {
  return looksLikeSeedFilePath(seed) ? await readFile(seed, "utf8") : seed;
}

const SQLITE3_MISSING_MSG =
  "offshoot testkit: the `sqlite3` CLI is not on PATH. Install it (e.g. " +
  "`apt-get install sqlite3` / `brew install sqlite3`) — see this repo's " +
  "CONTRIBUTING.md's Dev setup section.";

function execSqlite3(args: string[], input?: string): string {
  try {
    return execFileSync("sqlite3", args, { input, encoding: "utf8" });
  } catch (e: any) {
    if (e && e.code === "ENOENT") throw new OffshootError(SQLITE3_MISSING_MSG);
    const stderr = typeof e?.stderr === "string" ? e.stderr.trim() : String(e?.message ?? e);
    throw new OffshootError(`offshoot testkit: \`sqlite3 ${args.join(" ")}\` failed: ${stderr}`);
  }
}

/** Skip leading whitespace, `--`/`/* *​/` comments, and `PRAGMA ...;`
 * statements, returning whatever's left. Used only to detect whether a
 * seed script already opens its own transaction — a `sqlite3 <file>
 * .dump`'s text conventionally starts `PRAGMA foreign_keys=OFF;` before
 * `BEGIN TRANSACTION;`, so a naive "does this start with BEGIN" check would
 * miss it and wrap it in a second, nested (and rejected) BEGIN. Ported
 * verbatim from `offshoot.pytest_plugin._skip_leading_noise`. */
function skipLeadingNoise(text: string): string {
  let i = 0;
  const n = text.length;
  for (;;) {
    while (i < n && " \t\r\n".includes(text[i]!)) i++;
    if (text.startsWith("--", i)) {
      const nl = text.indexOf("\n", i);
      i = nl === -1 ? n : nl + 1;
      continue;
    }
    if (text.startsWith("/*", i)) {
      const end = text.indexOf("*/", i + 2);
      i = end === -1 ? n : end + 2;
      continue;
    }
    if (text.slice(i, i + 6).toUpperCase() === "PRAGMA") {
      const semi = text.indexOf(";", i);
      i = semi === -1 ? n : semi + 1;
      continue;
    }
    break;
  }
  return text.slice(i);
}

/** True if seed, once leading comments/PRAGMAs are skipped, starts with a
 * `BEGIN` statement — i.e. it already manages its own transaction and must
 * NOT be wrapped in another one (SQLite rejects a nested BEGIN). Ported
 * from `offshoot.pytest_plugin._seed_opens_own_transaction`. */
function seedOpensOwnTransaction(seed: string): boolean {
  const rest = skipLeadingNoise(seed);
  const head = rest.slice(0, 5).toUpperCase();
  if (head !== "BEGIN") return false;
  if (rest.length === 5) return true;
  return !/[A-Za-z0-9_]/.test(rest[5]!);
}

/** Run a SQL-string seed against dbPath via the `sqlite3` CLI, wrapping it
 * in one transaction unless it already opens its own — see
 * {@link seedOpensOwnTransaction}'s doc comment and
 * `offshoot.pytest_plugin._run_seed`'s for why this matters: an unwrapped
 * multi-statement seed runs every statement as its own autocommit
 * transaction, which the capture engine has to observe and encode
 * individually — measured ~100x slower for a couple hundred statements. */
function runSeedSql(dbPath: string, sql: string): void {
  const wrapped = seedOpensOwnTransaction(sql) ? sql : `BEGIN;\n${sql}\n;\nCOMMIT;\n`;
  execSqlite3([dbPath], wrapped);
}

/** A seed value, resolved once into a fingerprint (for `seedOnce`'s
 * memoization mismatch check) and a `run` step (what actually writes the
 * seed) — resolving both from a single pass over `seed` so a `.sql`-path
 * seed is read from disk exactly once per `seedOnce` call, not twice (once
 * to fingerprint, once to run).
 *
 * Fingerprinting: string seeds are fingerprinted by content hash of the
 * RESOLVED SQL text — i.e. a `.sql`-path seed is read from disk FIRST,
 * then hashed — not the raw argument. Hashing the raw path string instead
 * would fingerprint only the path's own text, so editing the file on disk
 * between two `seedOnce` calls for the same name and path would leave the
 * fingerprint unchanged and the mismatch would silently pass — exactly the
 * class of bug this check exists to catch. Callables are fingerprinted by
 * identity (mirrors Python's `id()`-based fingerprint). */
async function resolveSeed(seed: Seed): Promise<{ fingerprint: string; run: (session: Session) => Promise<void> }> {
  if (typeof seed === "function") {
    let id = callableIds.get(seed);
    if (id === undefined) {
      id = nextCallableId++;
      callableIds.set(seed, id);
    }
    return {
      fingerprint: `callable:${id}`,
      run: async (session) => {
        await seed(session.path);
      },
    };
  }
  const sql = await resolveSeedSql(seed);
  return {
    fingerprint: `sql:${createHash("sha256").update(sql, "utf8").digest("hex")}`,
    run: async (session) => {
      runSeedSql(session.path, sql);
    },
  };
}

/** Session-scoped-in-spirit named-seed factory: `seedOnce(daemon, {name,
 * seed})`. The first call for a given (daemon, name) pair creates database
 * `eval-{name}`, runs seed, and checkpoints it "seed"; later calls for the
 * same name are a pure memoization hit and return the same handle —
 * UNLESS a later call passes a seed that doesn't match what actually
 * seeded that name (fingerprinted: SQL/path text by RESOLVED content hash
 * — see {@link resolveSeed}'s doc comment — callables by identity), which
 * throws {@link OffshootError} rather than silently keeping the first
 * seed. Mirrors `offshoot.pytest_plugin.offshoot_db`, minus the ini-file
 * default-seed convenience (there's no pytest.ini equivalent here — every
 * call names its seed explicitly). */
export async function seedOnce(daemon: DaemonHandle, opts: SeedOnceOptions): Promise<SeedHandle> {
  const name = opts.name ?? "default";
  const cache = cacheFor(daemon);
  const resolved = await resolveSeed(opts.seed);
  const cached = cache.get(name);
  if (cached) {
    if (cached.fingerprint !== resolved.fingerprint) {
      throw new OffshootError(
        `seedOnce(name=${JSON.stringify(name)}): called again with a DIFFERENT seed than the ` +
          `one that already seeded ${cached.handle.db} earlier. seedOnce memoizes by name, not ` +
          `by seed content — pass a different name for a different seed, or the identical seed ` +
          `on every call for ${JSON.stringify(name)}.`,
      );
    }
    return cached.handle;
  }
  const client = await connectWithTail(daemon);
  try {
    const db = `eval-${name}`;
    await client.create(db);
    const session = await client.open(db, "main");
    try {
      await resolved.run(session);
      await session.flush("seed");
    } finally {
      await session.close();
    }
    const handle: SeedHandle = { db, checkpoint: "seed", name };
    cache.set(name, { handle, fingerprint: resolved.fingerprint });
    return handle;
  } finally {
    await client.close();
  }
}

function requireCachedSeed(daemon: DaemonHandle, name: string): SeedHandle {
  const cached = cacheFor(daemon).get(name);
  if (!cached) {
    throw new OffshootError(
      `forkPerTest: no seed named ${JSON.stringify(name)} on this daemon — call ` +
        `seedOnce(daemon, { name: ${JSON.stringify(name)}, seed }) before ` +
        `forkPerTest(daemon, ${JSON.stringify(name)}).`,
    );
  }
  return cached.handle;
}

// --------------------------------------------------------------------------
// forkPerTest
// --------------------------------------------------------------------------

export interface ForkPerTestOptions {
  /** A readable hint folded into the branch name (sanitized; a hash suffix
   * still guarantees distinctness even if this is omitted, empty, or
   * reused). */
  name?: string;
  /** A Go duration string (e.g. "30m", "2h"). Default "1h". */
  ttl?: string;
}

/** What {@link forkPerTest} returns: a writable, isolated branch forked
 * from a seed checkpoint, with a live session already open on it. */
export interface ForkedSession {
  readonly path: string;
  readonly db: string;
  readonly branch: string;
  /** The connection this fork's session (and its eventual close/destroy)
   * runs on — exposed for callers that want to issue extra calls (e.g.
   * `branches()`) without opening a second connection. */
  readonly client: Client;
  /** Flush this fork's checkout to a durable snapshot, optionally as a
   * named checkpoint, without waiting for close(). A thin passthrough to
   * the underlying session's `flush`. */
  flush(name?: string, opts?: FlushOptions): Promise<number>;
  /** Close the session and destroy the branch. Never throws: a failure in
   * either step is a `console.warn`, not an exception — call this from
   * your framework's afterEach without a surrounding try/catch. The TTL
   * passed to {@link forkPerTest} (default 1h) is the backstop for a
   * destroy that warns instead of succeeding. */
  close(): Promise<void>;
}

const DEFAULT_TTL = "1h";
const MAX_WORKER_LEN = 20;
// Anything outside this set gets collapsed to a single '-' by sanitize.
const UNSAFE_RUN = /[^a-z0-9-]+/g;
const MAX_BODY = 40;
const HASH_LEN = 8;

/** Map an arbitrary hint to a `store.ValidateName`-safe, [a-z0-9-] string,
 * with a short content hash suffix so two different hints that collapse to
 * the same body still produce distinct names. Deterministic. Ported from
 * `offshoot.langgraph._sanitize` (there is no TypeScript langgraph module
 * to import this from, so it's duplicated here rather than reaching across
 * an unrelated module boundary). */
function sanitize(raw: string): string {
  const lowered = raw.toLowerCase();
  let body = lowered.replace(UNSAFE_RUN, "-").replace(/^-+|-+$/g, "");
  if (!body) body = "id";
  body = body.slice(0, MAX_BODY).replace(/^-+|-+$/g, "") || "id";
  const digest = createHash("sha256").update(raw, "utf8").digest("hex").slice(0, HASH_LEN);
  return `${body}-${digest}`;
}

/** The xdist-equivalent worker id for this process: Vitest's pool id, else
 * Jest's worker id, else "local". Folded into every fork's branch name so
 * two workers forking concurrently never collide. */
function workerId(): string {
  return process.env.VITEST_POOL_ID ?? process.env.JEST_WORKER_ID ?? "local";
}

/** Worker-safe, `store.ValidateName`-safe branch name for the n-th fork on
 * this daemon: `t-{worker}-{sanitized-hint}-{n}`. `n` (a per-daemon
 * counter — see `forkCounters`) is what actually guarantees distinctness
 * across repeated calls with the same or no hint; sanitize's hash suffix
 * only protects against two DIFFERENT hints colliding after truncation. */
function branchName(worker: string, hint: string, n: number): string {
  const safeWorker = worker.toLowerCase().replace(UNSAFE_RUN, "-").replace(/^-+|-+$/g, "").slice(0, MAX_WORKER_LEN) || "w";
  return `t-${safeWorker}-${sanitize(hint)}-${n}`;
}

const forkCounters = new WeakMap<DaemonHandle, number>();

function nextForkIndex(daemon: DaemonHandle): number {
  const n = forkCounters.get(daemon) ?? 0;
  forkCounters.set(daemon, n + 1);
  return n;
}

async function warnClose(fn: () => Promise<void>, message: string): Promise<void> {
  try {
    await fn();
  } catch (e) {
    console.warn(`${message}: ${(e as Error).message}`);
  }
}

/** Fork a fresh, worker-safe-named branch from seedHandleOrName's checkpoint
 * with a TTL (default "1h", opts.ttl overrides), open a session on it, and
 * return a {@link ForkedSession}. seedHandleOrName is either a
 * {@link SeedHandle} (from {@link seedOnce}) or a plain string naming an
 * already-`seedOnce`d name on this same daemon — equivalent to calling
 * `forkPerTest(daemon, (await seedOnce(daemon, {name})).db...)` except it
 * throws a clear error naming the missing seed instead of a confusing "no
 * seed given" one, since (unlike the pytest plugin's `offshoot_fork`)
 * there's no ini-configured default seed this could silently fall back to.
 *
 * Call `.close()` on the returned handle yourself, typically from your
 * framework's afterEach — see this module's top doc comment. */
export async function forkPerTest(
  daemon: DaemonHandle,
  seedHandleOrName: SeedHandle | string,
  opts: ForkPerTestOptions = {},
): Promise<ForkedSession> {
  const seedHandle = typeof seedHandleOrName === "string" ? requireCachedSeed(daemon, seedHandleOrName) : seedHandleOrName;
  const client = await connectWithTail(daemon);
  const branch = branchName(workerId(), opts.name ?? seedHandle.name, nextForkIndex(daemon));
  const ttl = opts.ttl ?? DEFAULT_TTL;

  let session: Session;
  try {
    await client.fork(seedHandle.db, "main", branch, { from: seedHandle.checkpoint, ttl });
    session = await client.open(seedHandle.db, branch);
  } catch (e) {
    const tail = daemon.stderrTail();
    await client.close();
    if (e instanceof OffshootError && tail) {
      throw new OffshootError(`${e.message}\n--- offshoot daemon stderr tail ---\n${tail}`);
    }
    throw e;
  }

  let closed = false;
  return {
    path: session.path,
    db: seedHandle.db,
    branch,
    client,
    flush: (name = "", flushOpts?: FlushOptions) => session.flush(name, flushOpts),
    async close() {
      if (closed) return;
      closed = true;
      // Each of these three steps is independently best-effort: no
      // failure in one may stop the others from running, and none of
      // them may throw out of close() — see this function's doc comment
      // and the module doc comment's note on per-fork teardown isolation.
      await warnClose(
        () => session.close(),
        `offshoot testkit: closing the session on ${seedHandle.db}@${branch} failed during teardown`,
      );
      await warnClose(
        () => client.destroy(seedHandle.db, branch, { force: true }),
        `offshoot testkit: destroying branch ${seedHandle.db}@${branch} failed during teardown ` +
          "(it will leak until its TTL expires)",
      );
      await warnClose(
        () => client.close(),
        `offshoot testkit: closing the connection for ${seedHandle.db}@${branch} failed during teardown`,
      );
    },
  };
}

// --------------------------------------------------------------------------
// dump
// --------------------------------------------------------------------------

/** `sqlite3 <path> .dump`'s text output — THE way to compare two
 * offshoot-materialized SQLite files (e.g. two `Client.export()`/
 * `checkoutAt()` outputs, or a fresh fork's checkout against a golden
 * file) in a test. SQLite's on-disk bytes are not deterministic across
 * writes with identical logical content (page layout, freelist order,
 * vacuum state can all differ) — NEVER byte-compare two exported/checked-
 * out `.db` files. Compare `await dump(a) === await dump(b)` instead (or
 * diff the two strings for a readable failure). Requires the `sqlite3` CLI
 * on PATH — throws {@link OffshootError} with install guidance if it's
 * missing, rather than a bare ENOENT. */
export async function dump(path: string): Promise<string> {
  return execSqlite3([path, ".dump"]);
}
