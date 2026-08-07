// Thin client for the offshoot daemon's lifecycle API.
//
// Wire protocol: newline-delimited JSON over a unix socket; one request, one
// response, no pipelining (matches internal/daemon/protocol.go). Zero
// runtime dependencies.

import { createConnection, type Socket } from "node:net";

/** An error returned by the daemon, a transport failure, or a malformed
 * response — every failure mode this client can produce surfaces as this
 * one type, uniformly, whether or not the daemon ever sent an ok:false
 * response. */
export class OffshootError extends Error {
  override readonly name = "OffshootError";
}

/** One checkpoint's entry within {@link Branch.checkpoints_v2}.
 *
 * Mirrors `internal/daemon/protocol.go`'s `CheckpointInfo`. `created_at` is
 * RFC3339 UTC, empty for a checkpoint that predates per-checkpoint
 * timestamps.
 */
export interface CheckpointInfo {
  name: string;
  txid: number;
  created_at: string;
}

/** One branch of one db, as returned by {@link Client.branches}.
 *
 * Mirrors `internal/daemon/protocol.go`'s `BranchInfo`. `ttl` is the
 * canonical `time.Duration.String()` re-render (e.g. a fork requested with
 * ttl "1h" reads back here as "1h0m0s") — safe to echo straight back into a
 * future {@link Client.fork}/{@link Client.touch} call.
 *
 * `checkpoints` (bare names) and `checkpoints_v2` (name/txid/created_at)
 * describe the exact same set of checkpoints — `checkpoints` stays for
 * wire/API compat with code written before `checkpoints_v2` existed; new
 * code should prefer `checkpoints_v2`.
 *
 * `state` is this branch's computed state — one of `"active"`, `"pending"`,
 * `"error"`, `"dirty"`, `"detached"`, or `"idle"`; see
 * `internal/ops/status.go`'s `BranchStateAt` for the full taxonomy and
 * precedence. `""` against a pre-Milestone-4 daemon that never sends this
 * field at all (wire-additive: an old daemon still answers every other
 * field exactly as before).
 */
export interface Branch {
  branch: string;
  head_txid: number;
  protected: boolean;
  ttl: string;
  ttl_remaining: string;
  lease_holder: string;
  checkpoints: string[];
  touched_at: string;
  checkpoints_v2: CheckpointInfo[];
  state: string;
}

/** One event from the daemon's event stream — see {@link Client.events}.
 *
 * Mirrors `internal/daemon/events.go`'s `Event`: the ONE versioned JSON
 * schema (`v` is currently always `1`) the daemon emits over BOTH the unix
 * socket `subscribe` op and HTTP `GET /events` (SSE) — see
 * `docs/reference.md`'s Eventing section for the full `type` table
 * (`session_opened`, `flushed`, `flush_failed`, `fenced`, `session_closed`,
 * `reaped`, `evicted` (reserved — nothing emits it yet), and the terminal
 * `dropped_slow_consumer` — see {@link Client.events}'s doc comment for how
 * that one is handled).
 *
 * `ts` is RFC3339 UTC. `db`/`branch` are omitted for event types that carry
 * no branch (e.g. `dropped_slow_consumer`). `detail` is omitted when the
 * daemon sends none (events with no extra detail, e.g. `reaped`).
 */
export interface OffshootEvent {
  v: number;
  ts: string;
  type: string;
  db?: string;
  branch?: string;
  detail?: Record<string, unknown>;
}

/** One session open in the daemon, as returned by {@link Client.status}. */
export interface SessionInfo {
  db: string;
  branch: string;
  checkout: string;
  holder: string;
  epoch: number;
  durable_txid: number;
  error?: string;
}

/** Options for {@link Client.fork}. */
export interface ForkOptions {
  /** Source checkpoint name; omitted (or "") means source branch's head. */
  from?: string;
  /** A Go duration string (e.g. "1h"); omitted means no TTL. */
  ttl?: string;
  /** A small string->string map describing the new branch's lineage (e.g.
   * eval run id, git SHA, agent id), capped server-side (ops.ValidateMeta:
   * at most 32 keys, keys <= 64 bytes, values <= 512 bytes) and stored on
   * the new branch's ref. Omitted means no metadata. */
  meta?: Record<string, string>;
}

/** Options for {@link Session.flush}. */
export interface FlushOptions {
  /** A small string->string map, meaningful only alongside a non-empty
   * checkpoint name — it is stored on the resulting named checkpoint
   * (same caps as {@link ForkOptions.meta}). Passing meta with no name is
   * rejected by the daemon — there is no checkpoint for it to attach to. */
  meta?: Record<string, string>;
}

/** Options for {@link Client.destroy}. */
export interface DestroyOptions {
  /** Override the protected-branch/live-lease refusal. */
  force?: boolean;
}

/** Options for {@link Client.promote}. */
export interface PromoteOptions {
  /** Override the protected-target refusal. */
  force?: boolean;
}

/** Options for {@link Client.touch}. */
export interface TouchOptions {
  /** A Go duration string to set the TTL, "none" to clear it, or omitted to
   * keep the branch's current TTL. */
  ttl?: string;
}

/** Options for {@link Client.export}. */
export interface ExportOptions {
  /** Checkpoint to export; omitted (or "") means the branch's head. */
  checkpoint?: string;
  /** Overwrite an existing file at outPath. Without this, export refuses
   * to overwrite. */
  force?: boolean;
}

/** Options for {@link Client.checkoutAt}. */
export interface CheckoutAtOptions {
  /** Re-materialize an already-cached read-only checkout file. Without
   * this, an existing cache file for the same (db, branch, checkpoint) is
   * returned as-is with no store access. */
  force?: boolean;
}

/** Open a connection to the offshoot daemon listening on socketPath. */
export async function connect(socketPath: string): Promise<Client> {
  const sock = createConnection(socketPath);
  await new Promise<void>((res, rej) => {
    sock.once("connect", () => {
      sock.off("error", rej);
      res();
    });
    sock.once("error", rej);
  });
  return new Client(sock, socketPath);
}

// --------------------------------------------------------------------------
// Internal wire types
// --------------------------------------------------------------------------
//
// The interfaces below mirror internal/daemon/protocol.go's Response and
// its embedded structs EXACTLY as JSON arrives off the socket — every
// field that Go struct tags `omitempty` is optional here too, and none of
// this is exported. They exist purely so `_call`'s Promise<RawResponse>
// return type gives every call site in this file a real, typed field to
// read (`resp.checkout`, `resp.branches`, ...) instead of `any`. These are
// NOT the same types as the public {@link Branch}, {@link CheckpointInfo},
// {@link SessionInfo} a couple of them get normalized into above — e.g.
// RawBranchInfo's fields are all optional pre-normalization; Branch's
// aren't.

/** @internal Wire shape of one entry in Response.CheckpointsV2. */
interface RawCheckpointInfo {
  name: string;
  txid?: number;
  created_at?: string;
}

/** @internal Wire shape of one entry in Response.Branches. */
interface RawBranchInfo {
  branch: string;
  head_txid?: number;
  protected?: boolean;
  ttl?: string;
  ttl_remaining?: string;
  lease_holder?: string;
  checkpoints?: string[];
  touched_at?: string;
  checkpoints_v2?: RawCheckpointInfo[];
  state?: string;
}

/** @internal Wire shape of one entry in Response.Sessions — structurally
 * compatible with the public {@link SessionInfo} (every field SessionInfo
 * requires is required here too), so `status()` can return it unmodified. */
interface RawSessionInfo {
  db: string;
  branch: string;
  checkout: string;
  holder: string;
  epoch: number;
  durable_txid: number;
  error?: string;
}

/** @internal The daemon's raw JSON response line — mirrors
 * internal/daemon/protocol.go's `Response` struct. Every op populates a
 * different subset of these fields (see that struct's doc comment for
 * which); every field but `ok` stays optional here rather than chasing a
 * true per-op union, since each call site below already knows — and
 * defaults, via the exact same `??`/`!` it used before this type existed —
 * which fields its own op actually guarantees. */
interface RawResponse {
  ok: boolean;
  error?: string;
  checkout?: string;
  txid?: number;
  sessions?: RawSessionInfo[];
  branches?: RawBranchInfo[];
  databases?: string[];
}

/** @internal The subscribe op's one-line ack, read off the socket before
 * the connection leaves request/response mode — see {@link Client.events}. */
type RawAck = Pick<RawResponse, "ok" | "error">;

/** @internal One event line's wire shape, exactly as JSON.parse returns it
 * before {@link Client.events} narrows it into the public
 * {@link OffshootEvent} it yields. */
interface RawEvent {
  v: number;
  ts: string;
  type: string;
  db?: string;
  branch?: string;
  detail?: Record<string, unknown>;
}

interface Waiter {
  res: (v: RawResponse) => void;
  rej: (e: Error) => void;
}

/** A connection to one offshoot daemon.
 *
 * Correlation note: the daemon answers strictly in the order requests were
 * received on a connection (see protocol.go: "no pipelining ... the client
 * must wait for a Response before sending the next Request"). This client
 * still serializes writes through {@link Client._call} awaiting each
 * promise's caller before issuing the next request in practice, but even if
 * two calls were ever in flight at once, a simple FIFO queue of waiters is
 * sufficient to match each incoming response line to the request that
 * caused it, because the daemon never reorders its replies on one socket.
 * Do not share a single Client across concurrent callers that don't await
 * between calls unless you rely on exactly this in-order guarantee.
 */
export class Client {
  private buf = "";
  private readonly queue: Waiter[] = [];
  private closed = false;

  constructor(
    private readonly sock: Socket,
    /** The daemon's unix-socket path this Client connected to — retained
     * only so {@link Client.events} can dial its own fresh, dedicated
     * connection to the SAME daemon (see that method's doc comment for
     * why it can't reuse `sock` above). `undefined` for a Client built by
     * a test double that never goes through {@link connect} (e.g.
     * `Object.create(Client.prototype)` in client.test.ts) — such a
     * double never calls `events()` either. */
    private readonly socketPath?: string,
  ) {
    sock.setEncoding("utf8");
    sock.on("data", (chunk: string) => {
      this.buf += chunk;
      let i: number;
      while ((i = this.buf.indexOf("\n")) >= 0) {
        const line = this.buf.slice(0, i);
        this.buf = this.buf.slice(i + 1);
        const waiter = this.queue.shift();
        if (!waiter) continue;
        let resp: RawResponse;
        try {
          resp = JSON.parse(line);
        } catch (e) {
          waiter.rej(new OffshootError(`daemon sent a malformed response: ${(e as Error).message}`));
          continue;
        }
        if (!resp.ok) waiter.rej(new OffshootError(resp.error ?? "unknown daemon error"));
        else waiter.res(resp);
      }
    });
    sock.on("error", (e) => this.failAll(new OffshootError(`daemon connection failed: ${e.message}`)));
    sock.on("close", () => this.failAll(new OffshootError("daemon closed the connection")));
  }

  private failAll(e: Error): void {
    this.closed = true;
    for (const w of this.queue.splice(0)) w.rej(e);
  }

  /**
   * Send one request and resolve with its response. Public (but
   * underscore-prefixed, matching the Python SDK's `_call` convention) so
   * {@link Session} can issue requests through the same connection/queue
   * without an `as any` escape hatch. Not intended for use outside this
   * package's own client code.
   *
   * Stays a real, callable method at runtime, and still fully typed for
   * this package's own test files (which compile straight against this
   * source, not against the stripped declaration output below) — only the
   * PUBLISHED `.d.ts` loses it, via the `@internal` tag below plus
   * tsconfig.build.json's `stripInternal`.
   *
   * @internal
   */
  _call(op: string, fields: Record<string, unknown> = {}): Promise<RawResponse> {
    if (this.closed) {
      return Promise.reject(new OffshootError("daemon connection failed: socket is closed"));
    }
    const req: Record<string, unknown> = { op };
    for (const [k, v] of Object.entries(fields)) {
      if (v !== undefined && v !== "" && v !== false) req[k] = v;
    }
    return new Promise((res, rej) => {
      this.queue.push({ res, rej });
      this.sock.write(JSON.stringify(req) + "\n", (err) => {
        if (err) {
          const idx = this.queue.findIndex((w) => w.res === res);
          if (idx >= 0) this.queue.splice(idx, 1);
          rej(new OffshootError(`daemon connection failed: ${err.message}`));
        }
      });
    });
  }

  /** Create a fresh db (branch main at txid 1). */
  async create(db: string): Promise<void> {
    await this._call("create", { db });
  }

  /** Open a live session on db@branch; returns its Session. */
  async open(db: string, branch = "main"): Promise<Session> {
    const resp = await this._call("open", { db, branch });
    // "open" always populates checkout on an ok:true response — see
    // protocol.go's Response.Checkout — same non-defaulted assumption this
    // line always made back when resp was `any`, now spelled out as a
    // non-null assertion instead of an implicit one.
    return new Session(this, resp.checkout!, db, branch);
  }

  /** Materialize db@branch's head snapshot at rest; returns its path. */
  async checkout(db: string, branch: string): Promise<string> {
    const resp = await this._call("checkout", { db, branch });
    return resp.checkout!;
  }

  /** Branch `newBranch` off db@source (at opts.from, or source's head).
   *
   * Returns the fork point's txid.
   */
  async fork(db: string, source: string, newBranch: string, opts: ForkOptions = {}): Promise<number> {
    const resp = await this._call("fork", {
      db,
      branch: source,
      name: newBranch,
      from: opts.from ?? "",
      ttl: opts.ttl ?? "",
      meta: opts.meta,
    });
    return resp.txid ?? 0;
  }

  /** Delete db@branch. opts.force overrides the protected-branch refusal. */
  async destroy(db: string, branch: string, opts: DestroyOptions = {}): Promise<void> {
    await this._call("destroy", { db, branch, force: opts.force ?? false });
  }

  /** Repoint db@branch at checkpoint `to`; returns the refreshed checkout path. */
  async rollback(db: string, branch: string, to: string): Promise<string> {
    const resp = await this._call("rollback", { db, branch, name: to });
    return resp.checkout ?? "";
  }

  /** Repoint db@onto at db@source's head; returns the promoted txid. */
  async promote(db: string, source: string, onto: string, opts: PromoteOptions = {}): Promise<number> {
    const resp = await this._call("promote", { db, branch: source, name: onto, force: opts.force ?? false });
    return resp.txid ?? 0;
  }

  /** Reset db@branch's activity clock, optionally setting/clearing its TTL.
   *
   * opts.ttl: omitted keeps the current TTL, a Go duration string sets it,
   * "none" clears it.
   */
  async touch(db: string, branch: string, opts: TouchOptions = {}): Promise<void> {
    await this._call("touch", { db, branch, ttl: opts.ttl ?? "" });
  }

  /** List every branch of db. */
  async branches(db: string): Promise<Branch[]> {
    const resp = await this._call("branches", { db });
    const raw: RawBranchInfo[] = resp.branches ?? [];
    return raw.map((b) => ({
      branch: b.branch,
      head_txid: b.head_txid ?? 0,
      protected: b.protected ?? false,
      ttl: b.ttl ?? "",
      ttl_remaining: b.ttl_remaining ?? "",
      lease_holder: b.lease_holder ?? "",
      checkpoints: b.checkpoints ?? [],
      touched_at: b.touched_at ?? "",
      checkpoints_v2: (b.checkpoints_v2 ?? []).map((cp) => ({
        name: cp.name,
        txid: cp.txid ?? 0,
        created_at: cp.created_at ?? "",
      })),
      state: b.state ?? "",
    }));
  }

  /** List every database this store has at least one ref for, sorted. */
  async dbs(): Promise<string[]> {
    const resp = await this._call("dbs");
    return resp.databases ?? [];
  }

  /** Materialize db@branch's state at opts.checkpoint (omitted = head) to a
   * plain SQLite file at outPath, server-side, on the daemon's own host/
   * filesystem — outPath must be an ABSOLUTE path (same-host/same-user
   * unix-socket trust model; see internal/daemon/protocol.go's
   * `Request.Path`). Refuses to overwrite an existing outPath unless
   * opts.force. No sidecar, no lease — outPath has no ongoing relationship
   * to the store.
   *
   * This reads the branch's last DURABLE state from the store, never a
   * live session's checkout: if a session on db@branch has unflushed
   * writes, they are NOT in the export. Flush (or checkpoint) first if you
   * need them included. */
  async export(db: string, branch: string, outPath: string, opts: ExportOptions = {}): Promise<void> {
    await this._call("export", {
      db,
      branch,
      name: opts.checkpoint ?? "",
      path: outPath,
      force: opts.force ?? false,
    });
  }

  /** Materialize db@branch's state at checkpoint into a dedicated
   * read-only cache file, distinct from (and never touching) the branch's
   * writable checkout — safe to call alongside an open session on the
   * same branch. Repeat calls with opts.force unset (or false) return the
   * cached path as-is (a checkpoint's content is immutable, so no store
   * access is needed); opts.force re-materializes.
   *
   * Returns the read-only cache file's path. */
  async checkoutAt(db: string, branch: string, checkpoint: string, opts: CheckoutAtOptions = {}): Promise<string> {
    const resp = await this._call("checkout-at", { db, branch, name: checkpoint, force: opts.force ?? false });
    return resp.checkout ?? "";
  }

  /** Stream the daemon's event feed as an async iterator, yielding one
   * {@link OffshootEvent} per line, in publish order, until the stream
   * ends.
   *
   * *** OPENS ITS OWN, DEDICATED SOCKET CONNECTION *** — never this
   * Client's own connection. The daemon's `subscribe` op
   * (`internal/daemon/events.go`/`server.go`) permanently takes a
   * connection out of request/response mode the instant it acks: no
   * further `open`/`flush`/`status`/... call could ever get a reply on
   * that same connection again. This method dials a fresh unix-socket
   * connection to the same path this Client was built with (via
   * {@link connect}), precisely so callers never have to think about
   * that — keep using this same `Client` for ordinary ops exactly as
   * before, and iterate the async iterator this method returns for
   * events; the two never share a socket.
   *
   * ```ts
   * for await (const ev of client.events()) {
   *   console.log(ev.type, ev.db, ev.branch, ev.detail);
   * }
   * ```
   *
   * **`dropped_slow_consumer`** (this consumer fell behind the daemon's
   * bounded per-subscriber buffer and was dropped — see
   * `docs/reference.md`'s Eventing section): yielded like any other
   * event, and then the iterator ends (a plain return, not a thrown
   * error) — the daemon has already closed its end, there is nothing
   * more to read. This is the unambiguous contract: a caller that cares
   * whether it was dropped checks `ev.type === "dropped_slow_consumer"`
   * on the last event it received; a caller that doesn't care just sees
   * the loop end normally. It is deliberately NOT thrown as an
   * {@link OffshootError} — a drop ends the stream the same way an
   * ordinary disconnect does; only a genuine transport/protocol failure
   * (a connect error, a malformed line, a daemon `ok:false` ack) throws.
   *
   * **Closing early:** `break` out of a `for await` loop (or call
   * `.return()` on the returned iterator directly) to close the
   * dedicated socket. Per the async-iterator protocol, that calls this
   * generator's own `return()`, which resumes at the suspended `yield`
   * as an abrupt completion — the `finally` block below always runs in
   * response and destroys the underlying socket, so no file descriptor
   * is leaked by stopping early. */
  async *events(): AsyncGenerator<OffshootEvent, void, void> {
    if (!this.socketPath) {
      throw new OffshootError(
        "events(): this Client was not constructed via connect() (no socketPath on record); " +
          "cannot open a dedicated subscribe connection",
      );
    }
    const sock = createConnection(this.socketPath);
    try {
      await new Promise<void>((res, rej) => {
        sock.once("connect", () => {
          sock.off("error", rej);
          res();
        });
        sock.once("error", rej);
      });
    } catch (e) {
      sock.destroy();
      throw new OffshootError(`daemon connection failed: ${(e as Error).message}`);
    }
    try {
      sock.setEncoding("utf8");
      await new Promise<void>((res, rej) => {
        sock.write(JSON.stringify({ op: "subscribe" }) + "\n", (err) => {
          if (err) rej(err);
          else res();
        });
      });

      let buf = "";
      let sawAck = false;
      for await (const chunk of sock) {
        buf += String(chunk);
        let i: number;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i);
          buf = buf.slice(i + 1);
          if (!sawAck) {
            sawAck = true;
            let ack: RawAck;
            try {
              ack = JSON.parse(line);
            } catch (e) {
              throw new OffshootError(`daemon sent a malformed response: ${(e as Error).message}`);
            }
            if (!ack.ok) throw new OffshootError(ack.error ?? "unknown daemon error");
            continue;
          }
          let raw: RawEvent;
          try {
            raw = JSON.parse(line);
          } catch (e) {
            throw new OffshootError(`daemon sent a malformed event: ${(e as Error).message}`);
          }
          const ev: OffshootEvent = { v: raw.v, ts: raw.ts, type: raw.type };
          if (raw.db !== undefined) ev.db = raw.db;
          if (raw.branch !== undefined) ev.branch = raw.branch;
          if (raw.detail !== undefined) ev.detail = raw.detail;
          yield ev;
          if (ev.type === "dropped_slow_consumer") return;
        }
      }
      // The stream ended (remote closed) without ever throwing above. If
      // that happened before the subscribe ack was ever consumed, this is
      // NOT a legitimate empty stream (a subscriber that acked, then saw
      // zero events before a real disconnect, is fine and returns
      // normally) -- it's the daemon accepting the connection, reading
      // the subscribe request, and closing without ever acking (a crash,
      // a shutdown race, a transport hiccup). Silently completing here
      // would contradict this method's own contract ("only a genuine
      // transport/protocol failure ... throws") and mask a real daemon
      // failure as a successful-looking empty stream -- mirrors Python's
      // events()'s identical `if not line: raise OffshootError(...)`
      // check on the ack read.
      if (!sawAck) {
        throw new OffshootError("daemon closed the connection");
      }
    } catch (e) {
      if (e instanceof OffshootError) throw e;
      throw new OffshootError(`daemon connection failed: ${(e as Error).message}`);
    } finally {
      sock.destroy();
    }
  }

  /** List every session open in the daemon. */
  async status(): Promise<SessionInfo[]> {
    const resp = await this._call("status");
    return resp.sessions ?? [];
  }

  /** Close the connection to the daemon. */
  async close(): Promise<void> {
    this.closed = true;
    this.sock.destroy();
  }
}

/** A live daemon session: a lease plus a checkout under continuous capture. */
export class Session {
  constructor(
    private readonly client: Client,
    public readonly path: string,
    private readonly db: string,
    private readonly branch: string,
  ) {}

  /** Flush the checkout to a durable snapshot; returns its txid. */
  async flush(name = "", opts: FlushOptions = {}): Promise<number> {
    const r = await this.client._call("flush", { db: this.db, branch: this.branch, name, meta: opts.meta });
    return r.txid ?? 0;
  }

  /** Close the session, releasing its lease. */
  async close(): Promise<void> {
    await this.client._call("close", { db: this.db, branch: this.branch });
  }
}
