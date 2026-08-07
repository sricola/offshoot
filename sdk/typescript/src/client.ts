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
  return new Client(sock);
}

interface Waiter {
  res: (v: any) => void;
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

  constructor(private readonly sock: Socket) {
    sock.setEncoding("utf8");
    sock.on("data", (chunk: string) => {
      this.buf += chunk;
      let i: number;
      while ((i = this.buf.indexOf("\n")) >= 0) {
        const line = this.buf.slice(0, i);
        this.buf = this.buf.slice(i + 1);
        const waiter = this.queue.shift();
        if (!waiter) continue;
        let resp: any;
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

  /** Send one request and resolve with its response. Public (but
   * underscore-prefixed, matching the Python SDK's `_call` convention) so
   * {@link Session} can issue requests through the same connection/queue
   * without an `as any` escape hatch. Not intended for use outside this
   * package's own client code. */
  _call(op: string, fields: Record<string, unknown> = {}): Promise<any> {
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
    return new Session(this, resp.checkout, db, branch);
  }

  /** Materialize db@branch's head snapshot at rest; returns its path. */
  async checkout(db: string, branch: string): Promise<string> {
    const resp = await this._call("checkout", { db, branch });
    return resp.checkout;
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
    const raw: any[] = resp.branches ?? [];
    return raw.map((b) => ({
      branch: b.branch,
      head_txid: b.head_txid ?? 0,
      protected: b.protected ?? false,
      ttl: b.ttl ?? "",
      ttl_remaining: b.ttl_remaining ?? "",
      lease_holder: b.lease_holder ?? "",
      checkpoints: b.checkpoints ?? [],
      touched_at: b.touched_at ?? "",
      checkpoints_v2: (b.checkpoints_v2 ?? []).map((cp: any) => ({
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
