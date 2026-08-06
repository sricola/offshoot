# CLI reference

Every command below is verified against `cmd/offshoot/main.go`'s usage
strings and command dispatch — flags, arities, and defaults are taken from
the code, not from memory. If this doc and `offshoot`'s own `-h`-style usage
text ever disagree, the code wins; please file an issue.

## Global

```
offshoot [-store SPEC] <command> [args...]
```

`-store SPEC` selects the store. It can appear anywhere in the argument
list (it's extracted before the subcommand is parsed), so `offshoot create
app -store ./mystore` and `offshoot -store ./mystore create app` are
equivalent. If omitted, offshoot uses the `OFFSHOOT_STORE` environment
variable, and falls back to `./.offshoot` if that's unset too.

**Store spec forms:**

| Form | Meaning |
|---|---|
| `path` or `./path` | Local directory (relative or absolute, no scheme) |
| `file:///abs/path` | Local directory, explicit scheme |
| `s3://bucket/prefix` | S3-compatible bucket (AWS S3, R2, Tigris, MinIO) |

Any other URL scheme is refused with `unsupported store scheme`.

Every command except `init` first attaches to the store: it opens the
backend and runs a **CAS (compare-and-swap) capability probe**. This runs on
every invocation — a fresh CLI process re-pays it every time — and refuses
to proceed if the store doesn't enforce conditional writes, rather than
silently degrading to a weaker guarantee. A long-lived daemon (`offshoot
serve`) pays this cost once per process instead of once per command.

**S3 environment variables** (only consulted for `s3://` specs):

| Variable | Meaning |
|---|---|
| `OFFSHOOT_S3_ENDPOINT` | Custom endpoint (R2, Tigris, MinIO); unset means AWS's default endpoint |
| `OFFSHOOT_S3_REGION` | Region; defaults to `auto` when a custom endpoint is set |
| `OFFSHOOT_S3_PATH_STYLE` | Truthy (`1`, `true`, `yes`, `on`, case-insensitive) selects path-style addressing (needed for MinIO) |

Credentials are never read from an offshoot-specific variable — they come
from the AWS SDK's default chain (environment, shared config/credentials
file, IAM role).

**Other environment variables:**

| Variable | Meaning |
|---|---|
| `OFFSHOOT_STORE` | Default store spec when `-store` isn't passed |
| `OFFSHOOT_CHECKOUTS` | Where checkouts are materialized, for a *remote* (`s3://`) store; local stores always keep checkouts under the store directory itself. Defaults to a per-store directory under the user cache dir, keyed by the store's resolved identity (endpoint/region/path-style included, not just the literal spec string) |
| `OFFSHOOT_SOCKET` | Overrides the daemon socket path for `offshoot serve`, `offshoot session ...`, and `offshoot mcp`; if unset, all three derive the same default path from the store spec, so they agree without it |

**Naming rules**, enforced on every database name, branch name, and
checkpoint name: 1–128 characters, charset `[a-z0-9-_.]`, and never exactly
`.` or `..` or containing `..` as a substring (those are directory-traversal
segments once joined into a storage key). A bad name fails fast with `store:
invalid name ...`.

---

## `offshoot init`

```
offshoot init
```

Creates a new store at the resolved spec (a directory, or a bucket/prefix)
and writes its manifest (layout version, creation time) with a create-only
conditional write. Must be run once before any other command against a
fresh store. Running it again against an already-initialized store fails
(the manifest write loses its CAS) rather than silently succeeding — don't
script `init` unconditionally before every command.

**Errors:** manifest already exists (already initialized); any store-attach
failure (e.g. the CAS probe failing against a bucket without conditional
writes).

## `offshoot create <db> [--from file]`

```
offshoot create app
offshoot create app --from existing.db
```

Creates a new, empty database with a `main` branch at transaction id 1,
protected by default (destroying or promoting onto `main` requires
`--force`). With `--from file`, **imports** an existing SQLite file instead:
the source is copied (including `-wal`/`-shm` if present), the copy is
quiesced with a full WAL checkpoint, and *that* becomes the root snapshot —
the source file itself is never modified or truncated. There is no mode that
overwrites an existing user file.

**Errors:** refuses if `db` already exists (ref CAS conflict); `--from` with
a source file that doesn't exist or isn't a valid SQLite file.

## `offshoot checkout <db>[@branch]` / `offshoot path <db>[@branch]`

```
offshoot checkout app
offshoot checkout app@attempt-1
offshoot path app@attempt-1
```

`checkout` materializes `db@branch`'s current head to its fixed local path
and prints that path. `path` prints the same fixed path *without*
materializing — useful for scripting against a checkout you know is already
current. `branch` defaults to `main` when omitted (`db` alone means
`db@main`).

The checkout path is always `<store-root>/checkouts/<db>/<branch>.db`
(local stores) or under `OFFSHOOT_CHECKOUTS` / the cache dir (remote
stores) — never used as an identifier, always re-derivable from `db@branch`.

If a checkout already exists at that path, `checkout` requires it to be
quiescent first (no live writer holding it open) — re-materializing renames
a fresh file into place, which would delete a live writer's WAL out from
under it. If the existing checkout has un-checkpointed local edits, the head
state still wins, but a warning is printed to stderr first since those edits
are about to be overwritten.

**Errors:** no such `db@branch`; checkout is busy (a live connection is
holding it) — closes connections and retry.

## `offshoot checkpoint <db>[@branch] <name>`

```
offshoot checkpoint app v1
```

Snapshots the *current checkout's* state (not just the ref) as a named
checkpoint, quiescing the checkout first (busy timeout ~3s, then a clean
failure rather than a hang). Checkpoint names are unique per branch — this
is the only operation actually named "checkpoint"; continuous background
capture by the daemon is called "flush"/"commit," not "checkpoint." At rest
(no daemon), every checkpoint writes a full snapshot, because there's no
capture engine tracking which pages changed since the last one; the daemon's
`session flush` writes incremental segments instead (see [What a flush
costs](../README.md#what-a-flush-costs) in the README).

Children never inherit a parent's checkpoints — a fork's storage begins at
its fork point, so a checkpoint made before the fork isn't materializable
from the child; resolve it on the parent instead.

**Errors:** checkpoint name already exists on this branch; no checkout
exists yet (run `checkout` first); checkout is busy.

## `offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration]`

```
offshoot fork app attempt-1
offshoot fork app attempt-1 --at v1
offshoot fork app attempt-1 --ttl 2h
```

Creates `new-branch` as an independent branch from `db@branch`'s head, or
from a named checkpoint via `--at`. The fork **materializes**: the child
gets its own storage lineage, seeded by copying the source's snapshot state,
and never references the parent's storage again afterward — destroying the
parent never endangers a child. `branch` (the source) defaults to `main`.

If forking at head (no `--at`) and the source's local checkout has
un-checkpointed changes or is busy, a warning is printed and the fork
proceeds from the branch's last *committed* (ref) state, not whatever's
sitting uncommitted in the checkout.

`--ttl duration` sets the **child's** TTL (never the parent's — forking
doesn't reset the parent's activity clock either). Fork has no `"none"`
sentinel the way `touch` does — a brand-new branch has no existing TTL to
explicitly clear — so omit `--ttl` entirely for "no TTL." A non-positive
duration (`0s`, a negative value, or the literal string `none`) is refused
outright rather than silently treated as "no TTL," since that would let a
caller believe they set a TTL when they didn't.

**Errors:** `new-branch` already exists; unknown `--at` checkpoint name;
non-positive or `"none"` `--ttl` value.

## `offshoot touch <db>[@branch] [--ttl duration|none]`

```
offshoot touch app@attempt-1
offshoot touch app@attempt-1 --ttl 30m
offshoot touch app@attempt-1 --ttl none
```

Resets a branch's activity clock (deferring TTL-based reaping) without
changing anything else. With `--ttl duration`, also sets a new TTL. With
`--ttl none`, clears the TTL entirely (the branch then lives until
destroyed). Without `--ttl`, the TTL is left as-is — only the clock resets.
This is the only way to defer expiry on a branch nobody currently has open
(a daemon session holding a branch renews its lease continuously, which
defers reaping on its own).

Output re-renders the TTL through Go's canonical `time.Duration.String()` —
a branch forked with `--ttl 1h` reads back as `ttl=1h0m0s`.

**Errors:** the branch is currently being reaped (a reaper already claimed
it) — "too late to touch."

## `offshoot rollback <db>[@branch] --to <checkpoint>`

```
offshoot rollback app@attempt-1 --to fork
```

Repoints the branch at a **new** lineage seeded from `checkpoint`'s state
(internally, the same machinery as fork). The old lineage is orphaned,
retained through the GC grace period, then collected. Checkpoints at or
before the target are kept (their snapshots are copied into the new
lineage so they survive the old lineage's eventual collection); checkpoints
after it are dropped. Any lease on the branch is cleared by the repoint, so
it's immediately acquirable afterward. Prints the (re-materialized) checkout
path.

The ref repoint (a CAS write) is the point of no return; the local checkout
refresh that follows is best-effort — if it fails (e.g. the checkout is
busy), the command reports a partial success: the branch *did* roll back,
but the checkout needs a manual `offshoot checkout` to catch up.

**Errors:** unknown checkpoint name; lost a concurrent CAS race (retry).

## `offshoot promote <db>@<source> --onto <target> [--force]`

```
offshoot promote app@attempt-1 --onto main --force
```

Repoints `target` at a **new** lineage seeded from `source`'s current head
(fork machinery again — this is why promoting never risks an epoch
collision). `source` survives unchanged (it's typically left to TTL-reap
later, or destroyed explicitly). `target`'s old lineage is orphaned and
later garbage-collected; its checkpoint map resets to just `{"promote":
<txid>}`. If `target` is protected (`main` is protected by default),
`--force` is required. `target`'s checkout, if any, is refreshed after a
busy probe — same best-effort semantics as rollback.

**Errors:** `source == target`; `target` is protected without `--force`;
target checkout is busy (repoint still lands; checkout refresh is skipped
and reported); lost a concurrent CAS race (retry).

## `offshoot destroy <db>[@branch] [--force]`

```
offshoot destroy app@attempt-1
offshoot destroy app@attempt-1 --force
```

Deletes the branch's ref and its local checkout files (`.db`, `-wal`,
`-shm`, `.sum`). Destroying a parent is always safe and allowed regardless
of live children — children are storage-independent once forked, so a
parent's destruction can never corrupt them. `--force` is required to
destroy a protected branch (`main` by default), and also to destroy a branch
under an active lease (a live holder may still be mid-write; without
`--force` this is refused outright).

**Errors:** protected without `--force`; live lease without `--force`;
checkout is busy (close connections first).

## `offshoot gc [--grace duration]`

```
offshoot gc
offshoot gc --grace 30m
```

Two steps in one command. First, **reap**: destroys every branch whose TTL
has expired (same logic the daemon's janitor runs on a timer — see `serve
-reap-every`); a failure reaping one branch (e.g. its checkout is busy) is
reported to stderr but doesn't stop the rest of GC. Second, **collect**:
two-phase garbage collection over storage lineages no longer referenced by
any ref — first tombstoned (marked, timestamped), then actually deleted once
a tombstone is older than `--grace` (default `1h`) *and* still unreferenced
at sweep time (a lineage re-referenced during the grace window, e.g. by a
fork racing GC, is left alone). `--grace 0` makes a lineage eligible for
deletion on the very next `gc` run after being tombstoned, rather than
disabling collection.

Prints which branches were reaped, and how many lineages were tombstoned vs.
actually deleted.

## `offshoot status`

```
offshoot status
```

Prints every branch across every database: head transaction id, named
checkpoints, `protected` and `checked-out` flags (when applicable), and —
for a branch with a TTL — the TTL itself and time remaining until it's
reap-eligible (`remaining=expired` once past the deadline). TTL remaining is
computed from the later of the branch's last-touch time and its lease
expiry, whichever is later — matching exactly what the janitor's reap logic
uses, so `status` never disagrees with what will actually happen.

## `offshoot lease list`

```
offshoot lease list
```

Lists every branch currently carrying a lease record: holder identity
(`<hostname>/<pid>` by convention), state (`held` or `expired`), epoch, and
expiry timestamp. A lease with a corrupt (unparseable) expiry is listed as
expired with a warning to stderr, rather than hiding the branch or crashing
the listing.

## `offshoot lease acquire <db>[@branch] [--ttl 30s]`

```
offshoot lease acquire app@main
offshoot lease acquire app@main --ttl 60s
```

Claims (or renews, if already held by this same identity) a lease on the
branch, bumping its epoch. `--ttl` defaults to 30 seconds
(`ops.DefaultLeaseTTL`) if omitted. **This command exits immediately** — it
does not hold the process open — so the lease will simply expire unless
something else renews it before then. It exists for inspection and for
deliberately breaking/reclaiming a stuck lease (acquiring bumps the epoch,
fencing out whatever previously held it), not for long-running write
sessions — use `offshoot serve` + `session open` for that.

## `offshoot lease release <db>[@branch]`

```
offshoot lease release app@main
```

Releases the branch's current lease (looked up first via the same listing
`lease list` uses).

**Errors:** no lease currently held on that branch.

## `offshoot serve [-socket PATH] [-reap-every DURATION] [-gc-grace DURATION]`

```
offshoot serve
offshoot serve -socket /tmp/o.sock
offshoot serve -reap-every 1m -gc-grace 15m   # both are the defaults
```

Starts the daemon: a long-running process that serves a unix socket (mode
`0600`) for `session ...` commands, holds branch leases, captures every
committed WAL transaction continuously, and runs the janitor. Blocks until
`SIGINT`/`SIGTERM`, at which point it releases every lease and shuts down
cleanly (closing live sessions, draining in-flight opens, removing the
socket) rather than leaving stale leases behind.

`-socket PATH` overrides the default socket location (see `OFFSHOOT_SOCKET`
above); if a `session` command needs to reach this daemon, it must be given
the same `-socket PATH` (or `OFFSHOOT_SOCKET`) — there's no other way for
the CLI to discover a non-default socket.

`-reap-every` sets the janitor's interval for both TTL reaping and the
periodic GC sweep (default `1m`); `-reap-every 0` disables the janitor
entirely (GC and reaping are still available on demand via `offshoot gc`).
`-gc-grace` is the tombstone grace period the janitor's GC pass uses
(default `15m`) — see `offshoot gc` above for what grace means.

**Errors:** socket path already in use by another listener; underlying
store-attach failure.

## `offshoot mcp`

```
offshoot mcp [-default-ttl DURATION|none] [-socket PATH]
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

Serves the Model Context Protocol on stdio: seven tools (`offshoot_list`,
`offshoot_checkout`, `offshoot_checkpoint`, `offshoot_fork`,
`offshoot_rollback`, `offshoot_promote`, `offshoot_destroy`), each described
so a model knows not just what it does but *when* to reach for it (fork
before risky work, checkpoint when tests pass, roll back when they fail,
promote the attempt that worked). Destructive tools honor the same
protected-branch rules as the CLI — an unforced `offshoot_promote --onto
main` or `offshoot_destroy` on `main` is refused, and the refusal is
returned to the agent as the tool result, not a transport-level error.

Agent-initiated forks carry a TTL by default: `offshoot_fork` applies
`-default-ttl` (default `24h`) to any call that omits its own `ttl`
argument, so a branch an agent forks and forgets is eligible for reaping
instead of accumulating forever. `-default-ttl 0` or `-default-ttl none`
disables the default; an individual `offshoot_fork` call can still override
it either way — an explicit `ttl:"<duration>"` always wins, and
`ttl:"none"` always yields no TTL even under a configured default. The
fork tool's response echoes the applied TTL and, when there is one, the
computed expiry timestamp, so both land in the agent's own transcript.
**TTL alone does not reap anything**: reaping requires a running janitor
(`offshoot serve`); `offshoot mcp` runs no daemon of its own, so a
daemonless setup only sweeps expired branches when `offshoot gc` is run by
hand.

**MCP rides a running daemon, but only for a branch a session is already
open on.** No MCP tool ever opens a session itself (that's a harness's job —
the SDKs, `offshoot session open`, or a custom loop); each call to
`offshoot_checkpoint`, `offshoot_fork`, or `offshoot_checkout` freshly
checks whether the daemon named by `-socket` (default: the same socket
`offshoot serve` derives for this store) has one open for the branch in
question. If so, `offshoot_checkpoint` flushes it live through the daemon
instead of writing a full at-rest snapshot; `offshoot_fork` forks through
the daemon, which flushes an open source session first; `offshoot_checkout`
returns that session's own live checkout path. Without an already-open
session — the common case for a bare `offshoot mcp` — every one of those
tools behaves exactly as it does with no daemon running at all; see
[docs/status.md](status.md) for what's tested.

## `offshoot session open <db>[@branch] [-socket PATH]`

```
offshoot session open app
```

Opens a live daemon session on `db@branch`: acquires its lease, materializes
(or reuses) its checkout, and starts continuous WAL capture. Prints the
checkout path. Requires a running `offshoot serve` (reachable at the
resolved socket). `branch` defaults to `main`.

**Errors:** the branch is already open by this daemon; the branch's lease is
held elsewhere; the daemon is shutting down; no daemon reachable at the
socket.

## `offshoot session flush <db>[@branch] [name] [-socket PATH]`

```
offshoot session flush app
offshoot session flush app v1
```

Flushes the session's pending WAL to a durable snapshot or incremental
segment in the store — writes since the last flush are committed to SQLite
but not durable in the bucket until this runs. An optional `name` also
records a named checkpoint at the resulting transaction id (same checkpoint
namespace as `offshoot checkpoint`). Prints the transaction id now durable.
The daemon writes a full snapshot every 16th flush and an incremental
segment (only the changed pages) otherwise, so materializing state never
replays more than one snapshot plus fifteen segments.

**Errors:** `db@branch` is not open here; the session has lost its lease
(fenced — it will not write under a dead epoch).

## `offshoot session status [-socket PATH]`

```
offshoot session status
```

Lists every session currently open on this daemon: `db@branch`, durable
transaction id, epoch, lease holder, checkout path, and — if the session has
hit an error (e.g. fenced by a lost lease, or a contract violation) — that
error inline.

## `offshoot session close <db>[@branch] [-socket PATH]`

```
offshoot session close app
```

Closes the session and releases its lease. `branch` defaults to `main`.

**Errors:** `db@branch` is not open here.

## `offshoot session shutdown [-socket PATH]`

```
offshoot session shutdown
```

Asks the daemon to shut down gracefully (equivalent to sending it
`SIGINT`/`SIGTERM`): releases every lease, closes every session, removes the
socket.

---

## What's not here

`offshoot version`, HTTP endpoints, an auth flag, and a metrics endpoint do
not exist in the CLI today — see [docs/status.md](status.md) for the full
implemented/deferred matrix and links to the roadmap milestones tracking
each.
