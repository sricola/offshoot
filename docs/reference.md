# CLI reference

Every command below is verified against `cmd/offshoot/main.go`'s usage
strings and command dispatch — flags, arities, and defaults are taken from
the code, not from memory. If this doc and `offshoot`'s own `-h`-style usage
text ever disagree, the code wins; please file an issue.

Looking for the narrative walkthrough instead of a flag-by-flag reference —
install, seed-once-fork-many with the pytest/testkit fixtures, xdist/vitest
parallelism, golden-file assertions, CI — see
[docs/eval-harness.md](eval-harness.md).

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
| `OFFSHOOT_TOKEN` | The Bearer token for `offshoot serve -http`, in place of `-token`; see [`-http ADDR`](#-http-addr--opt-in-http-listener-milestone-4-task-3) below |

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

### Read-only historical checkout: `--at <checkpoint> --read-only [--force]`

```
offshoot checkout app@main --at v1 --read-only
```

Materializes a NAMED checkpoint (never the head, and never omitted — `--at`
has no "current head" alias the way `export` does) into a SEPARATE,
dedicated read-only cache path — `<store-root>/checkouts-ro/<db>/<branch>@<checkpoint>.db`
— and prints that path. `--at` and `--read-only` must be given together;
either alone is refused as a malformed command. This path is never the
writable `checkouts/<db>/<branch>.db` path `checkout` (without `--at`) uses,
and this command never touches that path, its `.sum` sidecar, or a live
session's open file descriptors on it — safe to run alongside `offshoot mcp`
or a daemon session on the same branch.

The result is `chmod 0444` (read-only) and has no `.sum` sidecar and no
lease — it has no ongoing relationship to the store once written, and the
whole `checkouts-ro` tree is safe to `rm -rf` at any time; the next call for
anything under it just rebuilds what it needs. A repeat call for the same
`db@branch@checkpoint` is a cache hit (returned as-is, no store access at
all) unless `--force` is given, which re-materializes unconditionally — see
[docs/status.md](status.md) for the exact staleness caveat this cache
convenience accepts (a branch destroyed and recreated with a
same-named checkpoint can leave a stale cache entry; `--force` or deleting
the cache file clears it).

**Errors:** `--at` without `--read-only` or vice versa; no such checkpoint on
`db@branch`.

## `offshoot export <db>[@branch[@checkpoint]] <out.db> [--force]`

```
offshoot export app out.db
offshoot export app@attempt-1 out.db
offshoot export app@attempt-1@v1 out.db --force
```

Copies `db@branch`'s state at `checkpoint` (third `@`-separated component;
omitted means the branch's current head) out to a plain SQLite file at
`out.db`, anywhere on the local filesystem. Unlike `checkout`, the result has
ZERO ongoing relationship to the store afterward: no `.sum` sidecar, no
lease, nothing else in this codebase will ever look at `out.db` again — it's
a one-shot copy-out, not a checkout.

Refuses to overwrite an existing `out.db` unless `--force`. The write itself
is always atomic regardless of `--force`: it's built via a temp file in
`out.db`'s OWN directory, renamed into place only once every chain member
has been fetched and its checksum verified — a failed export (a fetch
error, a checksum mismatch) never leaves a truncated or partial file at
`out.db`, and the rename is guaranteed same-filesystem (same directory).

**Errors:** no such `db@branch`; no such checkpoint; `out.db` already exists
and `--force` was not given.

## `offshoot diff <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary]`

```
offshoot diff app@attempt-1@v1 app@attempt-2@v1
offshoot diff app@attempt-1@v1 app@attempt-2@v1 --summary
offshoot diff app@attempt-1 app@attempt-2                # both at head
offshoot diff evals@golden@v1 candidate@main@final       # cross-db is legit
```

Materializes both sides READ-ONLY through the same primitives `export`/
`checkout --at --read-only` use (never a live checkout, never a lease — safe
alongside an open daemon session on either branch) and either streams
`sqldiff`'s output over them (default) or prints a stdlib-only table-level
row-count summary (`--summary`, no `sqldiff` dependency at all;
row-counts-only — equal counts with different values still report `same`,
use the default `sqldiff` mode for content). Each target uses the same
triple-`@` form `export` does — `db` alone means `db@main` head, `db@branch`
means that branch's head, `db@branch@checkpoint` means that named
checkpoint. The two targets may name the same `db` or two different ones.
Full walkthrough, the raw by-hand recipe, and the exact staleness rule for a
head-side (no-checkpoint) target: [docs/diff.md](diff.md).

Before either mode's own output, a header line names which raw target
string is which side: `left:  <target1> right: <target2>` (verbatim, using
exactly what was typed on the command line — not a normalized/expanded
form). `--summary`'s own table header row reuses those same two strings as
its count columns instead of bare `LEFT`/`RIGHT`, so the table stays
self-describing even scrolled away from the header line above it.

**Default mode** requires the separate `sqldiff` binary on PATH (NOT
included by installing plain `sqlite3` on every platform) — its absence is a
clear, per-OS-hinted error (`sudo apt-get install sqlite3-tools` on Debian/
Ubuntu, `brew install sqldiff` on macOS — both verified, not guessed; see
[docs/diff.md](diff.md#default-mode-sqldiff)) naming `--summary` as the
`sqldiff`-free alternative.

**Errors:** no such `db@branch` or checkpoint on either side; `sqldiff` not
on PATH (default mode only — `--summary` never needs it).

## `offshoot checkpoint <db>[@branch] <name> [--meta k=v ...]`

```
offshoot checkpoint app v1
offshoot checkpoint app v1 --meta eval_run=42 --meta git_sha=abc123
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

Every checkpoint records a creation timestamp (`created_at`, RFC3339 UTC)
automatically. `--meta k=v` is repeatable and attaches a small string→string
map to *this specific checkpoint* (e.g. an eval run id, a git SHA, an agent
id) — capped at 32 keys, 64-byte keys, 512-byte values, enforced before
anything is written; a rejected `--meta` leaves the branch untouched. This is
branch/checkpoint-level metadata, not row-level provenance — see the design
spec's metadata note.

Children never inherit a parent's checkpoints — a fork's storage begins at
its fork point, so a checkpoint made before the fork isn't materializable
from the child; resolve it on the parent instead.

**Errors:** checkpoint name already exists on this branch; no checkout
exists yet (run `checkout` first); checkout is busy; `--meta` over a cap
(key count, key length, or value length).

## `offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration] [--meta k=v ...]`

```
offshoot fork app attempt-1
offshoot fork app attempt-1 --at v1
offshoot fork app attempt-1 --ttl 2h
offshoot fork app attempt-1 --meta eval_run=42 --meta git_sha=abc123
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

`--meta k=v` (repeatable, same caps as `checkpoint`'s) attaches a small
string→string map to the **new branch's own lineage** — this describes the
branch, not the auto-created `fork` checkpoint (which still gets its own
`created_at`, but no metadata of its own). This is the same knob
`branches`/the daemon `fork` op and SDK `fork()` calls expose; `branches`
output surfaces each checkpoint's `created_at`/`txid` (`checkpoints_v2`) but
not raw metadata values today — read them back via the store directly, or a
future op, if you need to query by value.

**Errors:** `new-branch` already exists; unknown `--at` checkpoint name;
non-positive or `"none"` `--ttl` value; `--meta` over a cap.

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
`--force` is required. Promote never checks `target`'s lease at all — with
or without `--force` — and any lease on `target` is cleared by the repoint
(the same unconditional clearing rollback does; see above), so `target` is
immediately acquirable afterward regardless of who held it. `target`'s
checkout, if any, is refreshed after a busy probe — same best-effort
semantics as rollback.

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

## `offshoot status [-ro-cache-budget BYTES]`

```
offshoot status
offshoot status -ro-cache-budget 500MB
```

Prints every branch across every database: its computed **state** (see
below), head transaction id, named checkpoints, `protected` and
`checked-out` flags (when applicable), and — for a branch with a TTL — the
TTL itself and time remaining until it's reap-eligible
(`remaining=expired` once past the deadline). TTL remaining is computed
from the later of the branch's last-touch time and its lease expiry,
whichever is later — matching exactly what the janitor's reap logic uses,
so `status` never disagrees with what will actually happen.

After the branch listing, a final `ro-cache: N entries, B bytes used
(budget: unlimited)` line (Milestone 4 Task 5) reports `checkouts-ro`'s
current usage (`ops.Workspace.ROCacheUsage`, an at-rest read — no daemon
required, exactly like the rest of this command). `-ro-cache-budget` is
**display-only** here: it echoes back `serve -ro-cache-budget`'s own value
and grammar (see below) so an operator can see usage against the budget
they intend to run with, without needing a live daemon connection — the
budget itself is never persisted anywhere (like every other `serve` tuning
flag), so this at-rest command has no other way to know what a *running*
daemon was actually started with. Omitted, the line reads
`(budget: unlimited)` regardless of what any running daemon's own
`-ro-cache-budget` is actually set to.

### Branch states

Every branch is in exactly one of six states, computed fresh on every call
— nothing about state is persisted anywhere. `offshoot status`
(`ops.Workspace.Status`, CLI/at-rest — see above) and the daemon's
`branches` op (`BranchInfo.state`; Python `Client.branches()`'s
`Branch.state`, TypeScript `Client.branches()`'s `Branch.state`) report the
identical computation for the states both can see; a daemon additionally
knows two states no at-rest computation can, since they depend on its own
in-memory session map:

| State | Meaning | Who can report it |
|---|---|---|
| `active` | The branch's ref carries a live lease — someone (in or out of a daemon) holds it right now. | Both |
| `pending` | This daemon has reserved a session slot for the branch and is still inside its (slow) `session.Open` — no live session yet, but the branch is spoken for. | Daemon only |
| `error` | A session is open here and its `Err()` is non-nil (lease loss, a capture failure, any terminal session failure). | Daemon only |
| `dirty` | No live lease; a checkout exists whose sidecar-recorded identity (lineage/epoch/txid) matches the ref but whose content hash doesn't — un-checkpointed local edits. | Both |
| `detached` | No live lease; a checkout exists whose sidecar-recorded **lineage** doesn't match the ref's current lineage — a checkout orphaned by a `rollback`/`promote` that repointed the branch at a new lineage before (or without) refreshing this checkout (their own checkout refresh is best-effort and can be skipped by a busy checkout at repoint time). | Both |
| `idle` | None of the above. | Both |

**Precedence** when more than one condition technically holds, most to
least specific: `error` > `pending` > `active` > `dirty` > `detached` >
`idle`. In practice `error` and `pending` can never both apply to the same
branch at once (a daemon's session map holds at most one entry per
`db@branch`, either a reservation or a live session, never both) — the
ordering matters for `active` vs. `dirty`/`detached`: a branch can be both
leased AND locally modified/orphaned at the file level, and `active` wins
the report.

**`idle` is a deliberate addition, not in the original design spec.** The
spec's branch-state taxonomy (`active`/`pending`/`dirty`/`detached`/
`error`) assumed a daemon was always running to layer a state over every
branch. `offshoot status`'s CLI/at-rest mode has no daemon and no session
map, so a branch with nothing going on (never checked out, never leased)
still needs a name to report — `idle` is that name. It also covers a
case checkout-staleness detection can't cleanly attribute to `detached`:
a checkout whose sidecar-recorded lineage still matches the ref, but whose
epoch/txid lags behind (the branch advanced within the SAME lineage — e.g.
a daemon session's flush — since this checkout was last refreshed), isn't
orphaned, just stale; it needs a re-materialize (`offshoot checkout`), not
a warning that its parent branch moved out from under it. That case reads
`idle`, not `detached`.

**`dirty` and `detached` are structurally mutually exclusive** on any
single evaluation: a lineage mismatch reports `detached` immediately,
before the identity/hash comparison that could ever produce `dirty` runs
at all.

**Known blind spot:** a checkout with no readable `.sum` sidecar at all
(never stamped, or corrupt/legacy) always reads `idle`, even if its
content has actually diverged from the ref — there's no sidecar to detect
that against. This is a deliberate "no evidence, stay silent" stance
(mirrors the sidecar mechanism's own internal "unknown" verdict for the
identical input), not an oversight; it's only reachable via a checkout
materialized entirely outside `offshoot checkout`/`checkpoint`/`rollback`/
`promote`, all of which always stamp a sidecar.

**Cost:** determining `dirty` (once a checkout's sidecar identity already
matches the ref) requires a real WAL checkpoint (`wal_checkpoint(TRUNCATE)`,
up to a 3-second busy timeout) followed by a full SHA-256 hash of the
checkout's content — a committed write can sit in a checkout's WAL,
invisible to a bare hash of the main file, until something folds it in, so
skipping the checkpoint step would misreport a genuinely dirty checkout as
idle. This runs **per branch**, on every `offshoot status` / `branches`
call, for every branch that is checked out, unleased, and not already
`detached` — i.e. exactly the branches where "is it dirty" is still an
open question. A store with many large, checked-out, unleased branches
will feel this on every call. There is no cheap short-circuit: file
size/mtime cannot substitute for a real hash (mtime is exactly what this
codebase's own `.last-used`-style touch-file conventions elsewhere work
around, not something trustworthy for content identity). If the checkpoint
attempt itself reports busy — a live connection is actively using that
unleased checkout right now — the state is reported as `dirty` directly
(without a hash), on the reasoning that active, untracked use of an
unleased checkout IS itself the kind of activity `dirty` exists to
surface, even though the exact byte diff can't be observed at that
instant.

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

## `offshoot serve [-socket PATH] [-reap-every DURATION] [-gc-grace DURATION] [-flush-every DURATION] [-ro-cache-budget BYTES] [-http ADDR] [-token TOKEN] [-http-allow-non-loopback]`

```
offshoot serve
offshoot serve -socket /tmp/o.sock
offshoot serve -reap-every 1m -gc-grace 15m -flush-every 30s   # all three are the defaults
offshoot serve -http 127.0.0.1:8080                            # opt-in HTTP: token auto-generated, printed once
OFFSHOOT_TOKEN=$(openssl rand -hex 32) offshoot serve -http 127.0.0.1:8080
```

Starts the daemon: a long-running process that serves a unix socket (mode
`0600`) for `session ...` commands, holds branch leases, captures every
committed WAL transaction continuously, and runs the janitor. Blocks until
`SIGINT`/`SIGTERM`, at which point it releases every lease and shuts down
cleanly (closing live sessions, draining in-flight opens, removing the
socket, and closing any HTTP listener) rather than leaving stale leases
behind.

`-socket PATH` overrides the default socket location (see `OFFSHOOT_SOCKET`
above); if a `session` command needs to reach this daemon, it must be given
the same `-socket PATH` (or `OFFSHOOT_SOCKET`) — there's no other way for
the CLI to discover a non-default socket.

`-reap-every` sets the janitor's interval for both TTL reaping and the
periodic GC sweep (default `1m`); `-reap-every 0` disables the janitor
entirely (GC and reaping are still available on demand via `offshoot gc`).
`-gc-grace` is the tombstone grace period the janitor's GC pass uses
(default `15m`) — see `offshoot gc` above for what grace means.

`-flush-every` sets the background-flush cadence applied to every session
this daemon opens (default `30s`, `0` disables, a negative value is a usage
error): each open session ships whatever it's captured but not yet flushed
on that timer, without the agent ever calling `session flush` itself. It
bounds exposure — worst case, a daemon that dies loses at most one
`-flush-every` interval's worth of committed-but-unflushed writes, instead
of everything since the last manual flush. See [What a flush
costs](../README.md#what-a-flush-costs) in the README for what a background
flush (and every session's mandatory first "settling" flush) actually cost.

**Errors:** socket path already in use by another listener; underlying
store-attach failure; `-flush-every` given a negative duration;
`-ro-cache-budget` given a negative value.

### `-ro-cache-budget BYTES` — checkouts-ro disk budget (Milestone 4 Task 5)

```
offshoot serve -ro-cache-budget 0            # unlimited (the default)
offshoot serve -ro-cache-budget 536870912    # 512 MiB, in bytes
offshoot serve -ro-cache-budget 500MB        # same idea, via a size suffix
```

Bounds `checkouts-ro` — the read-only cache `offshoot checkout --at
--read-only` / the daemon's `checkout-at` op materializes into (see
[above](#read-only-historical-checkout---at-checkpoint---read-only---force))
— which otherwise grows without bound: one file per distinct
`db@branch@checkpoint` ever cached, never reclaimed on its own. **Never
`checkouts/`** (the writable, leased tree `checkout`/`session open` use) —
see the writable-never-evicted guarantee below.

Default `0` means unlimited, matching `CheckoutAt`'s own unbounded-by-default
behavior — the janitor still computes and reports usage every pass (see
below), it just never evicts. A bare integer is bytes, the contract this
flag, `offshoot_ro_cache_bytes`, and every eviction event/log line all
speak; as a convenience, a trailing power-of-1024 size suffix is also
accepted, case-insensitively: `K`/`KB`, `M`/`MB`, `G`/`GB`, `T`/`TB` (each
multiplies by 1024^n — **not** the SI decimal 1000-based convention some
tools use for the same letters; pass raw bytes if that distinction
matters to you). `offshoot status`'s own `-ro-cache-budget` flag accepts
the identical grammar, purely for display (see `offshoot status` above) —
it is never persisted, so that at-rest command has no other way to know
what a running daemon was actually started with.

**When it runs:** on the same cadence as reap/GC — every `-reap-every`
tick (the janitor loop; `-reap-every 0` disables the janitor entirely, so
no ro-cache pass runs either, exactly like reap/GC). Each pass:

1. Computes `checkouts-ro`'s total current size and sets
   `offshoot_ro_cache_bytes` to it — **always**, even when the budget is
   `0` or usage is already under it. This is a once-per-janitor-pass
   gauge, not a scrape-time one: between passes it can lag real usage by
   up to `-reap-every`'s interval (e.g. a `CheckoutAt` call materializing a
   large new entry moments after a pass won't be reflected until the
   next one).
2. If usage exceeds the budget, evicts entries **oldest-by-LRU-clock
   first** (see below) until usage is back at or under it.

**The LRU clock (PM Amendment 11, pre-decided): a `.last-used`
touch-on-HIT marker file, not the cache file's own mtime.** Every
force=false `CheckoutAt` cache HIT (a repeat call for an already-cached
`db@branch@checkpoint`) touches a `<cachefile>.last-used` sidecar file to
the current time. This exists because the cache file's own mtime is set
exactly once, by the materialize that created it — a cache HIT is a pure
read of an already-`chmod 0444` file and must never write through it again
— so without a separate marker, "least recently used" would silently
collapse to "least recently created," exactly backwards for a cache whose
entire purpose is that a checkpoint hit over and over stays hot. Ranking:
the marker's mtime when one exists, falling back to the cache file's own
mtime (the materialize timestamp) as the floor for an entry that has never
been hit since it was created. A cache file with **no** marker therefore
ranks no more recently than its own creation time — the correct, and only
sensible, floor.

**`checkouts/` is never evicted — by construction, not a runtime check.**
The eviction pass only ever lists and removes files under `checkouts-ro`
(a directory tree, and a filename shape — `<branch>@<checkpoint>.db` — that
`checkouts/<db>/<branch>.db` can never collide with; see the read-only
checkout section above for the same separation stated from `CheckoutAt`'s
side). There is no code path in the eviction pass that can construct, or
be handed, a `checkouts/` path at all — a leased, currently-open session's
writable checkout survives even the most aggressive budget (e.g. `1`, which
forces every `checkouts-ro` entry out) untouched, by the same guarantee.

**Eviction is loud**: one `offshoot: janitor: ro-cache: evicted
<db>@<branch>@<checkpoint> (<bytes> bytes)` line to stderr per entry
removed, `offshoot_ro_cache_evictions_total` (a counter) incremented once
per entry, and an `evicted` event published on the [event
bus](#eventing-subscribe-op--get-events) (`{type:"evicted", db, branch,
detail:{checkpoint, bytes}}`) — subscribe (socket or `GET /events`) to
watch evictions happen in real time. Each eviction removes both the cache
file and its `.last-used` marker together.

**TOCTOU under a configured budget:** a path `checkout --at --read-only` /
`checkout-at` returns — from a fresh materialize OR a cache hit — is not a
guarantee the file still exists by the time you open it once a nonzero
budget is running: a concurrent janitor pass can evict that exact entry
in the window between the call returning and your own open. This is the
accepted cost of turning what used to be a purely caller/operator-driven,
manually-`rm -rf`-safe cache into one an automatic background reclaimer
also touches. Two rules cover it completely:

- Already opened the file? Keep reading — POSIX unlink-of-an-open-file
  semantics mean an eviction racing your open connection never corrupts
  or truncates what you already have a handle on; it just stops being
  visible to a future `open`/`stat` on that path.
- Get `ENOENT` trying to open a path you were just handed? That's not
  data loss or a corrupted store — it means "evicted since that call
  returned." Re-call `checkout --at --read-only` (or the `checkout-at`
  op): the checkpoint's content is immutable, so re-materializing
  produces byte-identical content, not stale or different data.

A `.last-used` touch (a cache hit) landing during the SAME pass that's
about to evict that exact entry usually spares it (the janitor re-checks
the marker immediately before removing anything); a touch landing in the
last, microscopic instant right before the actual delete does not save
it that round, but is still safe per the two rules above — the entry
simply gets re-hit as a fresh materialize on the next `checkout-at` call,
and self-heals into staying hot from then on.

`checkouts-ro` remains safe to `rm -rf` at any time regardless of the
budget (see the read-only checkout section above) — a budget just
automates what that manual cleanup would otherwise require doing by hand.

### `-http ADDR` — opt-in HTTP listener (Milestone 4 Task 3)

Off by default. `-http ADDR` (e.g. `127.0.0.1:8080`) starts an HTTP
listener alongside the unix socket, exposing:

| Method | Path | Auth | Body |
|---|---|---|---|
| `POST` | `/rpc` | Bearer | The same `Request`/`Response` JSON the unix socket speaks, one op per POST (`Content-Type: application/json` required; body capped at 1MiB, oversized -> `413`) |
| `GET` | `/metrics` | Bearer | Prometheus text exposition of the locked `offshoot_*` metric set (see [docs/status.md](status.md)'s metrics row for the full name list) |
| `GET` | `/healthz` | **none** | `{"ok":true,"sessions":N}` — the one endpoint that needs no token, for liveness probes |
| `GET` | `/events` | Bearer | Server-Sent Events: the daemon's event stream (Milestone 4 Task 4a — see [Eventing](#eventing-subscribe-op--get-events) below) |
| `GET` | `/debug/pprof/*` | Bearer | `net/http/pprof`'s standard handlers (index, cmdline, profile, symbol, trace) |

**Token:** `-token TOKEN` or `OFFSHOOT_TOKEN` sets it explicitly; if
neither is given, one is generated and printed to stderr **exactly once**
at startup — treat that line, and your terminal scrollback/shell history,
as sensitive. Every request but `/healthz` requires `Authorization: Bearer
<token>`, compared in constant time; the token is never logged again after
that one startup line — only an 8-character fingerprint (`offshoot: http
listening on ... (token fingerprint XXXXXXXX)`) appears in any later
output.

**Binding beyond localhost:** a loopback bind (`127.0.0.1`, `::1`,
`localhost`) needs nothing further. Any other address requires BOTH
`-http-allow-non-loopback` (an explicit acknowledgment) AND an explicit
`-token`/`OFFSHOOT_TOKEN` — the auto-generated, printed-once token is a
loopback-only convenience. Missing either is a distinct startup error (the
daemon never starts, rather than starting under-acknowledged):

```
$ offshoot serve -http 0.0.0.0:8080
offshoot: daemon: -http "0.0.0.0:8080" binds a non-loopback address; this requires
-http-allow-non-loopback (an explicit acknowledgment that this endpoint will be
reachable beyond localhost)

$ offshoot serve -http 0.0.0.0:8080 -http-allow-non-loopback
offshoot: daemon: -http "0.0.0.0:8080" with -http-allow-non-loopback also requires
an explicit -token or OFFSHOOT_TOKEN; the auto-generated, printed-once token is a
loopback-only convenience
```

**Threat model:** the token is a shared secret for a single-tenant,
same-host-or-trusted-network deployment — anyone holding it can do
everything any daemon client can do (identical to anyone able to open the
unix socket today), not a multi-tenant isolation boundary. There is no TLS;
a non-loopback bind should sit behind a trusted network boundary (VPN,
private subnet) of its own. **Treat stderr as sensitive at daemon
startup**: it is the only place a freshly auto-generated token is ever
printed in full.

`http.Server` runs explicit timeouts rather than the stdlib's unbounded
defaults: `ReadHeaderTimeout` 5s (slow-header protection), `ReadTimeout`
30s, `WriteTimeout` 90s (sized to leave headroom for
`/debug/pprof/profile`'s/`trace`'s default 30s capture window — a longer
`?seconds=` capture than that budget allows will be cut off; request a
shorter window or profile out-of-band), `IdleTimeout` 2 minutes.

**Errors:** everything `offshoot serve` can already error on, plus: HTTP
address already in use; the two non-loopback-bind errors above.

## Eventing (subscribe op / GET /events)

The daemon publishes one versioned JSON event per state transition it
observes, drainable two ways — the unix socket protocol's `subscribe` op,
and HTTP's `GET /events` (Server-Sent Events) — both carrying the exact
same JSON, encoded by the same function daemon-side (PM Amendment 4: one
schema, one encoder). There is no history/replay: a subscriber only ever
sees events published *after* it subscribes.

**Event shape:**

```json
{"v":1,"ts":"2026-08-07T12:00:00Z","type":"flushed","db":"app","branch":"main","detail":{"kind":"manual","txid":7,"duration_seconds":0.017}}
```

`v` is the schema version (currently always `1`); `ts` is RFC3339 UTC.
`type` is one of:

| `type` | Fired when | `detail` |
|---|---|---|
| `session_opened` | A session opens (daemon `open` op) | `holder`, `epoch` |
| `flushed` | A flush succeeds (manual `flush` op or background auto-flush) | `kind` (`manual`/`auto`), `txid`, `duration_seconds` |
| `flush_failed` | A flush fails | `kind`, `error`, `duration_seconds` |
| `fenced` | A session is fenced out by a lease it no longer holds | `cause` |
| `session_closed` | A session closes (daemon `close` op, or `shutdown`) | `error` (only if the close itself errored) |
| `reaped` | The janitor destroys a branch whose TTL expired | *(none)* |
| `evicted` | The janitor evicts a `checkouts-ro` entry over `-ro-cache-budget` | `checkpoint`, `bytes` |
| `dropped_slow_consumer` | Sent to a subscriber being dropped (see below), never to anyone else | *(none)* |

**Slow-subscriber drop:** publishing never blocks the daemon (a session
transition or the janitor). A subscriber whose bounded buffer (64 events)
is full when an event is published is dropped immediately: removed from
the subscriber set, sent exactly one terminal `dropped_slow_consumer`
event, and has its stream closed. This can never slow down or fail a
session's own progress or the janitor's own cadence — see
[docs/status.md](status.md)'s eventing row for the test that proves a
write-heavy session keeps flushing successfully while a subscriber that
never reads its channel gets dropped.

**Stalled (still-connected) subscriber:** the drop above handles a
subscriber's *bus-side* buffer filling up, but not a subscriber that stays
connected and simply stops reading its socket/HTTP connection at all — the
daemon's write to that connection could otherwise block forever once the
kernel's own send buffer fills. Every write to a subscriber's connection
(both transports) is bounded by a per-write deadline, re-armed immediately
before each send (default 45s); a write that cannot complete within that
window means the reader is genuinely stuck, and the daemon gives up on
that subscriber — closing the connection (socket) or ending the handler
(SSE) — rather than leaking the goroutine and file descriptor. A live
stream re-arms this deadline forward on every successful send (and, for
SSE, at least every keepalive tick), so it never affects a merely slow but
still-draining subscriber.

### Unix socket: the `subscribe` op

```jsonc
{"op": "subscribe"}
```

The daemon acks (`{"ok":true}`) and the connection then **permanently
leaves request/response mode**: from that point on it streams one JSON
event per line until the client disconnects. No further op can ever be
sent on that same connection — the daemon stops reading it entirely.

> **Use a dedicated connection.** `subscribe` is unix-socket-only and
> takes over the whole connection for the life of the subscription — open
> a *fresh* socket connection for it and keep your original connection (or
> another fresh one) for ordinary `open`/`flush`/`status`/... ops. Sending
> `subscribe` over HTTP `POST /rpc` is refused outright (with a message
> pointing at `GET /events`), since an HTTP request/response cycle has no
> way to switch modes mid-stream the way a raw socket connection can.

**SDK helpers** (Milestone 4 Task 4b): both SDKs ship a thin `events()`
helper that does exactly this — opens its own fresh, dedicated socket
connection (never the caller's own `Client`/`Session` connection), sends
`subscribe`, reads the ack, and yields one parsed event per line:

```python
for ev in client.events():
    print(ev.type, ev.db, ev.branch, ev.detail)
```

```ts
for await (const ev of client.events()) {
  console.log(ev.type, ev.db, ev.branch, ev.detail);
}
```

Python's `events()` is a generator (nothing is opened until the first
iteration); TS's is an `AsyncGenerator<OffshootEvent>`. Both close their
dedicated socket cleanly when the caller stops iterating early (Python:
`break`/`generator.close()`, delivered as `GeneratorExit`; TS: `break`/
`.return()`, delivered through the async-iterator return protocol) — no
file descriptor is leaked by stopping mid-stream. The terminal
`dropped_slow_consumer` event (see above) is yielded like any other event
and then the stream simply ends (no exception) — a caller that cares
whether it was dropped checks the last event's `type`.

### HTTP: `GET /events`

Same Bearer auth as everything but `/healthz`. Response is
`Content-Type: text/event-stream`; each event is `data: <event JSON>\n\n`.
A `: ping` comment line is written every 15 seconds to keep the connection
alive across any proxy/load balancer/kubelet that kills silent streams
(PM Amendment 12) — ordinary SSE clients ignore comment lines
automatically. Unlike the other `-http` routes, this handler manages its
own per-connection write deadline (re-armed before every write, see
"Stalled (still-connected) subscriber" above) rather than being bound by
Task 3's `http.Server` `WriteTimeout` (90s, sized for
`/rpc`/`/metrics`/`/debug/pprof/*`, see above) — a live subscription is
never hard-cut by that 90s bound, while a stalled one is still cleaned up
promptly rather than left open indefinitely.

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

Three tools take the opposite stance: `offshoot_rollback`,
`offshoot_promote` (checked against its `target` only), and
`offshoot_destroy` **refuse** — rather than proceeding at rest — whenever
the daemon has any session, healthy or fenced, open on the affected branch.
The CLI's own `--force` is not a uniform precedent here: `offshoot destroy
--force` does override a live lease (see that section above), but `offshoot
promote --onto` never gates on the target's lease at all, with or without
`--force` — its `--force` overrides only the protected-branch check, and a
promote unconditionally clears the target's lease as a side effect of the
repoint either way (see "Promote" above). Whatever the CLI does, the MCP
tools' `force` argument has no effect on this particular refusal: repointing
or deleting a branch's ref out from under a session the daemon still
believes it owns is refused unconditionally, and the fix is to close the
session first (`offshoot session close`) and retry.
`offshoot_promote`'s `source` is the one exception not guarded this way: an
open session there doesn't block the promote, but the promoted state is the
source's last-flushed/checkpointed head, not whatever is unflushed in that
live session. Put together: **the good path for `offshoot mcp` requires a
harness-opened session** (the SDKs, `offshoot session open`, or a custom
loop) already open before the agent calls a tool — without one, every tool
still works, just entirely at rest.

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
namespace as `offshoot checkpoint`, and stamped with the same `created_at`).
Prints the transaction id now durable. The daemon writes a full snapshot
every 16th flush and an incremental segment (only the changed pages)
otherwise, so materializing state never replays more than one snapshot plus
fifteen segments.

There is no separate daemon "checkpoint" op — a live session's named flush
*is* how its checkpoints are created; the underlying daemon protocol's
`flush` op also accepts a `meta` map (same caps as `checkpoint`'s `--meta`),
which the Python/TypeScript SDKs' `flush(name, meta=...)` expose. This CLI
subcommand does not have a `--meta` flag today — use an SDK client for
metadata on a live-session checkpoint.

**Errors:** `db@branch` is not open here; the session has lost its lease
(fenced — it will not write under a dead epoch); `meta` given with no
checkpoint `name`; `meta` over a cap (SDK-only, since this CLI subcommand
has no `--meta` flag).

## `offshoot session status [-socket PATH]`

```
offshoot session status
```

Lists every session currently open on this daemon: `db@branch`, durable
transaction id, epoch, lease holder, checkout path, and — if the session has
hit an error (e.g. fenced by a lost lease, or a contract violation) — that
error inline. This is per-session detail; for the computed branch-state
taxonomy (`active`/`pending`/`error`/`dirty`/`detached`/`idle`) across
EVERY branch of a db — including ones with no session open at all — see
[Branch states](#branch-states) above and the daemon `branches` op (SDK
`Client.branches()`; no CLI `session branches` subcommand exists yet).

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

## `offshoot session dbs [-socket PATH]`

```
offshoot session dbs
```

Lists every database this store has at least one ref for (one per line,
sorted) — the daemon protocol's `dbs` op, the same one the Python/TypeScript
SDKs' `dbs()` calls. Useful for cleanup jobs that need to enumerate what
exists without shelling out to a store-directory listing. An empty store
prints nothing and is not an error (unlike `branches`, which errors on an
unrecognized `db` name — there's no single db to name here).

## Daemon protocol ops with no CLI `session` subcommand: `export`, `checkout-at`

The daemon protocol's `export` and `checkout-at` ops (Python `Client.export`/
`checkout_at`, TypeScript `Client.export`/`checkoutAt`) are reachable only
through an SDK client today — there is no `offshoot session export`/
`offshoot session checkout-at` CLI subcommand; use the top-level `offshoot
export` / `offshoot checkout --at --read-only` commands above for CLI/at-rest
access to the same underlying `ops.Workspace.Export`/`CheckoutAt` functions.

`export`'s destination is a path on the **daemon's own host**, not the
client's — it must be given as an absolute path (a relative one is refused
outright) and is written with the same refuse/`force`/atomic-temp+rename
semantics as the CLI command above. It reads the branch's last **durable**
state from the store, never a live session's checkout: an open session's
unflushed writes are not in the export (flush first if you need them
included). `checkout-at` materializes into the same `checkouts-ro` cache
path the CLI command above uses, and is safe to call even while this same
daemon has a live session open on the target branch (unlike `checkout`/
`rollback`/`promote`, which all refuse in that case) — it never touches the
writable checkout.

**Threat model:** the daemon speaks only a local unix socket (mode `0600`);
any process able to open that socket already runs as the same user on the
same host, so `export`'s destination path is trusted as an ordinary
filesystem path that process can write — the daemon does not sandbox it or
check it against an allow-list beyond requiring it be absolute.

---

## What's not here

See [docs/status.md](status.md) for the full implemented/deferred matrix
and links to the roadmap milestones tracking each — e.g. the FD budget
with idle-checkout eviction and `SnapshotEvery` tuning via the daemon,
both still not yet implemented as of this page's last update.
