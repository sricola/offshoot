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
offshoot help [command]        # also --help, -h; `offshoot <command> --help` prints that command's entry
offshoot version               # also --version
```

`help`, `--help`, `-h` and `--version` never open the store, so they work
in a directory with no store, and `offshoot <command> --help` prints that
command's usage instead of running it with `--help` as a name.

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
| `s3://bucket/prefix` | S3-compatible bucket (AWS S3, MinIO) |

Any other URL scheme is refused with `unsupported store scheme`.

Every store-touching command except `init` first attaches to the store
(`help`/`--help`, `version`/`--version` and a bare `offshoot` usage print
return before attaching): it opens the
backend and runs a **CAS (compare-and-swap) capability probe**. This runs on
every invocation — a fresh CLI process re-pays it every time — and refuses
to proceed if the store doesn't enforce conditional writes, rather than
silently degrading to a weaker guarantee. A long-lived daemon (`offshoot
serve`) pays this cost once per process instead of once per command.

**S3 environment variables** (only consulted for `s3://` specs):

| Variable | Meaning |
|---|---|
| `OFFSHOOT_S3_ENDPOINT` | Custom endpoint (MinIO, or any S3-compatible endpoint); unset means AWS's default endpoint |
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
| `OFFSHOOT_TOKEN` | The Bearer token for `offshoot serve -http`, in place of `-token` or `-token-file`; see [`-http ADDR`](#-http-addr--opt-in-http-listener) below |

**Naming rules**, enforced on every database name, branch name, and
checkpoint name: 1–128 characters, charset `[a-z0-9-_.]`, never starting
with `-` (it would read as a flag), and never exactly `.` or `..` or
containing `..` as a substring (those are directory-traversal segments once
joined into a storage key). A bad name fails fast with `store: invalid name
...`.

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
`--force`), and prints one confirmation line (`created app (branch
main)`). With `--from file`, **imports** an existing SQLite file instead:
the source is read under one read transaction with `VACUUM INTO` (so the
copy is consistent even when another process is writing the source), the
copy is quiesced with a full WAL checkpoint, and *that* becomes the root
snapshot — the source file itself is never modified or truncated. There is
no mode that overwrites an existing user file.

**Errors:** refuses if `db` already exists (ref CAS conflict); `--from` with
a source file that doesn't exist or isn't a valid SQLite file.

**Daemon/SDK parity.** The daemon's `create` op takes an optional `path`
request field: a server-side, absolute path (on the daemon's own host) to
an existing SQLite file to import via `ops.CreateFrom` instead of minting
an empty database — a relative path is refused with an error containing
"absolute". Like `export`, it is refused over the HTTP surface specifically
when `path` is set (`daemon: create with path is not available over HTTP;
use the local socket`); plain `create` (no `path`) keeps working there.
Python `create(db, from_path=None)` and TypeScript `create(db, {
fromPath })` resolve the path to absolute client-side before sending it as
`path`. MCP has no tool argument for this — by design, not an oversight: an
MCP tool must never import an arbitrary host file named by an agent.

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

**A checkout is a clone, not a copy, where the filesystem allows it.**
Since v0.2.12 every materialization goes through a content-addressed
cache: `<store-root>/checkouts-ro/<db>/~by-chain/<chainID>.db`, where the
chain ID is the SHA-256 of the resolved chain's object keys, so two
branches whose heads resolve to the same objects (a fresh shared fork and
its parent, say) name the same entry. An entry is built once — from the
store, or from a cached prefix of the chain plus the remaining segments —
made `0444`, and never written again; the writable checkout is a
filesystem clone of it (APFS `clonefile`, Linux `FICLONE`), so a second
checkout of the same state costs a clone, not a decode. Where the
filesystem cannot clone (ext4, tmpfs, most network filesystems), there is
no entry and the checkout is materialized straight from the store, as
before. On a cloning filesystem every checkout also gets a `.shadow`
file beside it (see `offshoot checkpoint` below).

**The `.sum` sidecar.** Next to every writable checkout sits
`<branch>.db.sum`, a small JSON record of what the checkout was
materialized from: `hash` (SHA-256 of the file), `lineage`/`epoch`/`txid`,
`post_apply_checksum`, and — since v0.2.12 — `chain_id` (the by-chain
entry it was cloned from, when there was one), `size`, `mtime_ns` and
`change_counter` (SQLite's header counter) as a fingerprint, `stamped_ns`
(when the record was written), and `shadow` (whether a checkpoint shadow
is current). A repeat `checkout` of an unchanged branch proves the file
clean from the fingerprint alone, without hashing it, when three things
hold: the recorded identity matches the ref, size, mtime and change
counter all match, and the file's mtime is more than **1 s** older than
the stamp (git's racily-clean rule: a write landing in the same mtime tick
as the stamp could otherwise go unseen). In WAL mode SQLite does not bump
the change counter on commit, so there only size and mtime count as
evidence. Any mismatch falls back to hashing the whole file, exactly as
before, and re-stamps the record so the next call is fast. A sidecar
written by an older binary lacks the fingerprint and always takes the hash
path once.

If a checkout already exists at that path, `checkout` requires it to be
quiescent first (no live writer holding it open) — re-materializing renames
a fresh file into place, which would delete a live writer's WAL out from
under it. If the existing checkout has un-checkpointed local edits, the head
state still wins, but a warning is printed to stderr first since those edits
are about to be overwritten.

**Errors:** no such `db@branch`; checkout is busy (a live connection is
holding it) — close connections and retry; another operation replaced or
removed the checkout while it was being opened (retry).

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
anything under it just rebuilds what it needs. That includes the
`~by-chain/` directory the same tree now holds (see `offshoot checkout`
above): a `--at` miss goes through it too, so it is a clone when some
branch already materialized that state, and on a filesystem that can clone
it leaves **two** entries behind — the `~by-chain` entry and the
`<branch>@<checkpoint>.db` file cloned from it. Removing either, or the
whole tree, costs only a rebuild on the next call. A repeat call for the same
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

## `offshoot diff <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary] [--table T]`

```
offshoot diff app@attempt-1@v1 app@attempt-2@v1
offshoot diff app@attempt-1@v1 app@attempt-2@v1 --summary
offshoot diff app@attempt-1@v1 app@attempt-2@v1 --summary --table orders
offshoot diff app@attempt-1 app@attempt-2                # both at head
offshoot diff evals@golden@v1 candidate@main@final       # cross-db is legit
```

Materializes both sides READ-ONLY through the same primitives `export`/
`checkout --at --read-only` use (never a live checkout, never a lease — safe
alongside an open daemon session on either branch) and either streams
`sqldiff`'s output over them (default) or prints a stdlib-only, content-aware
per-table summary (`--summary`, no `sqldiff` dependency at all): rows
added/removed/changed by the table's declared primary key (falling back to
its internal rowid when there's no usable declared key — meaningful when
both sides descend from one seed and rowids were not renumbered, e.g. by a
`VACUUM` on a table without an `INTEGER PRIMARY KEY`, or a cross-database
diff; otherwise it over-reports changes, the safe direction for a promote
decision), with schema changes flagged (the STATUS cell gets a trailing
" (schema)"); a table whose column list differs between the two sides is
reported, with row counts, but not compared. `--table` restricts either
mode to one table. Each target uses the
same triple-`@` form `export` does — `db` alone means `db@main` head,
`db@branch` means that branch's head, `db@branch@checkpoint` means that named
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

## `offshoot checkpoint <db>[@branch] <name> [--snapshot] [--meta k=v ...] [--force]`

```
offshoot checkpoint app v1
offshoot checkpoint app v1 --snapshot
offshoot checkpoint app v1 --meta eval_run=42 --meta git_sha=abc123
offshoot checkpoint app v1 --force
```

Snapshots the *current checkout's* state (not just the ref) as a named
checkpoint, quiescing the checkout first (busy timeout ~3s, then a clean
failure rather than a hang). Checkpoint names are unique per branch — this
is the only operation actually named "checkpoint"; continuous background
capture by the daemon is called "flush"/"commit," not "checkpoint."

**Segment or snapshot.** At rest (no daemon) there is no capture engine
recording which pages changed, so offshoot keeps its own record: after
every checkout and checkpoint it clones the checkout to
`<branch>.db.shadow` (a reflink, so it costs no data blocks until the
checkout diverges from it). The next checkpoint diffs the checkout against
that shadow page by page and writes an LTX **segment** of just the changed
pages when all of these hold: the shadow exists and its checksum matches
the recorded head, the sidecar's identity matches the ref, the branch's
resolved chain is shorter than the snapshot cadence (`SnapshotEvery`,
default 16 — the same floor that keeps a daemon's reads bounded), fewer
than half the pages changed (a database under 64 pages always counts as
under that bar), and `--snapshot` was not given. Otherwise it writes a
full **snapshot**, as every at-rest checkpoint did before v0.2.12. The
shadow is taken at `checkout` too, so even a branch's first checkpoint
can be a segment; a filesystem that cannot clone never has a shadow, so
every checkpoint there is a snapshot. The diff still reads the
whole checkout and the whole shadow — the saving is in what is uploaded
and stored, not in local I/O. The daemon's `session flush` writes segments
from its own capture engine instead (see [What a flush
costs](operations.md#what-a-flush-costs) in the README).

The output names what was written:

```
$ offshoot checkpoint demo seeded
checkpoint "seeded" at txid 2 (segment, 2 pages, 0.4 KiB)
$ offshoot checkpoint demo v1 --snapshot
checkpoint "v1" at txid 3 (snapshot, 0.0 MiB)
```

(From the [quickstart](quickstart.md)'s tiny database, which is why the
snapshot rounds to 0.0 MiB.)

Each checkpoint entry in the ref records the same thing as an additive
`kind` field (`"snapshot"` or `"segment"`; empty on entries written before
v0.2.12, on `fork`/`promote` entries, and by writers that don't know).
offshoot itself no longer reads it back (concurrent checkpoints used it
before the checkpoint took the branch lease); it stays for tools that
inspect refs.

Every checkpoint records a creation timestamp (`created_at`, RFC3339 UTC)
automatically. `--meta k=v` is repeatable and attaches a small string→string
map to *this specific checkpoint* (e.g. an eval run id, a git SHA, an agent
id) — capped at 32 keys, 64-byte keys, 512-byte values, enforced before
anything is written; a rejected `--meta` leaves the branch untouched. This is
branch/checkpoint-level metadata, not row-level provenance.

Children never inherit a parent's checkpoints — a fork's own history
begins at its fork point (even a shared, copy-on-write fork's ref carries
only its auto-created `fork` checkpoint), so a checkpoint made before the
fork isn't addressable from the child; resolve it on the parent instead.

**The checkpoint holds the branch lease.** From before it quiesces the
checkout until the ref write that advances the head, an at-rest checkpoint
holds the branch lease as `checkpoint:<host>/<pid>/<nonce>` (the nonce is
per call), renewing it every 10 s; the head write releases it. The
acquire bumps the branch's epoch, so the object goes under a key no other
writer can name. While it runs, the branch reads `active` in `offshoot
status` and in the daemon's `branches` op, whose `lease_holder` (and both
SDKs' `Branch.lease_holder`) names the checkpoint, as does `offshoot lease
list`. If the checkpoint fails after the acquire it releases the lease; if
its process dies, the lease expires after 30 s and the next writer
reclaims it. Each checkpoint reads the ref twice and writes it twice
(plus one read and one write per renewal), where it used to read and
write it once each: the acquire, whose read the refusals below run on,
and the head write. The extra write is the acquire, and it is durable,
so it costs time. On a local store it is one more fsync, nearly all of
the difference (a ref read takes about 0.1 ms there); on a local macOS
store BranchBench measured checkpoint p50 about 3 ms higher one at a
time and about 11 ms higher eight at a time (more on a heavily loaded
machine, where the fsyncs queue). On S3 every request is a round trip: a
snapshot checkpoint makes one more `GET` and one more conditional `PUT`
of the ref, in sequence, and a segment checkpoint makes as many requests
as before, the extra `GET` and `PUT` in place of the two `LIST`s it no
longer needs. On a local store each checkpoint also leaves one more
`data/<lineage>/<epoch>/` directory, and listing a lineage reads them
all, so resolving the branch's chain (a segment checkpoint's own
resolve, `checkout`, `fork`) slows as checkpoints accumulate on one
lineage: about 1.6 ms after 100 checkpoints and 16 ms after 1,000,
against well under a millisecond for one; `compact` starts a fresh
lineage and resets it. See [benchmarks](benchmarks.md). A head write
that loses its compare-and-swap to a `touch`, `protect` or TTL change is
retried, up to three attempts.

**Refusals, and `--force`.** A checkpoint is refused when the branch has
a **live lease** — an open daemon session, `offshoot lease acquire`, or
another checkpoint in progress — with or without `--force`: the
checkpoint takes the lease itself, so it waits its turn rather than write
under another writer's epoch. Under another checkpoint's lease, retry
when it finishes, in a few seconds: its lease lapses on its own 30 s
after its process dies. Under a session's, close the session and retry;
a session that is already closing holds its lease until its close
releases it, and `session close` then waits for that close. If the holder
is gone (a daemon that was killed with a session open, a `lease acquire`
nobody will release), `offshoot lease release <db>@<branch>` frees the
lease at once, as the refusal says. A session whose close failed to
release its lease is not such a holder: that lease lapses at its expiry,
and once the branch is reopened it is the new session's (see [`session
close`](#offshoot-session-close-dbbranch--socket-path)). An expired lease
is reclaimed without any flag. A
checkpoint is also refused when the checkout is **detached** — its
sidecar records a lineage the branch no longer points at, because the
branch was repointed (`rollback`, `promote`, `compact`) after the
checkout was materialized — since checkpointing it would silently revert
that repoint; `offshoot checkout` refreshes it (discarding its local
edits), `offshoot export` keeps them as a file, or `--force` checkpoints
it anyway. A branch that is mid-`destroy` or mid-reap is refused
outright; no flag overrides that. `rollback`, `promote --onto` (the target
only) and `compact` apply the same live-lease, destroy and reap rules,
except that `--force` does override a live lease for them: the repoint
clears it, fencing a session (its unflushed writes are lost) or making a
checkpoint in progress fail without committing. Their refusal of a
checkpoint's lease says to retry when it finishes. A forced repoint
reads the ref, copies objects and then compare-and-swaps the ref; when
that takes longer than the renewal interval (10 s), say for a large
database on S3, every attempt loses to the next renewal and returns
`lost a race (retry)` until the checkpoint ends, as it does against a
daemon session's renewals.

**Errors:** checkpoint name already exists on this branch; no checkout
exists yet (run `checkout` first); checkout is busy; another operation
replaced or removed the checkout while it was being opened (retry); live
lease (a session, `lease acquire`, or another checkpoint; `--force` does
not override it); detached checkout without `--force`; branch mid-destroy or
mid-reap; `--meta` over a cap (key count, key length, or value length).
After the lease is taken: the lease ended while the checkpoint ran (a
forced repoint, `lease release`, a reclaim after it expired, or
`destroy --force`, whose claim the head write also refuses to write
over), in which case nothing was committed and the checkpoint deletes its
own object; the head write lost three compare-and-swaps to concurrent
ref writes (retry; the object is left for GC, since on S3 a conflict
answer does not rule out an earlier attempt of the write still landing);
the object's create-only put reported its key taken but nothing could be
read back there (retry); an object with other bytes already sits at the
checkpoint's own key, which nothing else writes (store corruption; the
object is left in place); the branch moved under the checkpoint's own
lease, which only an older offshoot binary's `checkpoint --force` can do
(the object is kept, since the head may name it; upgrade every binary).
One outcome is not a failure but an unknown: the checkpoint **may have
committed** when a head write failed without a verdict from the store (a
timeout, a 5xx the SDK gave up on) and the ref cannot show whether it
landed, because the store may still apply it, the branch has moved
since, the ref cannot be read, or a destroy or reap has claimed the
branch since (an abandoned claim puts the ref back exactly as the write
found it). The checkpoint then
keeps its object, so a head or a fork that names it still materializes,
and a retry under the same name is refused as already existing if it did
commit.

## `offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration] [--meta k=v ...]`

```
offshoot fork app attempt-1
offshoot fork app attempt-1 --at v1
offshoot fork app attempt-1 --ttl 2h
offshoot fork app attempt-1 --meta eval_run=42 --meta git_sha=abc123
```

Creates `new-branch` as an independent branch from `db@branch`'s head, or
from a named checkpoint via `--at`. `branch` (the source) defaults to
`main`.

**Fork is copy-on-write.** A fork **shares** the parent's already-durable
store objects through a base pointer — it records where in the parent's
chain it forked from and writes new objects only as it diverges, so N
forks of a G-byte database no longer cost N×G in the store; they cost
near-zero until each child actually writes. Reads on the child resolve
through the parent's objects below the fork point and the child's own
objects above it. The exception is the **fork-time snapshot floor**: when
the fork point's fully-resolved chain is already at the depth bound, fork
falls back to materializing one fresh snapshot in the child's own lineage
(the pre-copy-on-write behavior), which keeps read
materialization bounded no matter how deep a fork-of-fork spine grows. A
shared child also self-snapshots on the ordinary snapshot cadence once its
own divergence crosses it, after which its reads never touch the parent.
`offshoot status` reports which class each branch is in
(`storage=shared` vs `storage=materialized` — see `offshoot status`
below), and `offshoot compact` converts a shared branch into a
self-contained one on demand (see `offshoot compact` below).

**The fork-time floor is a per-process setting, not a persisted one.** The
depth bound above defaults to 16 (`ops.ForkShareMaxDepth`) but is
configurable — the daemon sets it from its own `-snapshot-every N` (see
`offshoot serve` below) so the fork floor agrees with the cadence at which
a daemon-managed session self-snapshots. That configured value lives only
in the serving process's memory; nothing about it is written to the store.
An at-rest CLI `fork` (no daemon involved) therefore always uses the
default of 16, even against a store a daemon elsewhere serves with a
different `-snapshot-every N`. This is harmless — materialization stays
bounded either way — just potentially DIFFERENT from what a daemon's
configured cadence would have used: looser (shares for a few more levels
before falling back to materializing) if the daemon runs a lower N than 16,
tighter (falls back to materializing sooner) if the daemon runs a higher N.
`N` can be any value `>= 1` (`-snapshot-every`'s only floor), so either
direction is possible depending on how a given daemon is tuned. There's no
mechanism today for the CLI to learn a daemon's flag, since it isn't
persisted anywhere the CLI could read it.

Destroying a parent remains **instant and always allowed** — but under
sharing, the parent's *bytes* can outlive its ref: they are reclaimed by
GC only once no surviving child's chain still reads through them (see
`offshoot destroy` below for the full semantics).

**The first shared fork upgrades the store to layout version 2.** This is
one-way and intentional: pre-copy-on-write binaries refuse the entire
store from that point on (their GC reasons about whole lineages and would
sweep shared objects out from under live children — silent data loss —
so they are locked out up front by the manifest check rather than allowed
to corrupt). Don't point an old binary at a store any new binary has
forked in.

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

## `offshoot protect <db>[@branch]` / `offshoot unprotect <db>[@branch]`

```
offshoot protect app@main
offshoot unprotect app@attempt-1
```

Sets or clears a branch's `protected` flag. A protected branch refuses
unforced `destroy` and unforced `promote --onto` it, and is never TTL-reaped
(`main` is protected by default; every other branch starts unprotected).
Protecting a branch does **not** touch its activity clock — `TouchedAt` and
the TTL deadline it anchors are left exactly as they were, so unprotecting a
stale, TTL'd branch doesn't incidentally grant it a fresh TTL window.

Through an `offshoot mcp` server, an agent-supplied `force:true` against a
protected branch is honored only when that server was started with
`-allow-force`; see [`offshoot mcp`](#offshoot-mcp) below. `protect` and
`unprotect` are CLI-only by design — there is no `offshoot_protect` MCP
tool, so an agent can observe a branch's `protected` flag (via
`offshoot_list`) but never flip it.

**Errors:** the branch is currently being reaped or destroyed ("too late to
change its protection"); CAS races are retried internally.

## `offshoot rollback <db>[@branch] --to <checkpoint> [--no-backup] [--backup-ttl DUR] [--materialize] [--force]`

```
offshoot rollback app@attempt-1 --to fork
offshoot rollback app@attempt-1 --to fork --materialize
offshoot rollback app@attempt-1 --to fork --force
```

Repoints the branch at a **new** lineage seeded from `checkpoint`'s state
(internally, the same machinery as fork). Checkpoints at or before the
target are kept; checkpoints after it are dropped. A branch with a live
lease (an open daemon session, `lease acquire`, or an at-rest checkpoint
in progress) is refused unless `--force`, which clears the lease by the
repoint: a session is fenced and its unflushed writes are lost, and a
checkpoint in progress fails without committing (see [checkpoint](#offshoot-checkpoint-dbbranch-name---snapshot---meta-kv----force)'s
refusal rules; a branch mid-destroy or mid-reap is never forceable).
Afterwards the branch is immediately acquirable.

**Shared by default (since v0.2.12).** The new lineage is a base pointer
at the kept checkpoint in the old lineage — two small objects, no data
copy — exactly as a fork at that checkpoint would be, and the kept
checkpoints are carried over unchanged. The old lineage's objects *above*
the checkpoint (the abandoned future) are reclaimed by GC once nothing
else reads them (the safety fork below holds them until it is reaped or
destroyed); the objects at and below it stay live for as long as the
rolled-back branch reads through them. Repeated rollbacks to the same
checkpoint do not stack base pointers: each one points at the lineage that
actually holds the checkpoint. Two cases still copy: `--materialize`,
which seeds a self-contained lineage with a snapshot of the checkpoint and
copies each kept checkpoint's snapshot into it (the pre-v0.2.12
behaviour, for when you want the branch to stop pinning the old lineage
right away), and a checkpoint whose resolved chain is already at the
fork-time depth floor, which copies for the same reason a fork there
would. `offshoot compact` converts a shared result later.

Output: the (re-materialized) checkout path on the first line, then
`rolled back <db>@<branch> to "<checkpoint>" (shared)` — or
`(materialized)` — on the second, then the safety-fork line below. **The
second line is new in v0.2.12**: a script that read the whole of
`offshoot rollback`'s output as the path must now take the first line
(`offshoot rollback ... | head -1`).

**The safety fork.** Before the repoint lands, the branch's current head
(the state rollback is about to abandon) is kept first as a shared fork
named **`<branch>-pre-rollback`** — two metadata objects, no data copy —
with a TTL (default 24h; `--backup-ttl 2h` to change it; a safety fork
always carries one) and the marker metadata `offshoot.pre-rollback=<branch>`.
It is one rolling undo point per branch: the next rollback of the same
branch replaces it, and it is only ever replaced when the branch at that
name carries the marker — a branch of your own that merely shares the name
makes rollback refuse (destroy or rename it, or pass `--no-backup`). Undo a
rollback by promoting the safety fork back onto the branch:

```
offshoot promote app@attempt-1-pre-rollback --onto attempt-1 --force
```

(`--force` is needed only if `attempt-1` is itself protected.) `--no-backup`
skips minting the safety fork entirely.

The ref repoint (a CAS write) is the point of no return; the local checkout
refresh that follows is best-effort — if it fails (e.g. the checkout is
busy, or another operation replaced or removed it during the refresh), the
command reports a partial success: the branch *did* roll back, but the
checkout needs a manual `offshoot checkout <db>@<branch>` to catch up, which
the error names (a busy checkout's error says to close its connections
first). Do not retry the rollback: a second one would replace the safety
fork with the already-rolled-back head.

**Errors:** unknown checkpoint name; the branch has a live lease without
`--force`, or is mid-destroy or mid-reap (nothing is touched);
`<branch>-pre-rollback` exists but is not rollback's own safety fork
(nothing is touched); the previous safety fork has a live lease (nothing
is touched — close that session first, or `--no-backup`); lost a
concurrent CAS race (retry).

**Daemon/SDK parity.** The daemon's `rollback` op takes the same knobs as
request fields — `no_backup` (bool), `backup_ttl` (a Go duration
string; a non-positive value is refused) and `materialize` (bool) — and
echoes the safety fork's name back as `backup` (empty when none was
minted) and whether the result shares as `shared` in the response. Python
`rollback(db, branch, to, *, backup=True, backup_ttl=None,
materialize=False)` and TypeScript `rollback(db, branch, to, opts:
RollbackOptions)` (`materialize?: boolean`) expose the same options.
`offshoot_rollback` (MCP) takes no `materialize` argument: it shares unless
the depth floor copies.

## `offshoot promote <db>@<source> --onto <target> [--force] [--no-backup] [--backup-ttl DUR] [--materialize]`

```
offshoot promote app@attempt-1 --onto main --force
```

Repoints `target` at a **new** lineage seeded from `source`'s current head
(fork machinery again — this is why promoting never risks an epoch
collision). `source` survives unchanged (it's typically left to TTL-reap
later, or destroyed explicitly). `target`'s old lineage is orphaned and
later garbage-collected; its checkpoint map resets to just `{"promote":
<txid>}`. If `target` is protected (`main` is protected by default),
`--force` is required.

**Shared by default (since v0.2.12).** Like rollback, the new lineage is
a base pointer at `source`'s head rather than a copy of it, so promoting
the winner of a fork-per-attempt run costs two small objects however big
the database is. The cost moves to reclaim: `target` now reads through
`source`'s lineage, so destroying or reaping `source` frees nothing that
`target` still reads until `target` diverges past its own snapshot
cadence or is compacted (`offshoot compact`). `--materialize` copies
instead (the pre-v0.2.12 behaviour); the fork-time depth floor copies
automatically. The output line ends in `(shared)` or `(materialized)`:
`promoted app@attempt-1 -> app@main at txid 3 (shared)`.

**The safety fork.** Because that reset makes promote the one verb whose
inverse you'd otherwise have to build by hand ("fork `main` first, then
promote"), promote builds it: before the repoint lands, `target`'s current
head is kept as a shared fork named **`<target>-pre-promote`** — two
metadata objects, no data copy — with a TTL (default 24h; `--backup-ttl
2h` to change it; a safety fork always carries one) and the marker
metadata `offshoot.pre-promote=<target>`. It is one rolling undo point per
target: the next promote onto the same target replaces it, and it is only
ever replaced when the branch at that name carries the marker — a branch
of your own that merely shares the name makes promote refuse (destroy or
rename it, or pass `--no-backup`). Undo a promote by promoting the safety
fork back onto the target:

```
offshoot promote app@main-pre-promote --onto main --force
```

That undo skips minting a new safety fork (it would have to replace the
branch being promoted) and leaves `main-pre-promote` in place. The cost to
know: the safety fork's base pointer keeps `target`'s old lineage alive
until the fork is reaped or destroyed, so the old lineage's storage is
reclaimed after the TTL, not at promote time — exactly the "base-pointing
into a lineage meant to die pins it" trade-off [concepts](concepts.md)
describes, here bounded by the TTL. `--no-backup` skips all of this. A
live lease on `target` (an open daemon session, `lease acquire`, or an
at-rest checkpoint in progress) is refused unless `--force`, which clears
it by the repoint: a session is fenced and its unflushed writes are lost,
and a checkpoint in progress fails without committing; a `target` mid-destroy or
mid-reap is refused outright. A lease on `source` never blocks a promote.
Afterwards `target` is immediately acquirable. `target`'s
checkout, if any, is refreshed after a busy probe — same best-effort
semantics as rollback.

**Errors:** `source == target`; `target` is protected without `--force`;
`target` has a live lease without `--force`, or is mid-destroy or
mid-reap; `<target>-pre-promote` exists but is not promote's own safety fork (nothing
is touched); the previous safety fork has a live lease (nothing is
touched — close that session first, or `--no-backup`); target checkout is
busy, or another operation replaced or removed it, during the refresh after
the repoint (the promote stands; the refresh is skipped and reported: close
the checkout's connections if it was busy, then refresh it with
`offshoot checkout <db>@<target>`, not by retrying the promote, which would
replace the safety fork with the promoted head); lost a concurrent CAS race
(retry).

**Daemon/SDK parity.** The daemon's `promote` op takes the same knobs as
request fields — `no_backup` (bool), `backup_ttl` (a Go duration
string; a non-positive value is refused) and `materialize` (bool) — and
echoes the safety fork's name back as `backup` (empty when none was
minted) and whether the result shares as `shared` in the response. Python
`promote(db, source, onto, force=False, backup=True, backup_ttl=None,
materialize=False)` and TypeScript `promote(db, source, onto, opts:
PromoteOptions)` (`materialize?: boolean`) expose the same options.
`offshoot_promote` (MCP) takes no `materialize` argument: it shares unless
the depth floor copies.

## `offshoot compact <db>[@branch] [--force]`

```
offshoot compact app@attempt-1
offshoot compact app@attempt-1 --force
```

Turns a **shared** (copy-on-write) fork into a self-contained branch: its
full state at head is re-encoded as one snapshot in a fresh lineage and
the branch's base pointer is dropped, so it stops reading through — and
stops pinning — its ancestors' storage. This is the manual "pay the full
copy now to release a dead ancestor" lever: after compacting the last
sharing child, a destroyed ancestor's lingering bytes become reclaimable
by the next `offshoot gc` pass (compact itself deletes nothing; GC owns
reclaim). A branch that is already self-contained is a **no-op** (prints
the current head txid), so scripted "compact everything" loops never fail
on branches with nothing to do.

**Checkpoints are preserved**, as `rollback --materialize` preserves them
(unlike `promote`, which still resets its target to a single `promote`
checkpoint): every existing
checkpoint's snapshot is copied into the new self-contained lineage and
rewritten to epoch 1 — a location update, `CreatedAt`/`Meta` unchanged, not
a new checkpoint — and a `compact` checkpoint at the (unchanged) head txid
is *added* alongside them, since old checkpoints were anchored on the
shared ancestor's storage and need their own copy to resolve once that
ancestor's lineage is later reclaimed by GC. Checkpoints sharing a txid
share one copy. Nothing is dropped or renamed; you don't need to `export`
anything first to preserve history across a compact.

Cost class: compact is a full materialize — one full copy of the branch's
head state (~G bytes for a G-byte database) plus one additional snapshot
copy per *distinct* checkpoint txid kept — the same cost class a
`promote --materialize` or `rollback --materialize` pays, not a cheap
metadata flip (compact is the one repointing verb that still always
copies: copying is its purpose). (The N×G figure
elsewhere in this page is the *aggregate* cost of N materialized forks;
one compact pays ~G plus its checkpoint copies, once.) Through the daemon (the
`compact` op, SDK `compact()`), compact refuses while this daemon has an
open session on the branch — close it first, exactly like `rollback` and
`promote`: the session owns the checkout compact would repoint out from
under it. With no session open, the durable head is already current (the
last flush persisted), so compact materializes exactly that head. A
concurrent flush from elsewhere that advances the head between the copy
and the ref swap loses the CAS and returns a retry error.

**Errors:** no such `db@branch`; branch has a live lease (an open session,
`lease acquire`, or an at-rest checkpoint in progress) without `--force`,
which fences that session and loses its unflushed writes, or makes the
checkpoint fail without committing; branch mid-destroy or mid-reap; lost
a concurrent CAS race to a flush (retry). After the repoint, a checkout
that is busy, or that another operation replaced or removed during the
refresh, is not refreshed; the compact stands and reports it (close the
checkout's connections if it was busy, then run `offshoot checkout
<db>@<branch>`).

## `offshoot destroy <db>[@branch] [--force]`

```
offshoot destroy app@attempt-1
offshoot destroy app@attempt-1 --force
```

Deletes the branch's ref and its local checkout files (`.db`, `-wal`,
`-shm`, `.sum`, `.shadow`), and prints one confirmation line (`destroyed
app@attempt-1`). Destroying a parent is always safe, instant, and allowed
regardless of live children — a parent's destruction can never corrupt a
child, whether that child is a shared (copy-on-write) fork or a
materialized one. `--force` is required to destroy a protected branch
(`main` by default), and also to destroy a branch under an active lease (a
live holder may still be mid-write; without `--force` this is refused
outright); under an at-rest checkpoint's lease the refusal says to retry
when it finishes, in a few seconds. A forced destroy under an at-rest
checkpoint in progress makes that checkpoint fail without committing,
whether a renewal finds the branch gone or the head write finds the
destroy's claim; it deletes its own object and leaves the claim alone
(unless a head write it had already sent may still land, in which case
it keeps the object and reports that it may have committed). A lease
renewal, a checkpoint's or a session's, that lands between the destroy's
claim and its delete writes over the claim and leaves it set. The
destroy still deletes the branch (on a local store its conditional
delete re-reads the ref and deletes again while only the lease expiry
has moved, up to 4 attempts; on S3 the delete is unconditional), and the
holder's next renewal finds the branch gone. Without `--force`, a destroy
that found the lease lapsed is refused as a live lease on a local store
if the holder renewed it in that window. Under a claim that a killed
destroy left, renewals go on as they do without one, so the holder's
lease stays live until the janitor clears the claim.

**Under copy-on-write, "destroyed" and "reclaimed" are different events.**
Destroying a branch removes its ref immediately, but if any surviving
child still shares its storage (forked from it and hasn't diverged past
it), the destroyed parent's *bytes* linger in the store: GC's reachability
mark follows every live branch's chain through its base pointers, so a
shared ancestor's objects stay live for exactly as long as some
descendant's reads still resolve through them. The lingering bytes are
reclaimed once the last sharing child is itself destroyed — or compacted
(`offshoot compact`, the manual release valve). Objects of the destroyed
parent that no child ever needed (e.g. everything above the last fork
point) are reclaimed on the normal GC schedule right away. In short:
**destroy is instant; the storage refund waits for the last sharing
child.**

**Errors:** protected without `--force`; live lease without `--force`;
checkout is busy (close connections first); another operation replaced or
removed the checkout while destroy was opening it (retry); `is already
being destroyed` while another destroy of the branch is between its claim
and its delete, with or without `--force` (retry; see below); the destroy
lost a race to a concurrent `AcquireLease` on the same branch, or to
another write of its ref between its claim and its delete, such as a
flush, a `touch` or a lease renewal before every one of its 4 delete
attempts (retry — see
below).

### Claim-guarded delete

Between checking a branch's lease and actually deleting its ref sits a
window: a lease could be acquired in that gap and have its brand-new
holder's branch deleted out from under it moments later. Destroy closes
this by CAS-writing a `Deleting` claim on the ref *before* it does anything
irreversible — the same shape as `offshoot gc`'s reap claim (`Reaping`),
generalized to every `destroy` call, not just TTL reaping. A concurrent
`AcquireLease` (CLI `lease acquire`, the daemon `open` op, or the
embeddable library directly) checks for this claim and refuses outright
(retryable — the claim is always transient) rather than racing it. This
closes the exact TOCTOU an earlier design review documented for the M2
Destroy path.

`--force` still claim-guards: it bypasses the protected/live-lease checks
above — an operator's explicit override of *those* — never the claim
itself. A lease acquired a moment before a forced destroy lands still wins
the underlying compare-and-swap on the ref, and the forced destroy reports
a retryable race loss exactly like an unforced one would.

A claim stands for 30 seconds from its timestamp. While it does, a second
destroy of the same branch, forced or not, is refused with `is already
being destroyed` (`store.ErrDeleting`, retryable) and writes nothing, and
the reaper leaves such a branch to that destroy without writing its ref,
even when the reaper had claimed the branch first. A destroy that fails
after its claim removes its own claim and no other. Were a claim cleared
under its destroy, an acquire could take the branch at a new epoch, and on
S3, where the delete is unconditional, that destroy would then delete the
branch under the fresh lease. So a destroy sends its delete only while
its claim has more than 10 seconds left to stand, judged after the
checkout quiesce; one held up past that (its claim write slowed by
request timeouts and SDK retries, or a slow quiesce) removes its claim
and fails with `was held up too long to delete under its claim`
(retryable; `errors.Is(err, store.ErrCAS)`), deleting nothing. A destroy
whose delete request is itself held up past the 30 seconds can still
delete under a fresh lease on S3
([limitations](limitations.md#smaller-edges-worth-knowing)). A claim
stamped more than a minute ahead of a host's clock (its writer's clock runs
ahead) counts as abandoned on that host, as one 30 seconds old does. On
S3 a claim write can land and still report failure (the SDK's retry
answered 412 or 409 by its own first attempt, or a timeout that lost the
response). That claim is still the destroy's own, which it recognises by
the claim's timestamp, and it goes on under it rather than reporting a
lost race whose retry its own claim would refuse: if the claim landed in
time it deletes the branch, and if it is by then within 10 seconds of
standing 30 the destroy fails with `was held up too long to delete under
its claim` as above, sending no delete and removing its claim. When the
ref shows nothing landed yet (a 409 or a timeout with the write possibly
still in flight), the destroy re-reads it for about 2 seconds before
giving up, and its error then says the claim may still land, in which
case a retry is refused for up to 30 seconds. A local store settles
every write before it returns, so there a claim write that failed wrote
nothing, and the destroy reports the failure at once.

**Backend-specific mechanics** (deliberately: do not pretend S3
`DeleteObject` has preconditions it doesn't):

- **Local** gets a TRUE conditional delete on top of the claim: the same
  per-key lock file `PutIf` already uses to implement compare-and-swap also
  backs a compare-and-delete (`Local.DeleteIf`) — belt-and-suspenders, since
  the claim above already serializes concurrent destroys/acquires on its
  own. The lease holder's renewals go on under the claim, so a delete that
  loses its compare-and-swap to one (or to a reaper clearing its own
  reaping claim) re-reads the ref and deletes again (above); any other
  write in between fails the destroy as a retryable race loss.
- **S3** has no compare-and-delete precondition in its API at all
  (`DeleteObject` ignores `If-Match`/`If-None-Match`; those headers only
  apply to `GetObject`/`PutObject`). Its delete stays unconditional; the
  CAS-written `Deleting` claim marker is the entire safety mechanism on this
  backend, not a supplement to a conditional delete that doesn't exist. A
  lease renewal between the claim and the delete is not seen, so the
  branch is deleted even when an unforced destroy's holder renewed a
  lapsed lease in that window.

A Destroy call that crashes after landing its claim but before finishing
the delete leaves the claim stranded — the branch is untouched (no partial
delete), but a naive read of the claim alone would block it forever. The
daemon's janitor self-heals this on the same cadence as reap/GC: a
`Deleting` claim older than 30 seconds (destroy is a handful of local
filesystem operations plus at most one checkout quiesce — anything stuck
longer means the process that claimed it is gone) is cleared, and the
branch becomes destroyable/leasable again, as is a claim stamped more than
a minute in the future. `offshoot gc` triggers the same
self-heal on demand, same as it does for a stranded reap claim. A
`destroy` takes over such a claim itself, without waiting for the
janitor, as it does a claim whose timestamp cannot be read (which the
janitor never clears).

## `offshoot gc [--grace duration]`

```
offshoot gc
offshoot gc --grace 30m
```

Two steps in one command. First, **reap**: destroys every branch whose TTL
has expired (same logic the daemon's janitor runs on a timer — see `serve
-reap-every`); a failure reaping one branch (e.g. its checkout is busy) is
reported to stderr but doesn't stop the rest of GC. Second, **collect**:
two-phase garbage collection over storage *objects* no live branch can reach
— reachability follows every branch's resolved chain at its head and at
every checkpoint, transitively through copy-on-write base pointers, so a
shared ancestor's objects stay live as long as any descendant's chain still
reads through them, while a destroyed parent's above-fork objects are
reclaimed. Unreachable objects are first tombstoned (marked, timestamped),
then actually deleted once a tombstone is older than `--grace` (default
`1h`) *and* still unreachable at sweep time (an object re-referenced during
the grace window, e.g. by a fork racing GC, is left alone). `--grace 0`
makes an object eligible for deletion on the very next `gc` run after being
tombstoned, rather than disabling collection.

Prints which branches were reaped, and how many objects were tombstoned vs.
actually deleted.

## `offshoot status [-ro-cache-budget BYTES]`

```
offshoot status
offshoot status -ro-cache-budget 500MB
```

Prints every branch across every database: its computed **state** (see
below), its **storage class** (see below), head transaction id, named
checkpoints, `protected` and `checked-out` flags (when applicable), and —
for a branch with a TTL — the TTL itself and time remaining until it's
reap-eligible (`remaining=expired` once past the deadline), rounded to
whole seconds. TTL remaining is computed
from the later of the branch's last-touch time and its lease expiry,
whichever is later — matching exactly what the janitor's reap logic uses,
so `status` never disagrees with what will actually happen.

After the branch listing, a final `ro-cache: N entries, B bytes used
(budget: unlimited)` line reports `checkouts-ro`'s
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

### Storage class: `storage=shared` vs `storage=materialized`

Every branch line carries its copy-on-write cost class, because the two
classes have genuinely different storage bills and hiding that would be
dishonest:

- **`storage=shared`** — the branch reads through another lineage's
  durable objects via a base pointer: a fork, and since v0.2.12 the
  default result of `rollback` and `promote`. It added near-zero storage
  of its own when it was made. Cheap to hold, but it **pins** whatever
  storage its chain still resolves through (see `offshoot destroy` above).
- **`storage=materialized`** — the branch is a fully self-contained
  lineage: created roots, the results of `compact` and of
  `rollback --materialize`/`promote --materialize`, and forks, rollbacks
  or promotes that tripped the fork-time snapshot floor. It pins nothing
  and nothing else's destruction can defer its reclaim.

Before v0.2.12, `promote` and `rollback` always copied the whole
database into a new lineage ("fork at checkpoint X is free but rollback
to X is not"). They now share like fork does, and the cost they used to pay up front
moves to reclaim: the lineage they point into stays live until the branch
diverges past its snapshot cadence or is compacted. `compact` is the one
verb that still always copies. The daemon's `branches` op reports the same bit as
`BranchInfo.shared` (wire-additive; an older client simply doesn't read
the key), computed from the same ref field, so the CLI and daemon surfaces
can never disagree.

### Branch states

Every branch is in exactly one of seven states, computed fresh on every call
— nothing about state is persisted anywhere. `offshoot status`
(`ops.Workspace.Status`, CLI/at-rest — see above) and the daemon's
`branches` op (`BranchInfo.state`; Python `Client.branches()`'s
`Branch.state`, TypeScript `Client.branches()`'s `Branch.state`) report the
identical computation for the states both can see; a daemon additionally
knows three states no at-rest computation can, since they depend on its own
in-memory session map:

| State | Meaning | Who can report it |
|---|---|---|
| `active` | The branch's ref carries a live lease — someone (in or out of a daemon) holds it right now: a session, `offshoot lease acquire`, or an at-rest `checkpoint` in progress (holder `checkpoint:<host>/<pid>/<nonce>`). | Both |
| `pending` | This daemon has reserved a session slot for the branch and is still inside its (slow) `session.Open` — no live session yet, but the branch is spoken for. | Daemon only |
| `error` | A session is open here and its `Err()` is non-nil (lease loss, a capture failure, any terminal session failure). | Daemon only |
| `closing` | A session here is closing: its capture has stopped, and its lease stays live until the close releases it. An `open` of the branch waits for the close (up to 15 s); flush, fork, promote, rollback, compact, checkout and destroy refuse with a "retry" error. | Daemon only |
| `dirty` | No live lease; a checkout exists whose sidecar-recorded identity (lineage/epoch/txid) matches the ref but whose content hash doesn't — un-checkpointed local edits. | Both |
| `detached` | No live lease; a checkout exists whose sidecar-recorded **lineage** doesn't match the ref's current lineage — a checkout orphaned by a `rollback`/`promote` that repointed the branch at a new lineage before (or without) refreshing this checkout (their own checkout refresh is best-effort and can be skipped by a busy checkout at repoint time). | Both |
| `idle` | None of the above. | Both |

**Precedence** when more than one condition technically holds, most to
least specific: `closing` > `error` > `pending` > `active` > `dirty` >
`detached` > `idle`. A fenced session that is being closed matches both
`closing` and `error`; `closing` wins because it is the more actionable
answer: wait, then reopen. `pending` never applies together with `closing`
or `error` (a daemon's session map holds one entry per `db@branch`: a
reservation, a live session or a closing one). The ordering that matters in
practice is `active` vs. `dirty`/`detached`: a branch can be both leased
AND locally modified/orphaned at the file level, and `active` wins the
report.

**`idle` is a deliberate addition to the original state taxonomy.** The
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
will feel this on every call. `status` does not use the fingerprint
shortcut `checkout` takes (see the `.sum` sidecar under `offshoot
checkout` above): it always hashes. If the checkpoint
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
(`<hostname>/<pid>` by convention; `checkpoint:<hostname>/<pid>/<nonce>`
for an at-rest checkpoint in progress), state (`held` or `expired`), epoch, and
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
sessions — use `offshoot serve` + `session open` for that. While the lease
is live, `checkpoint` refuses the branch with or without `--force` (it
takes the lease itself, so it waits for this one to be released or to
expire), and `rollback`, `promote --onto`, `compact` and `destroy` refuse
it unless `--force`.

## `offshoot lease release <db>[@branch]`

```
offshoot lease release app@main
```

Releases the branch's current lease (looked up first via the same listing
`lease list` uses), whoever holds it. It is meant for a holder that is
gone. Released under a live holder, the lease's next use fails: a daemon
session is fenced (its next renewal or flush fails, and whatever it had
not flushed never reaches the store), and an at-rest checkpoint fails
without committing and deletes its object. A checkpoint's lease
(`checkpoint:<host>/<pid>/<nonce>`) never needs this: it ends with the
checkpoint, or lapses on its own 30 s after its process dies.

**Errors:** no lease currently held on that branch.

## `offshoot serve [-socket PATH] [-reap-every DURATION] [-gc-grace DURATION] [-flush-every DURATION] [-snapshot-every N] [-ro-cache-budget BYTES] [-fd-budget N] [-http ADDR] [-token TOKEN] [-http-allow-non-loopback]`

```
offshoot serve
offshoot serve -socket /tmp/o.sock
offshoot serve -reap-every 1m -gc-grace 15m -flush-every 30s   # all three are the defaults
offshoot serve -snapshot-every 4                                # snapshot every 4th flush instead of every 16th
offshoot serve -http 127.0.0.1:8080                            # opt-in HTTP: token auto-generated, printed once
OFFSHOOT_TOKEN=$(openssl rand -hex 32) offshoot serve -http 127.0.0.1:8080
```

Starts the daemon: a long-running process that serves a unix socket (mode
`0600`) for `session ...` commands, holds branch leases, captures every
committed WAL transaction continuously, and runs the janitor. Blocks until
`SIGINT`/`SIGTERM` or the `shutdown` op, at which point it releases every
lease and shuts down cleanly (closing live sessions, waiting for closes
already in progress, draining in-flight opens, removing the socket, and
closing any HTTP listener) rather than leaving stale leases behind. Either
way the process exits only after every session has closed and released its
lease (at most 30 s).

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
costs](operations.md#what-a-flush-costs) in the README for what a background
flush (and every session's mandatory first "settling" flush) actually cost.

`-snapshot-every N` sets the full-snapshot cadence
(`session.Options.SnapshotEvery`) applied to every session this daemon
opens (default `16`, unchanged if the flag is omitted; must be `>= 1` —
there is no "unlimited"/"disabled" sentinel the way `-flush-every 0`
disables auto-flush, since every flush must eventually snapshot).
`Options.SnapshotEvery` has been configurable in the embeddable session
library; this flag exposes the same knob to a daemon-managed session. See [What a flush
costs](operations.md#what-a-flush-costs) in the README for the cost
trade-off this cadence controls: a **lower** N (more frequent snapshots)
means cheaper, bounded reads (a chain never replays more than N-1
segments past its snapshot) at the cost of shipping a full-database
upload on every Nth flush instead of an incremental segment; a **higher**
N amortizes that upload cost across more flushes but lets read-side
replay grow proportionally longer between snapshots. There is no single
right answer — it is a bandwidth/write-cost vs. read-latency trade-off
tuned to a workload's actual write-vs-read ratio, the same trade-off
[docs/benchmarks.md](../docs/benchmarks.md) measures at the library's
default of 16.

**Errors:** socket path already in use by another listener; underlying
store-attach failure; `-flush-every` given a negative duration;
`-snapshot-every` given a value less than 1, or a non-integer;
`-ro-cache-budget` given a negative value; `-fd-budget` given a negative
or non-integer value.

### `-ro-cache-budget BYTES` — checkouts-ro disk budget

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

**The by-chain entries count too.** Since v0.2.12 the same tree holds
`<db>/~by-chain/<chainID>.db`, the immutable entries writable checkouts
are cloned from (see `offshoot checkout` above), and the budget, the
`offshoot_ro_cache_bytes` gauge and `offshoot status`'s `ro-cache:` line
all count them at their **logical** size. On a cloning filesystem an entry
shares its data blocks with every checkout cloned from it, so this
over-states the real disk they use, and evicting one frees little until
those checkouts diverge; a `--at` miss leaves both a by-chain entry and its
`<branch>@<checkpoint>.db` clone, so it counts twice. Size the budget
with that in mind. An evicted by-chain entry costs only a rebuild the
next time a checkout of that state misses; it never touches a writable
checkout.

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

**The LRU clock (a deliberate design decision): a `.last-used`
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
watch evictions happen in real time. Each eviction removes the cache file
together with its `.last-used` marker (and, for a by-chain entry, its
`.sum`). A by-chain entry is reported with branch `~by-chain` and its
chain ID in the checkpoint position — `evicted app@~by-chain@<chainID>` —
since it belongs to no single branch.

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

### `-fd-budget N` — cached checkout descriptors

```
offshoot serve -fd-budget 64     # the default
offshoot serve -fd-budget 0      # unlimited
```

The daemon reads checkout files raw, to fingerprint and snapshot them,
through descriptors it caches, one per checkout path. `-fd-budget N`
bounds how many it keeps: each janitor pass (on the `-reap-every`
cadence) closes cached descriptors least recently used first until `N`
remain. It bounds cached checkout descriptors, not the process's total
(sockets, WAL readers and store files are not counted), and it counts
every file the cache holds, `checkouts-ro/<db>/~by-chain/` entries
included. A descriptor pinned by an open session or an in-flight read is
never closed: the session's capture engine holds SQLite locks on its
checkout, and closing any descriptor on that file would drop them. So a
daemon with more open sessions than `N` keeps one per session. `0` means
unlimited, matching `-ro-cache-budget`; `-reap-every 0` turns the janitor,
and with it this bound, off.

Separately, whatever the budget: a descriptor whose checkout was renamed
over (re-materialized) or deleted is stranded, holding the unlinked file's
disk, and is closed as soon as nothing pins it. When this process made the
strand, that happens right after the checkout, rollback, promote or
compact refresh, by-chain prune, destroy or reap that made it. A checkout
another process removed or replaced (a CLI `destroy`, `gc` or `checkout`
against the same store) is found only by the full sweep every janitor
pass runs. `offshoot mcp` reclaims its own strands the same way and runs
the full sweep on every `-reap-every` tick, daemon or not. With
`-reap-every 0`, the daemon or `offshoot mcp` keeps descriptors stranded
by another process until it restarts.

Metrics: `offshoot_dbfile_descriptors` (cached plus stranded),
`offshoot_dbfile_pins`, `offshoot_dbfile_stranded_pinned` (stranded but
still pinned, by a read or connection on the file or a connection on its
path: brief while a session or read outlives its file, a pin leak
if it stays non-zero across passes, logged as `offshoot: janitor:
dbfile: ...`) and `offshoot_dbfile_evicted_total{reason}` (`stranded` or
`budget`). The daemon `status` op reports the descriptor count as
`dbfile_descriptors` (SDK `daemon_status()` / `daemonStatus()`).

### `-http ADDR` — opt-in HTTP listener

Off by default. `-http ADDR` (e.g. `127.0.0.1:8080`) starts an HTTP
listener alongside the unix socket, exposing:

See [docs/operations.md](operations.md) for the operator-facing view of
this whole surface (metrics reference table, HTTP/auth threat model,
tuning-flag trade-offs) and [docs/recipes/kubernetes.md](recipes/kubernetes.md)
for a real sidecar manifest — this section stays the flag-by-flag command
reference.

| Method | Path | Auth | Body |
|---|---|---|---|
| `POST` | `/rpc` | Bearer | The same `Request`/`Response` JSON the unix socket speaks, one op per POST (`Content-Type: application/json` required; body capped at 1MiB, oversized -> `413`). Two ops are refused over HTTP: `subscribe` (use `GET /events`; refused in-band as a normal `{"ok":false,...}` JSON response) and `export` — the one op that writes to an unconfined, client-chosen path on the daemon host, safe under the unix socket's same-host trust model but an arbitrary-file-write primitive for a network client, so `export` alone answers `400` pre-dispatch and stays socket-only |
| `GET` | `/metrics` | Bearer | Prometheus text exposition of the locked `offshoot_*` metric set — see [docs/operations.md](operations.md#metrics) for the full name/type/label reference table |
| `GET` | `/healthz` | **none** | `{"ok":true,"sessions":N}`, where `N` counts open sessions (a closing one is not counted) — the one endpoint that needs no token, for liveness probes |
| `GET` | `/events` | Bearer | Server-Sent Events: the daemon's event stream (see [Eventing](#eventing-subscribe-op--get-events) below) |
| `GET` | `/debug/pprof/*` | Bearer | `net/http/pprof`'s standard handlers (index, cmdline, profile, symbol, trace) |

**Token:** `-token TOKEN`, `-token-file PATH` (the token read from a file, whitespace trimmed; the form to use on a shared host, since a flag value shows in `ps`) or `OFFSHOOT_TOKEN` sets it explicitly, in that order of precedence — an
explicit token shorter than **16 characters is a startup error** (a
too-short bearer token is guessable; generate a real one). If
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
address already in use; the two non-loopback-bind errors above; an explicit
token shorter than 16 characters; and `-token` or
`-http-allow-non-loopback` given without `-http ADDR` (they have no effect
alone, so serve refuses to start rather than silently ignore them).

## Eventing (subscribe op / GET /events)

See [docs/operations.md](operations.md#eventing) for the operator summary
(drop-slow-consumer semantics, what to do when a subscriber disappears,
the SDK helpers to reach for) — this section is the wire-level reference.

The daemon publishes one versioned JSON event per state transition it
observes, drainable two ways — the unix socket protocol's `subscribe` op,
and HTTP's `GET /events` (Server-Sent Events) — both carrying the exact
same JSON, encoded by the same function daemon-side. There is no history/replay: a subscriber only ever
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
| `fenced` | A session is fenced out by a lease it no longer holds | `cause`, `holder`, `epoch` |
| `session_closed` | A session's close has finished and this daemon has let go of the branch (daemon `close` op, or `shutdown`). Acting on it never meets a `closing` refusal, and it arrives before the `session_opened` of any reopen of the branch. | `holder`, `epoch`, `error` (only if the close itself errored) |
| `reaped` | The janitor destroys a branch whose TTL expired | *(none)* |
| `evicted` | The janitor evicts a `checkouts-ro` entry over `-ro-cache-budget` | `checkpoint`, `bytes` (a by-chain entry reports branch `~by-chain` and its chain ID as `checkpoint`) |
| `dropped_slow_consumer` | Sent to a subscriber being dropped (see below), never to anyone else | *(none)* |

`holder` and `epoch` name a session's lease, not the session, and a later
session of the branch can carry the same pair. When a close fails to
release its lease, a reopen of the branch by the same daemon renews that
lease in place, so its `session_opened` repeats the `holder` and `epoch` of
the `session_closed` (with `error`) just before it. And rollback, promote
and compact restart a branch's epoch, as does destroying the branch and
creating it again, so the next session's `epoch` can equal an earlier
one's. To match a `session_closed` or `fenced` to its `session_opened`, take
the latest `session_opened` for the same `db` and `branch`: a session's
`session_closed` always arrives before the `session_opened` of a reopen.

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

**SDK helpers:** both SDKs ship a thin `events()`
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
alive across any proxy/load balancer/kubelet that kills silent streams — ordinary SSE clients ignore comment lines
automatically. Unlike the other `-http` routes, this handler manages its
own per-connection write deadline (re-armed before every write, see
"Stalled (still-connected) subscriber" above) rather than being bound by
the `http.Server` `WriteTimeout` (90s, sized for
`/rpc`/`/metrics`/`/debug/pprof/*`, see above) — a live subscription is
never hard-cut by that 90s bound, while a stalled one is still cleaned up
promptly rather than left open indefinitely.

## `offshoot mcp`

```
offshoot mcp [-default-ttl DURATION|none] [-socket PATH] [-allow-force] [-reap-every DURATION|none]
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

Serves the Model Context Protocol on stdio: nine tools (`offshoot_list`,
`offshoot_checkout`, `offshoot_checkpoint`, `offshoot_fork`,
`offshoot_rollback`, `offshoot_promote`, `offshoot_destroy`,
`offshoot_touch`, `offshoot_diff`), each described so a model knows not just what it does
but *when* to reach for it (fork before risky work, checkpoint when tests
pass, roll back when they fail, promote the attempt that worked). Besides
`claude mcp add`, the Claude Code plugin (`claude plugin marketplace add
sricola/offshoot && claude plugin install offshoot@offshoot`) installs the
same server plus a skill that teaches the loop and advisory hooks; see
[docs/agents.md](agents.md#wire-it-into-your-agent). Destructive tools
honor the same protected-branch rules as the CLI — an unforced
`offshoot_promote --onto main` or `offshoot_destroy` on `main` is refused,
and the refusal is returned to the agent as the tool result, not a
transport-level error. There is no `offshoot_protect`/`offshoot_unprotect`
tool; flipping a branch's protected flag is CLI-only by design (see
[`offshoot protect`](#offshoot-protect-dbbranch--offshoot-unprotect-dbbranch)
above) — an agent can see the flag via `offshoot_list` but never change it.

**`-allow-force` (off by default): the force gate.** `offshoot_promote`'s
and `offshoot_destroy`'s `force` argument is honored exactly as before this
flag existed *only* when the server was started with `-allow-force`.
Without it — the default — a call carrying `force: true` against a
protected branch is refused *before any mutation*, with a tool-result
error naming `-allow-force` and pointing the agent at the CLI or a fork
instead; `force` against an *unprotected* branch is silently downgraded to
`false`, so the call proceeds exactly as an unforced one would (no
behavior change there). This also disables `offshoot_destroy`'s live-lease
bypass — under the old default, `force` could delete a branch out from
under an active lease; without `-allow-force` it no longer can, exactly
like the protected-branch check. The gate (`refuseForceOnProtected`) fails
CLOSED on a `GetRef` error (e.g. a transient backend blip): `force` is
downgraded to `false` rather than let through, so a storage hiccup can
never accidentally honor a force the gate couldn't actually verify was
safe — the real error still surfaces from the downstream call, just never
with force intact. Turning `-allow-force` on restores the pre-gate
behavior in full: an agent's own `force: true` is honored as-is, same as
`--force` on the CLI. See
[docs/demo/mcp-walkthrough.md](demo/mcp-walkthrough.md) for this refusal
firing for real against a live server, followed by an `offshoot_diff`
comparison and a human-run CLI promote — the intended shape of the
guardrail, not a workaround for it.

**`-reap-every DURATION|none` (default `60s`): daemonless TTL reaping.**
`offshoot mcp` is not a daemon in the `offshoot serve` sense (it never owns
a live SQLite session), but a bare `offshoot mcp` is no longer inert with
respect to TTLs either: on this cadence, the process reaps expired forks
and self-heals any stranded delete claim, for as long as it stays up —
exactly `offshoot serve`'s janitor's reap pass, minus GC. `-reap-every 0`
or `-reap-every none` disables it, back to the old always-daemonless
behavior. When a real `offshoot serve` daemon is reachable at the same
socket, the MCP process defers to it entirely and logs once that it's
skipping its own pass — never a second writer racing the daemon's own
janitor against the same store. Either way, `-reap-every` runs **no GC**:
reclaiming a reaped branch's storage still needs `offshoot gc` (by hand) or
a running `offshoot serve` daemon. Each tick, daemon or not, also closes
this process's descriptors on checkouts that were renamed over or removed
(see `serve -fd-budget`), including ones another process removed.

**Annotations.** Every tool's `tools/list` entry carries an explicit
`annotations` object — `readOnlyHint`, `destructiveHint`, `idempotentHint`,
and `openWorldHint` (always `false`: every tool acts only on its own
store). `offshoot_list` is the only read-only tool; `offshoot_checkout`,
`offshoot_fork`, `offshoot_checkpoint`, and `offshoot_touch` are
non-destructive; `offshoot_rollback`, `offshoot_promote`, and
`offshoot_destroy` are destructive, so a host that honors
`destructiveHint` prompts before them. Nothing is left to the spec's
default (which is `destructiveHint: true`), so an unannotated fork never
reads as destructive to a host that checks the hint.

**`structuredContent`.** Every successful tool result returns
`structuredContent` — a snake_case JSON object (`txid`, `path`, `ttl`,
`expires_at`, `backup`, …, tool-dependent) — alongside the human-readable
`content` text, so a harness can read fields instead of parsing sentences.
An error result (`isError: true`) carries no `structuredContent`.

**`meta`.** `offshoot_fork` and `offshoot_checkpoint` accept an optional
`meta` argument (string→string, at most 32 keys) that tags the resulting
branch or checkpoint with a run id, git SHA, or agent name for later
lookup via `offshoot_list`.

**`offshoot_touch`.** Resets a branch's activity clock so its TTL does not
expire mid-task, and optionally changes the TTL: omitting `ttl` keeps the
current one, a Go duration string (e.g. `"2h"`) sets it, and `"none"`
clears it so the branch never expires. Calling it does not itself reap
anything — see `-reap-every` above for what actually does.

Agent-initiated forks carry a TTL by default: `offshoot_fork` applies
`-default-ttl` (default `24h`) to any call that omits its own `ttl`
argument, so a branch an agent forks and forgets is eligible for reaping
instead of accumulating forever. `-default-ttl 0` or `-default-ttl none`
disables the default; an individual `offshoot_fork` call can still override
it either way — an explicit `ttl:"<duration>"` always wins, and
`ttl:"none"` always yields no TTL even under a configured default. The
fork tool's response echoes the applied TTL and, when there is one, the
computed expiry timestamp, so both land in the agent's own transcript.
Reaping an expired TTL is `offshoot mcp`'s own `-reap-every` background
pass (default `60s`; see above), an `offshoot serve` daemon's janitor when
one is reachable, or `offshoot gc` run by hand — never a side effect of the
`offshoot_fork`/`offshoot_touch` calls that set the TTL in the first
place.

**MCP rides a running daemon, but only for a branch a session is already
open on.** No MCP tool ever opens a session itself (that's a harness's job —
the SDKs, `offshoot session open`, or a custom loop); each call to
`offshoot_checkpoint`, `offshoot_fork`, or `offshoot_checkout` freshly
checks whether the daemon named by `-socket` (default: the same socket
`offshoot serve` derives for this store) has one open for the branch in
question. If so, `offshoot_checkpoint` flushes it live through the daemon
instead of writing an at-rest checkpoint (an at-rest result carries `kind`,
`"segment"` or `"snapshot"`, as `offshoot checkpoint` above; a live flush's
result carries none); `offshoot_checkout`
returns that session's own live checkout path. `offshoot_fork` goes
further: it routes through the daemon whenever one is merely *reachable*,
session or no session — so the fork uses the daemon's configured
`-snapshot-every` share floor and lands in the daemon's fork metrics; when
the source does have an open session, the daemon flushes it first so
unflushed writes land in the child. Without a reachable daemon — the
common case for a bare `offshoot mcp` — every tool behaves exactly as it
does with no daemon running at all; see [docs/status.md](status.md) for
what's tested.

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
session first (`offshoot session close`) and retry. A session that is
already closing (`state: "closing"` in the daemon's `status`) is refused
too, but the refusal says to retry in a few seconds rather than to close
it, and `offshoot_checkout` and `offshoot_checkpoint` refuse a closing
session the same way (`a daemon session (holder "...") on <db>@<branch> is
closing; retry in a few seconds`) instead of falling back to at rest,
which would race the close for the checkout and the lease.
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
resolved socket). `branch` defaults to `main`. The daemon protocol's `open`
response also carries a `session_id`, 128 random bits the daemon mints for
this session, which a `close` can send back to close only this session
(see `session close` below).

If a session on the branch is closing (another client's `close`, or one a
killed client left running), `open` waits up to 15 s for that close to
finish, then opens with a fresh lease epoch. The one exception: if that
close failed to release its lease, the daemon renews the lease in place,
and the new session keeps the closed one's holder and epoch. That lasts
until each daemon session has a lease holder of its own.

**Errors:** the branch is already open by this daemon; the branch is still
closing after 15 s (`daemon: <db>@<branch> is still closing; retry`); the
branch's lease is held elsewhere (an at-rest checkpoint's,
`checkpoint:<host>/<pid>/<nonce>`, ends within seconds: retry); the daemon
is shutting down; no daemon reachable at the socket.

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
every Nth flush (`serve -snapshot-every`, default 16) and an incremental
segment (only the changed pages) otherwise, so materializing state never
replays more than one snapshot plus N-1 segments.

There is no separate daemon "checkpoint" op — a live session's named flush
*is* how its checkpoints are created; the underlying daemon protocol's
`flush` op also accepts a `meta` map (same caps as `checkpoint`'s `--meta`),
which the Python/TypeScript SDKs' `flush(name, meta=...)` expose. This CLI
subcommand does not have a `--meta` flag today — use an SDK client for
metadata on a live-session checkpoint.

**Errors:** `db@branch` is not open here; the session is closing
(`daemon: <db>@<branch> is closing`); the session has lost its lease
(fenced — it will not write under a dead epoch); `meta` given with no
checkpoint `name`; `meta` over a cap (SDK-only, since this CLI subcommand
has no `--meta` flag).

## `offshoot session status [-socket PATH]`

```
offshoot session status
```

Lists every session this daemon holds, open or closing: `db@branch`,
`state=open` or `state=closing` (the status op's `SessionInfo.state`; a
closing session is listed until its close has released the lease; an older
daemon sends no `state`, which means open), durable transaction id, epoch,
lease holder, checkout path, and — if the session has hit an error (e.g.
fenced by a lost lease, or a contract violation) — that error inline. This
is per-session detail; for the computed branch-state taxonomy
(`active`/`pending`/`closing`/`error`/`dirty`/`detached`/`idle`) across
EVERY branch of a db — including ones with no session open at all — see
[Branch states](#branch-states) above and the daemon `branches` op (SDK
`Client.branches()`; no CLI `session branches` subcommand exists yet).

## `offshoot session close <db>[@branch] [-socket PATH]`

```
offshoot session close app
```

Closes the session and releases its lease. `branch` defaults to `main`.

If the session is already closing (a duplicate or retried close), waits up
to 15 s for that close and returns its result, or `daemon: <db>@<branch> is
still closing; retry`. While a session is closing, the daemon refuses flush
with `daemon: <db>@<branch> is closing`, and fork, promote (either side),
rollback, compact, checkout and destroy of the branch with `daemon:
<db>@<branch> is closing; retry when the close finishes`.

This command names a branch, not a session: it closes whatever session is
open on the branch when it runs. A close retried after the first one has
finished therefore closes a session opened on the branch since, and that
session's writes since its last flush are never shipped. A daemon protocol
`close` that carries `session_id` (the value its `open` returned) closes
only that session: if that session is still closing, the close waits for it
as above, and if it has closed, the close fails with `daemon: session <id>
on <db>@<branch> is not open` even when another session is open on the
branch by then. The Python and TypeScript SDKs' `Session.close()` send it.
A close whose lease release failed has still closed its session: retried
with `session_id`, it fails with `is not open`.

**Errors:** `db@branch` is not open here (it was never opened, or its close
has finished); with `session_id`, that session is neither open nor closing
here; the session closed but its lease release failed. In that last case
the lease lapses at its expiry unless the branch is reopened first: a
reopen by this daemon renews that lease in place (see `session open`
above), and the reopened session holds it from then on. Do not free it with
`offshoot lease release`. That command releases whatever lease the branch
carries without asking who holds it, so once the branch has been reopened
it releases the reopened session's lease: that session is fenced, and its
writes since its last flush are never shipped.

## `offshoot session shutdown [-socket PATH]`

```
offshoot session shutdown
```

Asks the daemon to shut down gracefully (equivalent to sending it
`SIGINT`/`SIGTERM`): releases every lease, closes every session (waiting for
closes already in progress), removes the socket. The daemon process exits
only after every lease is released, bounded by 30 s like the signal path; a
signal that arrives while the shutdown is still closing sessions waits for
it too.

The command itself returns as soon as the daemon accepts the request, before
those closes finish. The daemon stops listening and removes its socket right
away and never touches the socket path again, so a new `offshoot serve` on
the same socket can start at once. Until the old process has released a
branch's lease, the new daemon's `open` of that branch fails because the
branch is leased. A script that needs every lease released first should
wait for the old `serve` process to exit.

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

**Threat model:** these ops trust their caller the way the local unix
socket (mode `0600`, created with that mode from the first instant) does:
any process able to open it already runs as the same user on the same
host, so `export`'s destination path is trusted as an ordinary filesystem
path that process can write — the daemon does not sandbox it or check it
against an allow-list beyond requiring it be absolute. That trust does NOT
extend to the opt-in HTTP listener: `export` is refused over `POST /rpc`
(it would be an arbitrary-file-write/exfiltration primitive for an
authenticated network client) and remains unix-socket-only; `checkout-at`
stays available over both transports since it only ever writes inside the
store's own `checkouts-ro` tree.

## Daemon protocol op: `diff`

Unlike `export`/`checkout-at` above, `diff` has a CLI equivalent (`offshoot
diff`, documented above) — the daemon op is the same content-aware summary
reachable from a session-aware SDK client (Python/TypeScript `diff()`) or
directly over `POST /rpc`.

Request fields: `left`, `right` — `db[@branch[@checkpoint]]` targets, the
same form `export` takes (the two sides may name the same `db` or two
different ones); `table` (optional) restricts the comparison to one table;
`full` (optional bool) also runs `sqldiff` on the daemon host and returns its
SQL, capped at `max_bytes` (0 means `ops.DefaultSqldiffMaxBytes`, 1 MiB; the
daemon refuses anything over an 8 MiB ceiling). Both sides materialize
through the same read-only primitives as `export`/`checkout-at` (never a
live checkout, never a lease).

Response: a `diff` object — `left`, `right` (the request's own target
strings, echoed back), `tables` (one entry per table, the same shape as
`ops.TableDiff`: row counts, `added`/`removed`/`changed` counts, `key`,
`schema_changed`, `status`), `totals` (`same`/`changed`/`added`/`removed`
table counts), and, when `full` was set, `full` (the capped `sqldiff` SQL
text) and `truncated` (true when that text was cut short of the complete
diff). `full` fails with a clear error if `sqldiff` isn't on the **daemon's**
PATH — omit `full` for the summary alone, which never needs it.

`diff` is allowed over the HTTP surface (unlike `export`): its response is
always JSON built from data already inside the store, never a path on the
daemon's filesystem, so it carries none of `export`'s
arbitrary-file-write/exfiltration risk for an authenticated network client.

---

## Surface parity: CLI vs daemon vs SDKs

Which operations exist on which surface today — verified against
`cmd/offshoot/main.go`'s dispatch, the daemon protocol's op list
(`internal/daemon/protocol.go`: `open`, `flush`, `status`, `close`,
`shutdown`, `create`, `checkout`, `fork`, `destroy`, `rollback`,
`promote`, `compact`, `touch`, `branches`, `dbs`, `export`, `checkout-at`,
`subscribe`, `diff`), and both SDK clients (`sdk/python/offshoot/client.py`,
`sdk/typescript/src/client.ts`).

| Operation | CLI | Daemon op | Python/TS SDK | Notes |
|---|---|---|---|---|
| create / checkout / fork / destroy / rollback / promote / compact / touch / branches / dbs | yes | yes | yes | Full parity. `compact` through the daemon refuses while a session is open on the branch (see above). `rollback`/`promote`'s safety-fork backup (`--no-backup`/`--backup-ttl` on the CLI, `no_backup`/`backup_ttl` request fields and a `backup` response field on the daemon op, `backup`/`backup_ttl` kwargs on both SDKs) is full parity too, and so is their `--materialize` (`materialize` request field and `shared` response field on the daemon op; Python `materialize=False`, TypeScript `materialize?: boolean`). |
| `protect` / `unprotect` | yes | no | no | **CLI-only, by design.** No daemon op or MCP tool sets the `protected` flag — only `offshoot_list`/the daemon's `branches` op reads it. Keeping the write side off every remote-callable surface means an agent (MCP) or a network client (daemon/HTTP) can observe protection but never grant or revoke it. |
| open / flush / status / close (sessions) | `session ...` | yes | yes | SDK `flush(name, meta=...)` can attach checkpoint metadata; the CLI `session flush` subcommand has no `--meta` flag. The SDKs' `Session.close()` sends the `session_id` its `open` returned, so it closes only that session; the CLI `session close` sends none and closes whatever session is open on the branch. |
| export / historical read-only checkout | yes | yes (`export`, `checkout-at`) | yes | No CLI `session` subcommand — the CLI's `export`/`checkout --at --read-only` are the at-rest equivalents (see the section above); `export` is unix-socket-only over the daemon. |
| events | — | yes (`subscribe` / `GET /events`) | yes (`events()`) | No CLI subscriber today. |
| shutdown | `session shutdown` | yes | **no** | Neither SDK exposes shutdown. |
| `create --from` (import) | yes | yes (socket only) | yes | Shipped in v0.2.11: daemon `create` op's `path` field, same same-host path-trust model as `export`, refused over HTTP; Python `from_path=`, TypeScript `{fromPath}`. MCP excluded by design, not deferred — an MCP tool must never import an arbitrary host file named by an agent; see [docs/status.md](status.md)'s `create --from` row. |
| `gc` (on-demand reap + collect) | **CLI-only** | no | no | A running daemon's janitor performs the same reap/GC on its `-reap-every` timer, so daemon deployments don't lack GC — they lack an RPC to *trigger* it on demand. |
| `lease list` / `acquire` / `release` | **CLI-only** | no | no | Daemon sessions manage their own lease lifecycle (`open` acquires, `close` releases); the CLI commands are the manual inspect/break-glass surface. |
| `diff` | yes | yes | yes | Full parity, plus an MCP tool (`offshoot_diff`) — the only surface here MCP reaches: one database; `left`/`right` are `branch[@checkpoint]`, not the CLI/daemon's `db@branch[@checkpoint]` (there's no cross-database diff over MCP). See [docs/status.md](status.md)'s diff row. |
| at-rest `checkpoint` | **CLI-only** | n/a | n/a | Not a gap: a live session's *named flush* is how daemon/SDK checkpoints are created (see `session flush` above). |
| whole-store `status` | **CLI-only** | no | no | The daemon's per-db `branches` op reports the same branch states/storage classes; only the all-dbs-plus-ro-cache-summary view is CLI-only. |
| `init` / `serve` / `mcp` / `version` / `path` | CLI | n/a | n/a | Process-level or purely local commands; nothing to proxy (`path` is `checkout`'s no-materialize sibling — see its section above). |

Summary: the SDKs cover the entire daemon protocol except `shutdown`;
what's genuinely CLI-only today is `init`, on-demand `gc`, the `lease`
commands, `protect`/`unprotect`, at-rest `checkpoint`, and the whole-store
`status` view — `diff` and `create --from` now both have full CLI/daemon/
SDK parity (`create --from` unix-socket-only over the daemon, and
deliberately excluded from MCP by design), and `diff` additionally reaches
MCP. For
CI patterns that mix the two surfaces (CLI seeding + SDK sessions), see
[docs/ci-recipes.md](ci-recipes.md).

## What's not here

See [docs/status.md](status.md) for the full implemented/deferred matrix
and links to the roadmap milestones tracking each. See also
[docs/stability.md](stability.md) for what pre-1.0 means for the commands
above (and the `export` → `create --from` format escape hatch),
[docs/testing.md](testing.md) for how this surface is tested, and
[docs/ci-recipes.md](ci-recipes.md) for ready-made GitHub Actions
workflows built from these commands.
