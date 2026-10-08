# offshoot roadmap

This is the working roadmap from here to a public launch and a 1.0-worthy tool.
It came out of a deliberate gap analysis from two chairs — a staff AI engineer
deciding whether to adopt offshoot for an agent platform / eval harness, and an
OSS operator auditing what a launch needs. Items are grouped by milestone, each
with the user story it unblocks. Non-goals are at the bottom and are as binding
as the goals.

Ground rules carried over from the design spec: correctness stays paranoid
(CAS everywhere, fail-closed probes, loud failures), honesty is a feature
(limits documented as plainly as features), and the storage format carries a
layout version so incompatibility is always detected, never guessed.

Releases use the 0.x prerelease series (tags `v0.1.0` … `v0.2.18`); 1.0 is
reserved for the storage-format freeze. 0.2.0 is the copy-on-write release —
a minor (not patch) bump because it changes the storage format
(LayoutVersion 1 → 2; see the copy-on-write milestone below). The 0.2.x
patch line since then is hardening on top of that arc — see the follow-ups
bullet in that milestone and [CHANGELOG.md](CHANGELOG.md).

---

## Milestone 1 — Installable and trustworthy

*Bar: a stranger can install offshoot without a Go toolchain, read the repo
without tripping over internal artifacts, and watch CI prove the claims.*

- **Fix the module-path/org mismatch.** Done — `go.mod` used to declare a
  module path under the aspirational `offshoot-db` GitHub org while the repo
  actually lived at `github.com/sricola/offshoot`; rather than transfer the
  repo into an org that doesn't exist, the module path and every
  import/doc/workflow reference were retargeted to
  `github.com/sricola/offshoot` to match where the code already lives. `go
  install github.com/sricola/offshoot/cmd/offshoot@latest` resolves.
  PyPI/npm package names are unaffected by this — see "Claim the names"
  below.
- **Claim the names.** `offshoot-db` on PyPI, the `@offshoot-db` npm scope,
  brew formula name; collision + trademark check on "offshoot" in dev tools;
  buy a domain.
- **Code CI.** Linux + macOS matrix (cgo needs real macOS runners): `go test
  ./... -race`, `go vet`, plus a MinIO service container running the S3
  conformance suite on every PR with no secrets. Nightly on main: real
  AWS conformance + the full kill-9 torture suite, feeding the
  provider-support table. Badges in the README.
- **Release engineering.** goreleaser with per-OS native build runners (cgo
  rules out naive cross-compilation; zig-cc is the fallback), darwin/linux ×
  amd64/arm64, `offshoot version` with embedded version/commit, Homebrew tap,
  curl-sh installer, Docker image, THIRD_PARTY_LICENSES/NOTICE bundled with
  binaries.
- **Community floor.** CONTRIBUTING (dev setup, the four test tiers and what
  they cost), SECURITY.md with private vulnerability reporting, Contributor
  Covenant, DCO (no CLA), issue templates that ask for store type and
  `offshoot status` output, Discussions on.
- **Docs scrub.** Rewrite the README status line in user terms (no internal
  plan numbers), edit the design spec into a public architecture doc, publish
  an honest implemented/deferred matrix so adopters can tell which spec
  guarantees are real today, add a CLI reference covering every command, and
  write the "why not Litestream / LiteFS / Turso / Dolt / cp" FAQ. Verify and
  record capture-engine provenance (clean-room vs adapted) so attribution is
  airtight.
- **Site hygiene.** The live site must not link to a private repo; teaser mode
  until the repo flips public.

## Milestone 2 — Safe by default for agents

*Bar: an unattended agent writing through offshoot cannot silently lose hours
of work, leak branches forever, or take the slow path by default.*

- **Background flush interval.** Today durability advances only on explicit
  `flush`; capture is continuous but nothing ships to the store on its own. A
  daemon that dies four hours after the last flush loses four hours. Add
  `serve -flush-every` (per-session override), defaulting on. *Delivered as
  one daemon-wide cadence, not a per-session override — see
  [docs/status.md](docs/status.md)'s "Per-session `FlushEvery` override" row
  for that gap, deferred as YAGNI. Both follow-ups originally noted here have
  since shipped: every session's mandatory first "settling" flush now skips
  the upload entirely when the checkout `Open` received was already proven
  unchanged since the branch's current head, and a clean `Session.Close` now
  refreshes the checkout's `.sum` sidecar so reopen-after-settling stays flat
  too — see status.md's "Settling-flush checksum-compare suppression" and
  "Sidecar refresh on clean Close" rows for exactly what's covered and what
  still isn't (a dirty/stale checkout, or a session that ever took a
  mid-session rebase-on-divergence, still pays the old cost once).*
- **MCP forks get TTLs.** The MCP `fork` tool currently cannot set a TTL, so
  every agent-initiated fork is immortal — the exact orphan-leak class the
  design calls launch-killing. Add the tool argument plus a server-side
  default TTL for MCP-created branches.
- **MCP rides the daemon.** *(Amended to reflect what this milestone
  actually delivers.)* MCP's `checkpoint`/`fork`/`checkout` tools ride an
  **existing** daemon session — one already opened by a harness (the SDKs,
  `offshoot session open`, or a custom loop) — instead of running at rest,
  whenever a daemon is up and has a session open on the branch in question;
  `rollback`/`promote` (its target)/`destroy` refuse rather than repoint or
  delete a branch out from under such a session. No MCP tool opens or closes
  a session itself — that scope (an `offshoot_open`/`offshoot_close` pair or
  similar) is explicitly deferred, not delivered here, and moves to
  [Milestone 3](#milestone-3--the-eval-harness-release): an MCP-opened
  session would have no natural owner responsible for closing it, which
  would recreate exactly the leak class (leases and branches nobody ever
  releases) this milestone's TTL and background-flush work exists to kill.
  *(Milestone 3 update: the pytest fixture plugin and its vitest testkit
  counterpart both shipped and are exactly the always-present lifecycle
  owner this note anticipated — but for their own harness workload, not as
  an MCP tool pair. The MCP `open`/`close` gap named here is still open by
  the same reasoning; see [docs/status.md](docs/status.md)'s "MCP session
  open/close" row for the confirmed-still-deferred status.)* MCP-first
  stacks are the default enterprise path; the existing-session path (a
  harness opens the session, MCP rides it — see
  [docs/recipes/claude-agent-sdk.md](docs/recipes/claude-agent-sdk.md) for
  the concrete wiring) is the good path they get today.
- **Fork performance, measured then fixed.** Fork is currently a local byte
  copy plus a synchronous fork-point upload — O(size) twice, unmeasured at any
  size. Ship a benchmark suite (100MB / 1GB / 10GB, local + MinIO), publish
  the numbers, then implement reflink/clonefile fork with copy fallback and
  async fork-point upload. Seed-once-fork-many is the headline workload; it
  must be fast and provably so.
- **3am observability, first half.** `status` gains durable-through age,
  last-flush time, and capture lag; structured logs on every branch state
  transition with cause. (The full metrics endpoint shipped in Milestone 4.)
- **Resource behavior documented.** Budgets shipped in Milestone 4 (ro-cache disk budget; the FD budget followed as `serve -fd-budget`), but the current
  per-session disk/FD costs and failure modes go in the docs now.
- **Promote keeps its own undo.** *(Added after launch, from the first
  external design question — [discussion
  #40](https://github.com/sricola/offshoot/discussions/40).)* Promote was
  the one verb whose inverse the user had to build by hand (the demo's
  "fork main first" step). It now keeps the target's previous head as a
  shared, TTL'd `<target>-pre-promote` safety fork before repointing —
  one rolling undo point per target, marker-guarded, opt-out via
  `--no-backup`, always on for MCP. See
  [docs/reference.md](docs/reference.md)'s promote section for the
  pinning trade-off it accepts (bounded by the TTL).

## Milestone 3 — The eval-harness release

*Bar: the target persona's first hour is paved end to end: install, seed,
fork-per-test, inspect, export, clean up — from their language.*

**Status: mostly shipped, with two named deferrals (⏸ below).** Everything
marked ✅ landed on the `eval-harness` branch; see
[docs/status.md](docs/status.md)'s Integration Surface section for the
shipped-and-tested rows and [docs/eval-harness.md](docs/eval-harness.md)
for the tutorial. Two items were consciously pushed out with a stated
reason rather than silently dropped, not shipped and not pretended
otherwise — `create --from`'s daemon/SDK/MCP reach, and the actual
PyPI/npm/registry button-presses — see their own ⏸ bullets below.

- ✅ **`offshoot.pytest` fixture plugin + vitest helper + the serious
  tutorial:** session-scoped daemon, seed fixture, fork-per-test with TTL,
  worker-parallel branch naming, teardown. Shipped as
  `offshoot.pytest_plugin` (`offshoot-db[pytest]`) and
  `sdk/typescript/src/testkit.ts`; the tutorial is
  [docs/eval-harness.md](docs/eval-harness.md). This is also where
  MCP-initiated session open/close was slated to belong, deferred from
  Milestone 2's "MCP rides the daemon" (see that bullet) — the fixture and
  testkit are now real, always-present lifecycle owners for the harness
  workload they were built for, but no MCP `open`/`close` tool pair was
  built on top of them; that reach stays deferred for the same
  no-natural-owner reason — see
  [docs/status.md](docs/status.md)'s "MCP session open/close" row.
- ✅ **Publish the SDKs.** PyPI + npm with trusted publishing/provenance; SDK
  docs rewritten to installed-package form; manifests filled out
  (urls, classifiers, files whitelist). Publish pipeline (`.github/workflows/publish.yml`)
  is prepared and gated (`PUBLISH_ENABLED`, default off) — the pipeline
  itself is done; see the Listings bullet below for what's still
  user-gated.
- ✅ **Export.** New `offshoot export <db>@<branch>[@checkpoint] out.db`
  shipped for plain-file egress (backups, handoff) without
  fork-checkout-copy-destroy, daemon op and SDK parity included — this
  half of "Import/export everywhere" is fully shipped.
- ✅ **`create --from` reach (daemon protocol, SDKs) — shipped in
  v0.2.11.** The daemon's `create` op takes an optional absolute `path`
  (refused over HTTP, same-host path trust like `export`'s), both SDKs
  expose it (`from_path=` / `{ fromPath }`) and both test fixtures seed
  from a `.db` file through it. MCP deliberately does not get it: an
  agent must never import an arbitrary host file, so no MCP tool takes a
  path. The upload-channel alternative was not needed. See
  [docs/status.md](docs/status.md)'s `create --from` reach row. (This
  entry said "deferred" until 2026-10-03; it was stale.)
- ✅ **Read-only and historical checkouts.** Materialize a checkpoint for
  inspection without forking; sanctioned read-only sessions alongside a
  live writer. Shipped as `ops.Workspace.CheckoutAt` / `offshoot checkout
  --at --read-only` / daemon `checkout-at` op / SDK `checkout_at()`.
- ✅ **List databases** in the protocol and SDKs (cleanup jobs shouldn't
  shell out to the CLI). Shipped as the daemon `dbs` op / `offshoot session
  dbs` / SDK `dbs()`.
- ✅ **Checkpoint/branch metadata.** Timestamps and txids in `branches`
  output; a small user-metadata map on fork/checkpoint (eval run id, git
  SHA, agent id); branch-level lineage is the right grain — no row-level
  provenance. Shipped as `Ref.Meta`/`Checkpoint.Meta`, `--meta k=v`,
  `checkpoints_v2`/`touched_at`. MCP tool metadata exposure
  (`offshoot_fork`/`offshoot_checkpoint` taking a caller-supplied `meta`)
  stayed out of scope for this task — see [docs/status.md](docs/status.md).
- ✅ **Branch diff.** `offshoot diff a@x b@y` wrapping sqldiff over two
  materializations, plus a content-aware `--summary` (rows added/removed/
  changed by primary key or rowid, schema changes flagged); the daily
  "attempt-2 passed, attempt-3 failed, what changed?" loop. Shipped
  everywhere: CLI, a daemon `diff` op, SDK `diff()` in both languages, and
  the MCP tool `offshoot_diff` — see [docs/diff.md](docs/diff.md).
- ✅ **Framework recipes, not adapters.** The ThreadForks pattern (thread →
  branch, checkpoint-id → checkpoint) documented once and applied as short
  recipes: [Claude agent SDK hooks](docs/recipes/claude-agent-sdk.md),
  [OpenAI Agents SDK session store](docs/recipes/openai-agents.md),
  [LlamaIndex/CrewAI notes](docs/recipes/frameworks.md). LangGraph keeps the
  real companion (`offshoot.langgraph.ThreadForks`); everyone else gets a
  page, not a package.
- ⏸ **Listings — prepared, submission deliberately deferred (user-gated).**
  MCP registry `server.json` manifest authored in-repo
  but **not
  submitted** — the exact registry schema needs to be fetched and validated
  against at submission time, not assumed from this repo's own docs.
  LangGraph community-integration PR text drafted
  
  but **not submitted** — its install command needs real PyPI publication
  to be true. Both are blocked on the same user action as actual SDK
  publication: claiming the `offshoot-db` PyPI name and `@offshoot-db` npm
  scope (see the Launch track's Milestone 1 note and
  [docs/status.md](docs/status.md)'s publish-pipeline row) — everything up
  to that button-press shipped this milestone.

## Milestone 4 — Operable at scale

*Bar: a platform running hundreds of agent sessions can see, bound, and
automate offshoot.*

**Status: shipped**, with one bullet delivered narrower than originally
scoped and named as such below rather than silently dropped. See
[docs/status.md](docs/status.md)'s Observability/Resource-behavior sections
for the shipped-and-tested rows, [docs/operations.md](docs/operations.md)
for the operator-facing reference this milestone's Task 8 wrote, and
[docs/status.md](docs/status.md#not-yet-done) for
what's left that no further engineering resolves.

- ✅ **Prometheus `/metrics`**: capture lag, durable-through age per branch
  (open sessions only, by design — see
  [docs/operations.md](docs/operations.md#deliberately-out-of-scope)),
  GC backlog, checkout cache usage, fork/checkpoint latencies. Sixteen
  `offshoot_*` metric families total, names locked as API as of the
  [0.1.3] tag (one free rename window, now closed) — see
  [docs/operations.md](docs/operations.md#metrics)'s full reference table.
- ✅ **HTTP binding + single-token auth** (loopback by default, explicit
  opt-in beyond), unlocking sidecars and remote dev; container/k8s recipe
  docs. Shipped as `serve -http ADDR` (`internal/daemon/http.go`) plus
  [docs/recipes/kubernetes.md](docs/recipes/kubernetes.md)'s sidecar
  manifest — no container image is published yet (see the Standing-nag
  section linked above), so the recipe builds its own.
- ✅ **Branch state taxonomy** (`active / detached / dirty / error /
  pending`, plus an `idle` sixth state the design spec's taxonomy didn't
  anticipate — see [docs/operations.md](docs/operations.md#branch-states))
  implemented and reported, not just spec'd.
- ✅ **Eventing.** A `subscribe` op (SSE once HTTP exists) for flush /
  lease-loss / fence / reap events, replacing supervisor polling. Shipped
  with a `checkouts-ro` eviction event type too, feeding directly off the
  budget bullet below.
- ⚠️ **Resource budgets — delivered narrower than scoped.** Checkout-cache
  (`checkouts-ro`) disk budget with LRU eviction shipped
  (`serve -ro-cache-budget`); writable leased checkouts are never evicted,
  structurally. The **FD budget with LRU eviction of cold read-only
  materializations did not ship this milestone** — consciously narrowed at
  Task 8 dispatch time, not discovered as a gap late:
  `internal/dbfile`'s file descriptors were then deliberately unclosable by
  that package's own design (a stray-close lock hazard the design
  deliberately avoids), which made "evict a cold session's FD" a real design
  problem needing its own pass, not a variant of the ro-cache budget's
  shape. See [docs/status.md](docs/status.md)'s FD-budget row. It shipped
  afterwards as `serve -fd-budget`, on a pin registry that lets
  `internal/dbfile` close a descriptor only when nothing in the process
  relies on its inode's locks. **Follow-up, not yet
  scoped:** `destroy`/GC never clean up a branch's `checkouts-ro` entries —
  they linger until LRU eviction claims them, which never happens at the
  default `-ro-cache-budget 0` (unlimited). Safe today (every entry is an
  immutable checkpoint snapshot, and `rm -rf checkouts-ro` remains sound at
  any time), but worth a real cleanup path now that Task 5 turned what used
  to be a manual `rm -rf` grace into an institutionalized cache.
- ✅ **Tuning surface.** `SnapshotEvery` exposed through the daemon
  (`serve -snapshot-every N`); documented guidance in
  [docs/operations.md](docs/operations.md#tuning-flags) including the
  flush-cost/replay-latency trade-off.
- ✅ **Follow-ups from review:** CAS-conditional ref delete (closes
  Destroy's read-then-delete window generally — local gets a true
  conditional delete, S3 formalizes the claim-marker pattern since
  `DeleteObject` has no real preconditions); typed TS response shapes
  shipped ahead of any 1.0 SDK claim.

## Copy-on-write storage — the storage-amplification arc

*Bar: N forks of a G-byte database stop costing N×G in the bucket, without
giving up bounded reads, GC safety, or destroy-anytime.*

**Status: shipped**, as the [0.2.0] release (see
[CHANGELOG.md](CHANGELOG.md)).
This closes the "storage amplification" risk the v1 design carried as its
open wart (its own spec said "N materialized forks cost up to N×G").

- ✅ **Storage amplification, killed for the fork workload.** A fork now
  shares its parent's durable objects through a base pointer and writes
  new objects only as it diverges — N forks of a G-byte database cost
  near-zero added bytes (N×G → shared). Bounded reads survive via two
  automatic snapshot floors; GC is rewritten as object-granular
  reachability over the transitive base closure; `offshoot compact` is
  the manual cord-cutter; the first shared fork bumps the store to
  LayoutVersion 2 and locks pre-0.2.0 binaries out of the store
  (deliberately — their lineage-granular GC would sweep shared objects).
- ✅ **Honesty preserved:** `status`/`branches` report each branch's cost
  class (`storage=shared` vs `storage=materialized`); the fork-shares vs
  promote/rollback/compact-materialize asymmetry and the
  destroy-lingers-until-last-child reclaim semantics are documented
  rather than hidden.
- ✅ **Post-0.2.0 follow-ups shipped across the 0.2.x patch line** (see
  [CHANGELOG.md](CHANGELOG.md) for each): the fork-time snapshot floor
  tracks the daemon's configured `-snapshot-every` cadence, plus
  `offshoot_fork_mode_total`/`offshoot_gc_errors_total` observability
  (0.2.1); a store-RPC/memory perf pass — streaming chain materialization
  and snapshot-flush upload, batched `DeleteObjects` GC sweeps, one less
  List/resolution per fork and GC pass (0.2.2); S3 multipart uploads, so
  >5 GiB snapshot flushes work (0.2.3); concurrent part uploads, multipart
  server-side copy up to S3's 5 TiB ceiling, and an epoch-aware GC
  compensating rule closing a bounded space leak (0.2.4); a
  fencing-vs-resolution dedup fix for a silent-data-loss race (0.2.5); and
  bounded waits on every S3 call — per-RPC deadlines plus a `GetReader`
  progress watchdog — so a stalled backend can't wedge flush or close
  (0.2.6/0.2.7).
- ⏸ **Page-level / content-addressed cross-database dedupe — still
  deferred, explicitly out of scope for this arc.** Copy-on-write shares
  whole objects between a fork and its ancestors only; deduping unrelated
  databases (or sub-object pages) remains the standing non-goal below,
  to be revisited only on evidence that per-object fork sharing isn't
  enough. **Measured 2026-10-03** (a throwaway tool that decodes every
  LTX object in a local store and hashes its pages; BranchBench at default
  scale, v0.2.16): in `software_dev` 55% and in `data_cleaning` 67% of
  stored page data duplicates a page stored under another lineage, almost
  none within a lineage (under 1%). The duplicates are in segments, not
  snapshots (2 snapshots per store): sibling forks applying the same
  mutation to the same parent state write identical changed pages. But
  the object data those segments make up is 44 MiB and 76 MiB of stores
  that end at 3.8 GiB and 8.3 GiB; checkouts and the by-chain cache are
  the footprint, and reflink already shares those. Page-level dedupe
  would save tens of MiB per store at the cost of a storage-format
  change. Not worth it on this evidence; revisit if S3-billed object
  bytes, not local disk, become the complaint.
- ✅ **Promote/rollback on sharing — shipped (v0.2.12).**
  Rollback to a kept checkpoint and promote now write a base pointer
  instead of copying (`--materialize` keeps the copy; the fork-time depth
  floor still forces one), and repeated rollbacks to one checkpoint keep
  the base spine flat. Shipped alongside it: clone-based checkouts from a
  by-chain cache, an O(1) clean check, and at-rest segment checkpoints
  against a reflinked shadow. ⏸ **Compact stays a copy**, by design —
  dropping the base pointer is what it is for.
- ✅ **Tier 2 follow-ups from that work** (none blocks v0.2.12):
  - ✅ Route Rollback/Promote/Compact's local checkout refresh through the
    by-chain cache (`materializeFromChain`), so the local refresh is
    O(delta) like `checkout`, not a full decode.
  - ✅ Move `internal/ops/reflink` to `internal/reflink`: `internal/ltxio`
    imported it, a layering inversion.
  - ✅ Pass the resolved chain members into `planSegment`, saving one Chain
    resolution per at-rest checkpoint.
  - ✅ Close the concurrent at-rest checkpoint race
    ([limitations](docs/limitations.md#one-writer-per-branch)): after
    winning the ref CAS, the checkpoint `Head`s its object and, on an etag
    mismatch, reads the stored trailer checksum; on a content mismatch it
    stamps `PostApplyChecksum=0`, drops the shadow and counts
    `offshoot_checkpoint_overwrite_detected_total`, so neither the next
    checkpoint nor a daemon session trusts content a same-kind loser
    overwrote. The final review widened it: a segment winner also probes
    for a racer's snapshot at its txid (which would anchor the head), a
    write between encode and stamp is caught by fingerprint, and a
    distrusted stamp records a hash no file matches, so the checkout reads
    "modified" unless it provably equals the store's head.
  - ✅ Fence concurrent at-rest checkpoints with the branch lease
    ([limitations](docs/limitations.md#one-writer-per-branch)): an at-rest
    checkpoint takes the lease before it writes its object and releases it
    in the ref write that advances the head, so its epoch, and with it its
    object key, is its own. A second checkpoint (or a session open) on the
    branch is refused while it runs, the third-racer and dead-loser windows
    are gone, and `checkpoint --force` no longer writes under a live
    session's epoch.
  - ✅ Normalize etags across providers before comparing them (strip quotes
    and a weak `W/` prefix), so an S3-compatible provider that reformats
    the etag between `PUT` and `HEAD` costs no needless `GET`.
  - ✅ A Local `Head` that does not re-hash the whole object: since v0.2.13
    the local backend records each object's etag in a `user.offshoot.etag`
    xattr at write time, so `Head` answers from it.
  - ✅ A Dependabot `pip` entry for `requirements/` (v0.2.14), so the
    hash-pinned CI locks get update PRs like the Go modules and Actions do.
  - ✅ Commit a 64 KiB-page, incompressible ltx v0.5.1 fixture beside the
    4 KiB ones, so the pinned frame shape is tested at the largest page
    size and at the LZ4 worst-case block
    (`internal/ltxio/testdata/ltx-v0.5.1-64k`).
  - ✅ Apply the 1 s racily-clean margin to the checkpoint stamp's
    matching-fingerprint shortcut (`stampCheckpoint`): on a coarse-mtime
    filesystem in WAL mode a foreign same-size write inside the quiesce
    tick could otherwise be stamped trusted; fall back to
    `ChecksumDatabase` when the mtime is younger than the margin.
  - ✅ Runtime image updates: decided and written down in the Dockerfile.
    The base image stays digest-pinned with Dependabot's weekly bumps as
    the only update path; no `apt-get upgrade` at build time, because an
    unpinned upgrade would make the image non-reproducible and silently
    diverge from the digest the SLSA provenance names.
- ✅ **A local lineage listing that does not grow with its epochs —
  shipped, two candidates.** Since at-rest checkpoints take the branch
  lease, each one writes under its own epoch, and a local store keeps one
  directory per epoch, so resolving a lineage's chain used to read one
  more directory per checkpoint it has taken: 16 ms after 1,000
  checkpoints on one lineage, against 1.2 ms when they shared an epoch
  ([benchmarks](docs/benchmarks.md)). The `.sum` sidecar now records the
  resolved chain's member keys at the head identity it stamps; a segment
  checkpoint and a fork at head reuse that recorded chain instead of
  calling `Store.Chain` when the sidecar matches the ref's head, is
  snapshot-anchored, contiguous and ends at the head (any mismatch
  resolves as before, re-recording the chain) — this removes the List
  from a segment checkpoint's own resolve, which now grows only
  25.5 → 30.3 ms across 1 → 1,000 checkpoints where it used to grow
  26.3 → 36.4 ms (a smaller climb, not a flat one). A separate
  diagnostic run found the largest measured contributor to what remains:
  the ref's own `Checkpoints` map, which grows by one entry per named
  checkpoint; part of that diagnostic's own climb is still unexplained,
  and its absolute numbers were not reconciled against this benchmark's
  ([benchmarks, "Diagnostic: isolating the ref's own
  cost"](docs/benchmarks.md)). It also drops a segment
  checkpoint's S3 request count by one `LIST`. Separately, the local
  backend now removes an epoch directory, then its lineage directory,
  once a delete empties it, bounding the directories a GC sweep or a
  failed checkpoint attempt leaves behind. **Residual, by design:**
  `Store.Chain` itself is unchanged — `checkout`, a fork below head,
  materialize, and every descendant resolving through the lineage as a
  base (a descendant's own checkpoints only until its recorded chain
  serves them; see the shared-child bullet below) still list every epoch
  directory a *kept* checkpoint left (the table above, unmoved by this
  work). `compact` still resets the count by making its result
  self-contained; `rollback` and `promote`, which share via a base
  pointer by default since v0.2.12, start a new lineage ID that
  `checkout`, a fork below head and materialize keep resolving through
  the old lineage's directories until the new one writes its own snapshot or `--materialize` copies it
  forward — the count moves, it does not reset, in that default case.
  S3 is still unaffected (its listing is flat). A local layout that
  lists a lineage in one directory read would remove the `Store.Chain`
  residual outright, but it is a store-format migration and is left to
  the maintainer to decide against the format-stability contract
  ([docs/stability.md](docs/stability.md)).
- ⏭ **A checkpoint index that does not grow the ref.** The ref's
  `Checkpoints` map holds one entry per named checkpoint a branch has
  ever taken and is read, json-decoded, mutated and written back twice
  per at-rest checkpoint (the lease acquire, then the head write); it
  never shrinks on its own (there is no "delete a checkpoint" op).
  `BenchmarkSegmentCheckpointRefGrowth` measured this as a real,
  separate cost: in that diagnostic run, one more checkpoint after
  1,000 prior checkpoints costs about 7.3 ms more than after 1, and
  holding the map at one entry throughout removes about 3.5 ms of that
  — the largest measured contributor, though not all of it
  ([benchmarks](docs/benchmarks.md)); the remaining ~3.8 ms of the
  diagnostic's own climb does not depend on the map's size and was not
  isolated further. A smaller or
  separately-stored checkpoint index (a side file listing names →
  `{txid,epoch}`, read lazily by name/rollback/prune instead of
  decoded whole on every checkpoint; or a bound on how many named
  checkpoints `Checkpoints` keeps, with older ones falling back to a
  slower listing) would close this, at the cost of a format change.
- ✅ **Extend the recorded-chain cache to shared child lineages —
  shipped.** A lineage that still resolves through a base pointer (a
  fresh fork, or a promote or rollback that kept the base spine) records
  a chain whose first keys name the base lineages, which the binding
  check above used to reject, so such a lineage listed on every segment
  checkpoint until it wrote its own snapshot. The recorded chain now
  serves it with no store read: the keys on other lineages must start
  with a snapshot and end exactly at `ref.Base.TXID`, and the lineage's
  own keys must be a contiguous run of segments starting at
  `ref.Base.TXID + 1`. Promote and a rollback to the head's own
  checkpoint now resolve the head through the same shortcut as a
  checkpoint and a fork at head. The test matrix that pins it, in
  `internal/ops/chain_cache_test.go`:
  `TestSharedChildSecondCheckpointUsesTheRecordedChain` and
  `TestSharedChildAfterOwnSnapshotUsesOwnChain` (a shared child
  checkpointing before its own snapshot, and after),
  `TestPassThroughSpineUsesTheRecordedChain` and
  `TestTwoHopSpineUsesTheRecordedChain` (a base-of-a-base spine),
  `TestSharedChildCacheSurvivesBaseDestroyAndGC` (the base branch
  destroyed and GC'd while the child keeps checkpointing),
  `TestSharedChildCorruptSeamResolves` (a suffix that does not start at
  `ref.Base.TXID + 1`, a prefix that does not end there, or an own key
  in front of an ancestor's), `TestSharedChildSeamMustMatchRefBase` (an
  intact record against `ref.Base.TXID` shifted by one either way),
  `TestForeignKeysWithoutBaseResolve`,
  `TestForkOfSharedChildAtHeadUsesTheRecordedChain`,
  `TestForkAfterOwnSnapshotFirstResolvesThenHits`,
  `TestPromoteAtHeadUsesTheRecordedChain` and
  `TestRollbackToHeadUsesTheRecordedChain`. Measured with
  `BenchmarkSharedChildCheckpointAfterParentCheckpoints` (a fresh child's
  second checkpoint after n checkpoints on its parent; two before and two
  after series, run one after another at one-minute load averages 2.62
  to 4.54), the series read, at n = 1, 100 and 1,000: before 1, 68.68 /
  77.07 / 34.23 ms (−34.45 ms from n = 1 to 1,000; its two high points
  did not recur); before 2, 23.87 / 28.11 / 34.87 ms (+11.00 ms); after
  1, 22.36 / 24.24 / 24.84 ms (+2.48 ms); after 2, 24.70 / 25.40 /
  19.99 ms (−4.71 ms) ([benchmarks, "A shared child's
  checkpoints"](docs/benchmarks.md)). The saving is bounded: a shared
  child's first checkpoint after a checkout still lists, and the
  checkpoints served run from the second up to and including the one
  that writes the child's own snapshot, which comes when the chain
  reaches `SnapshotEvery` if set, else `ForkShareMaxDepth` (16); after
  1,000 checkpoints on the parent, that is the child's 2nd through 8th.

## Launch track (parallel to v0.1–v0.3)

1. **Foundations** — org + transfer, names claimed, dead links fixed.
   *Exit: install path resolves; no public 404s.*
2. **Trust floor** — CI green and public, community files in place, docs
   scrubbed. *Exit: a stranger finds no internal artifacts and can run the
   tests.*
3. **Quiet public** — repo flips public, v0.1 tagged with binaries, SDKs
   published, FAQ live; 3–5 hand-picked eval-harness/agent-platform engineers
   invited. *Exit: one external person goes install → fork → promote without
   help.*
4. **Announce** — asciinema of parallel-attempts + an MCP-in-Claude-Code
   demo (the one nobody else can show), Show HN + lobste.rs + LangGraph/MCP
   communities same day, author on 48-hour triage rotation with pre-written
   answers for the predictable questions. *Only after phase 3's external user
   succeeded.*

**Rigor artifacts (2026-09-26).** Ahead of phase 4, the trust-floor and
quiet-public work picked up a rigor pass that now exists in the repo: every
tagged release is signed and attested (keyless cosign, SLSA build
provenance, an SPDX SBOM, a signed and attested GHCR image) with a
verification recipe in `docs/installation.md`; an OpenSSF Scorecard workflow
runs on push to `main`, weekly, and on demand, with a badge on the README;
`cmd/branchbench` reproduces BranchBench's five agentic-branching topologies
against a local store, pasted in `docs/benchmarks.md`; `docs/testing.md`
gained an "At a glance" evidence table naming what's proven and what isn't;
and the README's first screen now leads with install and a recording before
the quickstart. None of this changes phase 4's gate: **Announce** is still
blocked on phase 3's external user succeeding, not on further engineering —
see [docs/status.md](docs/status.md#not-yet-done) for the remaining
out-of-band items.

## Non-goals (v1)

- **Multi-node orchestration.** One daemon per store is the supported
  topology ([why](docs/limitations.md#one-daemon-per-store));
  placement/failover/routing are the v2 arc. We don't use the word "cluster."
- **Merge.** Forks are for pick-a-winner (`promote`), not three-way merge.
  The escape hatch is application-level reconciliation over two checkouts.
- **Windows.** The capture path and lock probing are POSIX-dependent.
- **Page-level dedupe / content-addressed storage.** Still a non-goal even
  after 0.2.0's copy-on-write forks: that arc shares whole objects between
  a fork and its own ancestors, never sub-object pages and never across
  unrelated databases. Revisit only on evidence that per-object fork
  sharing plus TTLs isn't enough.
- **Row-level provenance.** Branch-level lineage plus checkpoint metadata is
  the right grain for the attempt workload.
