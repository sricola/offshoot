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

Releases use the 0.x prerelease series (tags `v0.1.0` … `v0.2.7`); 1.0 is
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
  AWS/R2 conformance + the full kill-9 torture suite, feeding the
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
  transition with cause. (Full metrics endpoint lands in v0.4.)
- **Resource behavior documented.** No budgets yet (v0.4), but the current
  per-session disk/FD costs and failure modes go in the docs now.

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
- ⏸ **`create --from` reach (daemon protocol, SDKs, MCP) — deferred, not
  shipped.** The other half of "Import/export everywhere" did not land
  this milestone. This is a **pre-written deferral, not a gap found
  late**: importing an existing file through the daemon needs either an
  upload channel (stream bytes over the unix socket — a new wire
  primitive) or a same-host path-trust story like `export`'s own (the
  daemon reads/writes a path the caller names, trusted under the existing
  same-host/same-user socket trust model) — that design deserves its own
  pass. CLI `offshoot create --from` remains the only import path; see
  [docs/status.md](docs/status.md)'s `create --from` reach row.
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
  materializations; the daily "attempt-2 passed, attempt-3 failed, what
  changed?" loop. Shipped, CLI-only per the task's own scope (no daemon op,
  no SDK parity) — see [docs/diff.md](docs/diff.md).
- ✅ **Framework recipes, not adapters.** The ThreadForks pattern (thread →
  branch, checkpoint-id → checkpoint) documented once and applied as short
  recipes: [Claude agent SDK hooks](docs/recipes/claude-agent-sdk.md),
  [OpenAI Agents SDK session store](docs/recipes/openai-agents.md),
  [LlamaIndex/CrewAI notes](docs/recipes/frameworks.md). LangGraph keeps the
  real companion (`offshoot.langgraph.ThreadForks`); everyone else gets a
  page, not a package.
- ⏸ **Listings — prepared, submission deliberately deferred (user-gated).**
  MCP registry `server.json` manifest authored in-repo
  ([docs/launch/mcp-registry.md](docs/launch/mcp-registry.md)) but **not
  submitted** — the exact registry schema needs to be fetched and validated
  against at submission time, not assumed from this repo's own docs.
  LangGraph community-integration PR text drafted
  ([docs/launch/langgraph-listing.md](docs/launch/langgraph-listing.md))
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
[docs/status.md](docs/status.md#standing-nag-user-gated-launch-items) for
what's left that no further engineering resolves.

- ✅ **Prometheus `/metrics`**: capture lag, durable-through age per branch
  (open sessions only, by design — see
  [docs/operations.md](docs/operations.md#deliberately-out-of-scope-in-m4)),
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
  `internal/dbfile`'s file descriptors are deliberately unclosable by that
  package's own design (a stray-close lock hazard the design deliberately
  avoids), which makes "evict a cold session's FD" a real design problem
  needing its own pass, not a variant of the ro-cache budget's shape. See
  [docs/status.md](docs/status.md)'s FD-budget row. **Follow-up, not yet
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
[CHANGELOG.md](CHANGELOG.md) and the design spec,
`docs/superpowers/specs/2026-08-07-offshoot-copy-on-write-design.md`).
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
  enough.
- ⏸ **Promote/rollback (and compact) on sharing — deferred.** All three
  still materialize a full copy, by design (they abandon or replace a
  lineage; base-pointing into a lineage meant to die would pin it
  forever). Rollback-to-a-*kept*-checkpoint onto base pointers is the
  named follow-up in the design spec, not scoped here.

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

## Non-goals (v1)

- **Multi-node orchestration.** Shared-bucket safety is guaranteed by fencing;
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
