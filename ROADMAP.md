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

Releases use the 0.1.x prerelease series (tags `v0.1.0`, `v0.1.1`, …); 1.0 is
reserved for the storage-format freeze.

---

## Milestone 1 — Installable and trustworthy

*Bar: a stranger can install offshoot without a Go toolchain, read the repo
without tripping over internal artifacts, and watch CI prove the claims.*

- **Fix the module-path/org mismatch.** `go.mod` already declares
  `github.com/offshoot-db/offshoot`; the repo lives elsewhere. Create the
  `offshoot-db` org, transfer, sweep links. Until then `go install` — the only
  install path — 404s.
- **Claim the names.** `offshoot-db` on PyPI, the `@offshoot-db` npm scope,
  brew formula name; collision + trademark check on "offshoot" in dev tools;
  buy a domain.
- **Code CI.** Linux + macOS matrix (cgo needs real macOS runners): `go test
  ./... -race`, `go vet`, plus a MinIO service container running the S3
  conformance suite on every PR with no secrets. Nightly on main: real
  AWS/R2/Tigris conformance + the full kill-9 torture suite, feeding the
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

- **Prometheus `/metrics`**: capture lag, durable-through age per branch, GC
  backlog, checkout cache usage, fork/checkpoint latencies.
- **HTTP binding + single-token auth** (loopback by default, explicit opt-in
  beyond), unlocking sidecars and remote dev; container/k8s recipe docs.
- **Branch state taxonomy** (`active / detached / dirty / error / pending`)
  implemented and reported, not just spec'd.
- **Eventing.** A `subscribe` op (SSE once HTTP exists) for flush / lease-loss
  / fence / reap events, replacing supervisor polling.
- **Resource budgets.** Checkout-cache disk budget and FD budget with LRU
  eviction of cold read-only materializations; writable leased checkouts never
  evicted.
- **Tuning surface.** `SnapshotEvery` exposed through the daemon; documented
  guidance.
- **Follow-ups from review:** CAS-conditional ref delete (closes Destroy's
  read-then-delete window generally); typed TS response shapes before any
  1.0 SDK claim.

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
- **Page-level dedupe / content-addressed storage.** Revisit only if the v0.2
  benchmarks show TTLs + reflink + CopyObject aren't enough.
- **Row-level provenance.** Branch-level lineage plus checkpoint metadata is
  the right grain for the attempt workload.
