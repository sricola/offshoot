# Offshoot Milestone 3: The Eval-Harness Release

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The target persona's first hour is paved end to end — install, seed, fork-per-test, inspect, export, clean up — from Python or TypeScript, with the pytest fixture as the paved road and honest docs at every fork in it.

**Architecture:** New protocol/SDK surface first (list-databases, metadata, import/export, read-only checkout-at), then the fixture plugin built on that surface, then diff/recipes/tutorial on top. The two ledgered Milestone-2 performance follow-ups (settling-flush suppression via checksum compare; sidecar refresh on clean close) land here because the fixture's session-per-test pattern is exactly the workload they fix. SDK publishing is PREPARED (workflows, manifests) but actual PyPI/npm publication is gated on user-claimed registry names — the plan ships everything up to the button-press.

**Tech Stack:** Go 1.24+ (existing); Python 3.10+ stdlib-only for the SDK core; the pytest plugin may declare `pytest>=7` as a dependency OF THE PLUGIN PACKAGE EXTRA only (`offshoot-db[pytest]`) — the base SDK stays dependency-free. Node 20+, zero runtime deps.

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; cgo; Linux/macOS; **no new Go module dependencies**
- Base Python SDK stays **stdlib-only**; pytest integration ships as an optional extra (`offshoot-db[pytest]`), never a hard dependency; TS SDK keeps zero runtime deps
- Wire-protocol additions are backward compatible: new optional request fields / new ops only; existing SDK methods keep working against the new daemon and vice versa (new SDKs against an old daemon fail with clear "unknown op" errors, not hangs)
- Read-only materializations must be UNAMBIGUOUSLY read-only in the API's language and never hold a lease; nothing about them may weaken the one-writer-per-lineage invariant
- Metadata is a small string→string map, capped (keys ≤ 32, key ≤ 64 bytes, value ≤ 512 bytes, enforced at the ops layer with clear errors) — branch-level lineage is the grain; no row-level provenance
- Any change under `internal/session` flush paths or `internal/capture` requires `make test-torture` run SYNCHRONOUSLY (single foreground Bash call, output tee'd — backgrounded runs have lost their logs three times) with numbers reported
- Every command in changed docs runs verbatim; claims match code exactly; status.md flips with each task
- CHANGELOG under [Unreleased] per task; releases stay in 0.1.x
- Commit messages: conventional commits with the repo's session trailers

## PM Amendments (binding)

1. **Execution order:** Task 3 FIRST (independent, torture-gated — a blowup there must not stall the fixture spine), then Task 1, then Task 7 (dry-run publish early so the user's registry-name claim becomes the only publish gate), then 2 → 4 → 5 → 6 → 8. Task 6 and the non-Claude recipes in Task 8 are the designated schedule-pressure cuts.
2. **Task 3 lands as two separate commits** (suppression; sidecar-refresh) so either can defer alone with a status.md row.
3. **`create --from` reach (daemon/SDK/MCP): pre-written deferral row.** Decision made now: it does NOT ride Task 1 — importing an existing file through the daemon needs an upload channel or a same-host path-trust story like export's, which deserves its own design; status.md row + ROADMAP note in Task 8, CLI remains the import path.
4. **Task 8 explicitly ships the ROADMAP amendment + status.md row for MCP open/close** (fixture is the lifecycle owner; no MCP `open` tool in M3 — an unowned session recreates the leak class).
5. **Task 4 locked API decisions (public forever once on PyPI):** per-worker daemon+seed under xdist (documented stance + ONE measured number for seed cost ×N workers); `offshoot_db` is a NAMED-SEED FACTORY (named seeds → named checkpoints; the singleton case is the factory's default seed) or at minimum an additively-widenable surface; fixture TTL default **1h** (ini-overridable; teardown-destroy is primary cleanup, TTL is the crashed-run backstop); pytester timebox = 2 fix rounds, then downgrade to unit + 2-3 smoke scenarios.
6. **Task 2 locked semantics:** export refuses to overwrite by default, `--force`/`force:true` overrides; export writes via atomic temp+rename (a failed export can never leave a truncated file); ro-cache documented in README resource-behavior with an explicit "safe to `rm -rf`" guarantee (no lease, no sidecar, rebuilt on demand); one line of threat-model text (same-host/same-user unix-socket trust) in reference.md.
7. **Task 4/8 golden-file framing:** SQLite files are not byte-deterministic — golden assertions are documented and implemented as `.dump`/query comparison, NEVER byte-compare of exported files.
8. **Outside the plan (user action, urgent, parallel):** claim `offshoot-db` on PyPI and the `@offshoot-db` npm scope this week — an unclaimed name beside a public publish workflow is the cheapest possible public embarrassment.

## File Structure

```
internal/ops/ops.go               (modify) metadata on fork/checkpoint; export; checkout-at
internal/ops/export.go            Export: materialize any checkpoint/head to a plain file
internal/ops/export_test.go
internal/store/store.go           (modify) Ref.Meta + Checkpoint metadata fields (omitempty, no schema bump)
internal/session/flush.go         (modify) settling-flush suppression (checksum compare)
internal/session/session.go       (modify) sidecar refresh on clean Close
internal/daemon/protocol.go       (modify) ops: dbs, export, checkout-at; meta fields; BranchInfo timestamps/txids
internal/daemon/server.go         (modify) handlers
cmd/offshoot/main.go              (modify) export command; checkout --at/--read-only; fork/checkpoint --meta
sdk/python/offshoot/client.py     (modify) dbs/export/checkout_at/meta params
sdk/python/offshoot/pytest_plugin.py   the fixture plugin
sdk/python/tests/test_pytest_plugin.py
sdk/python/pyproject.toml         (modify) [project.optional-dependencies] pytest; entry point
sdk/typescript/src/client.ts      (modify) parity additions
sdk/typescript/src/testkit.ts     vitest/jest helper (seed/fork/teardown)
sdk/typescript/test/testkit.test.ts
.github/workflows/publish.yml     PyPI trusted publishing + npm provenance (tag-gated, dry-run until names exist)
docs/eval-harness.md              the serious tutorial
docs/recipes/                     claude-agent-sdk.md, openai-agents.md, frameworks.md
docs/diff.md                      branch-diff recipe (sqldiff over two materializations) + offshoot diff wrapper
CHANGELOG.md / README.md / docs/status.md / docs/reference.md per task
```

## Task List (outline granularity — each task's dispatch carries exact interfaces; the loop's reviewers hold the bar)

### Task 1: Protocol + ops surface — list-databases, metadata, timestamps
`dbs` op returns all databases; `BranchInfo` gains `head_txid` (exists), `touched_at`, per-checkpoint `{name, txid, created_at}` objects (new `checkpoints_v2` field, old `checkpoints` kept for compat); `fork`/`checkpoint` accept `meta` (string map, capped per Global Constraints) stored on the ref (`Ref.Meta`) and per-checkpoint; CLI `--meta k=v` repeatable; SDK parity both languages. TDD; wire-compat tests (old-client-shaped requests still work).

### Task 2: Export + checkout-at (read-only, historical)
`ops.Export(db, branch, checkpoint, dstPath)` — materialize to a plain SQLite file anywhere (no sidecar, no lease); CLI `offshoot export <db>@<branch>[@checkpoint] out.db`; daemon op `export` (writes server-side path — document the trust model: same-host daemon, path must be absolute and the daemon refuses to overwrite existing files without `force`). `checkout --at <checkpoint> --read-only`: materializes into a distinct read-only cache path (`checkouts-ro/…`), never the writable checkout path, no lease, safe beside a live session (does NOT touch the live checkout's inode chain — dbfile hazard note). SDK parity. The fixture (Task 4) uses export for golden-file assertions.

### Task 3: Settling-flush suppression + sidecar refresh on clean Close (ledgered M2 follow-ups)
(a) `rebaseline()` already computes the fresh `ChecksumDatabase`; when it equals the head's `postApplyChecksum` (and TXIDs align), skip the settling flush (no forceSnapshot; document the exact condition). Read-only daemon sessions stop uploading full snapshots. (b) On clean `Close` (no error, capture drained, last flush successful), refresh the checkout's `.sum` sidecar so the next `Open`/`Checkout` clean-skips. Together these restore Milestone 2 Task 1's win for daemon reopen patterns. BOTH touch flush/session paths → torture (synchronous, tee'd) required; the M2 whole-branch reviewer's phantom-Lag regression test must stay green; update README/dbfile.go/status.md claims that were corrected in M2 to the new (better) truth.

### Task 4: `offshoot.pytest` fixture plugin
Package extra `offshoot-db[pytest]`; entry-point-registered plugin. Fixtures: `offshoot_daemon` (session-scoped: builds/locates binary via env `OFFSHOOT_BIN` or PATH, temp store, serve, teardown kills), `offshoot_db` (session-scoped seed: user provides a seed callback or SQL file via decorator/ini options; seeds once, checkpoints `seed`), `offshoot_fork` (function-scoped: forks from `seed` with TTL, worker-safe branch naming via `PYTEST_XDIST_WORKER` when present, opens a session, yields the path, closes + destroys on teardown — destroy failures WARN, never fail the test). Tested by running pytest-in-pytest (pytester fixture) against a real daemon. Honest failure modes: no binary → skip with instructions; daemon dies mid-session → the owning test errors with the daemon's stderr tail.

### Task 5: TS testkit + SDK parity finish
`sdk/typescript/src/testkit.ts`: `startDaemon()`, `seedOnce()`, `forkPerTest()` helpers mirroring the pytest semantics for vitest/jest (framework-agnostic functions, no framework dependency); test via node:test against a real daemon. Client parity for Task 1/2 surface if not already done there.

### Task 6: Branch diff
`offshoot diff a@x[@cp] b@y[@cp]`: materializes both (read-only cache) and runs `sqldiff` when present (clear error naming the package when absent), `--summary` for table-level row counts via stdlib queries when sqldiff is missing. docs/diff.md documents both the command and the raw recipe. No merge machinery — the FAQ's stance stands.

### Task 7: Publish pipeline (prepared, gated) + listings prep
`.github/workflows/publish.yml`: PyPI trusted publishing (OIDC) + npm provenance, triggered on `sdk-v*` tags, with a repository-variable gate (`PUBLISH_ENABLED`) defaulting off and a dry-run mode that builds artifacts + runs twine check/npm pack without uploading. Manifests filled (urls, classifiers, readme, files). MCP registry `server.json` manifest authored in-repo. LangGraph community-listing PR text drafted in docs/launch/ (not submitted — needs PyPI). Everything up to the button-press; the button needs the user's registry accounts.

### Task 8: The serious tutorial + recipes + docs sweep
docs/eval-harness.md: the full story — install, seed-once-fork-many with the fixture, xdist parallelism, golden-file assertions via export, TTL hygiene, CI recipe (daemon in CI), what it costs (link benchmarks). docs/recipes/: Claude agent SDK (MCP config + hooks pattern: fork on session start, checkpoint on stop), OpenAI Agents SDK (session-store recipe), frameworks.md (LlamaIndex/CrewAI short notes — recipes, not adapters). Every command run verbatim. README integration-surface refresh; status.md flips; CHANGELOG [0.1.2] drafted; ROADMAP M3 items checked off with deferral rows for anything consciously pushed (listings submission, actual publish).

## Deliberately out of scope (state in status.md)
- Actual PyPI/npm publication + registry submissions (user-gated: account/name claims)
- Metrics/HTTP/auth/eventing/budgets (Milestone 4)
- Merge machinery (FAQ stance stands)

## Self-Review
1. Roadmap coverage: every M3 bullet has a task (fixture T4, publish T7-prepared, import/export T2 — note `create --from` daemon/SDK reach rides Task 1's surface work if trivial or gets a deferral row, read-only T2, list-dbs T1, metadata T1, diff T6, recipes T8, listings T7-prepared); M2 follow-ups T3; MCP session lifecycle deliberately stays OUT (the fixture is the lifecycle owner — ROADMAP's amended stance; an MCP `open` tool remains unowned and is NOT built).
2. Placeholder scan: outline-granularity is deliberate (dispatches carry exact interfaces per the established loop); no TBDs; caps and gates are concrete.
3. Type consistency: `checkpoints_v2` naming consistent T1/T4/T8; export signature consistent T2/T4/T6; the fixture consumes only published SDK surface.
